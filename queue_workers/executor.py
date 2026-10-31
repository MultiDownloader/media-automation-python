"""
Executors for the queue/worker layer.

Two implementations share one execution model:

- :class:`InlineExecutor` -- synchronous, in-process. Used as the default so the
  whole platform works (and is testable) without Redis or Celery installed.
- :class:`CeleryExecutor` -- production scale-out over Celery + a Redis broker.
  Celery is imported lazily; if it is missing or misconfigured the factory
  falls back to the inline executor so operational flows never hard-fail.

Both honour the same :class:`~queue_workers.retry.RetryPolicy` and route
exhausted retries to a dead-letter store (see ``dlq.py``). Compliance, auth and
permanent-input failures are terminal and are never retried -- the user's policy
decision is authoritative and retrying it would only hammer the system.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Protocol

from .dlq import DeadLetterStore, get_dlq_store
from .envelope import TaskEnvelope, TaskResult, TaskStatus, get_logger, now_iso
from .retry import AUTH, COMPLIANCE_BLOCKED, PERMANENT_INPUT, RetryPolicy, classify_error

logger = get_logger(__name__)

#: Categories that are terminal for a task (never retried, never dead-lettered).
TERMINAL_CATEGORIES = frozenset({COMPLIANCE_BLOCKED, AUTH, PERMANENT_INPUT})


# ============================================================================
# Shared helpers
# ============================================================================

def resolve_callable(fn: Any) -> Callable[[Dict[str, Any]], Any]:
    """Return a synchronous callable for ``fn``.

    Async handlers are supported: when ``fn`` is a coroutine function the
    returned wrapper runs it on an event loop. If the calling thread already
    has a running loop (e.g. the handler is invoked from async code) the
    coroutine is executed on a fresh loop in a short-lived worker thread so the
    caller's loop is never blocked or re-entered.
    """

    if not inspect.iscoroutinefunction(fn):
        return fn

    def _sync(payload: Dict[str, Any]) -> Any:
        coro = fn(payload)

        def _run() -> Any:
            return asyncio.run(coro)

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return _run()

        result: List[Any] = []
        error: List[BaseException] = []

        def _thread() -> None:
            try:
                result.append(_run())
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                error.append(exc)

        t = threading.Thread(target=_thread, name="queue-workers-async", daemon=True)
        t.start()
        t.join()
        if error:
            raise error[0]
        return result[0]

    return _sync


class BaseExecutor(Protocol):
    """Structural interface every executor implements."""

    def submit(self, envelope: TaskEnvelope) -> TaskResult:
        ...

    def get(self, task_id: str) -> Optional[TaskResult]:
        ...

    def stats(self) -> Dict[str, Any]:
        ...

    def drain(self) -> List[TaskResult]:
        ...


# ============================================================================
# Inline (in-process) executor
# ============================================================================

class InlineExecutor:
    """Synchronous in-process executor with retry + dead-letter bookkeeping.

    Handlers are registered per task type and receive the envelope payload as a
    plain dict (not the envelope itself), so handler signatures stay simple.
    Execution is thread-safe; results are kept in memory (bounded by
    ``max_results``) so ``get``/``stats``/``drain`` work for dashboards and
    tests. Attach a :class:`DeadLetterStore` to persist exhausted tasks.
    """

    def __init__(
        self,
        retry_policy: Optional[RetryPolicy] = None,
        dlq_store: Optional[DeadLetterStore] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        max_results: int = 1000,
    ) -> None:
        self._retry_policy = retry_policy or RetryPolicy()
        self._dlq = dlq_store
        self._sleep = sleep_fn
        self._max_results = max(1, int(max_results))
        self._handlers: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._results: Dict[str, TaskResult] = {}
        self._lock = threading.RLock()
        self._submitted = 0
        self._succeeded = 0
        self._failed = 0
        self._dead_lettered = 0

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------
    def register_handler(self, task_type: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
        """Register (or replace) the handler for ``task_type``."""
        with self._lock:
            self._handlers[task_type] = resolve_callable(fn)

    def has_handler(self, task_type: str) -> bool:
        with self._lock:
            return task_type in self._handlers

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def submit(self, envelope: TaskEnvelope) -> TaskResult:
        """Run ``envelope`` synchronously and return its final :class:`TaskResult`.

        The envelope's ``attempts`` field is advanced on every attempt and its
        ``id`` is used as the result key, so re-submitting the same envelope
        keeps one continuous attempt history (DLQ replay relies on this).
        """
        with self._lock:
            handler = self._handlers.get(envelope.task_type)
            self._submitted += 1

        if handler is None:
            result = TaskResult(
                id=envelope.id,
                status=TaskStatus.FAILED.value,
                error_category=PERMANENT_INPUT,
                error_message=f"No handler registered for task type {envelope.task_type!r}",
                attempts=envelope.attempts,
                started_at=now_iso(),
                finished_at=now_iso(),
            )
            with self._lock:
                self._failed += 1
                self._store(result)
            return result

        started_at = now_iso()
        t0 = time.perf_counter()
        last_error: Optional[BaseException] = None
        last_category = "unknown"

        while True:
            envelope.attempts += 1
            try:
                out = handler(dict(envelope.payload))
                result = TaskResult(
                    id=envelope.id,
                    status=TaskStatus.SUCCEEDED.value,
                    result=out,
                    attempts=envelope.attempts,
                    duration_ms=int((time.perf_counter() - t0) * 1000),
                    started_at=started_at,
                    finished_at=now_iso(),
                )
                with self._lock:
                    self._succeeded += 1
                    self._store(result)
                logger.info(
                    "task_succeeded",
                    task_id=envelope.id,
                    task_type=envelope.task_type,
                    attempts=envelope.attempts,
                )
                return result
            except BaseException as exc:  # noqa: BLE001 - classified below
                last_error = exc
                last_category = classify_error(exc if isinstance(exc, Exception) else Exception(str(exc)))
                if self._retry_policy.should_retry(envelope.attempts, last_category):
                    delay = self._retry_policy.compute_delay(envelope.attempts)
                    logger.warning(
                        "task_retrying",
                        task_id=envelope.id,
                        task_type=envelope.task_type,
                        attempt=envelope.attempts,
                        error_category=last_category,
                        delay_s=round(delay, 3),
                    )
                    if delay > 0:
                        self._sleep(delay)
                    continue
                break

        # Terminal categories (compliance/auth/permanent-input) -> FAILED and
        # NOT dead-lettered: replaying them would re-violate the same policy.
        # Retryable but out of attempts -> DEAD_LETTERED + persisted to the DLQ.
        terminal = last_category in TERMINAL_CATEGORIES
        status = TaskStatus.FAILED.value if terminal else TaskStatus.DEAD_LETTERED.value
        result = TaskResult(
            id=envelope.id,
            status=status,
            error_category=last_category,
            error_message=f"{type(last_error).__name__}: {last_error}",
            attempts=envelope.attempts,
            duration_ms=int((time.perf_counter() - t0) * 1000),
            started_at=started_at,
            finished_at=now_iso(),
        )
        with self._lock:
            if terminal:
                self._failed += 1
            else:
                self._dead_lettered += 1
            self._store(result)
            if self._dlq is not None and not terminal:
                try:
                    self._dlq.add(result, envelope=envelope)
                except Exception as dlq_exc:  # pragma: no cover - defensive
                    logger.error("dlq_add_failed", task_id=envelope.id, error_type=type(dlq_exc).__name__)
        logger.error(
            "task_failed",
            task_id=envelope.id,
            task_type=envelope.task_type,
            status=status,
            error_category=last_category,
            attempts=envelope.attempts,
        )
        return result

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def get(self, task_id: str) -> Optional[TaskResult]:
        with self._lock:
            return self._results.get(task_id)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "backend": "inline",
                "submitted": self._submitted,
                "succeeded": self._succeeded,
                "failed": self._failed,
                "dead_lettered": self._dead_lettered,
                "handlers": sorted(self._handlers),
                "tracked_results": len(self._results),
            }

    def drain(self) -> List[TaskResult]:
        """Return all tracked results and clear them."""
        with self._lock:
            results = list(self._results.values())
            self._results.clear()
            return results

    def _store(self, result: TaskResult) -> None:
        self._results[result.id] = result
        # Bounded memory: drop the oldest entries once the cap is exceeded.
        overflow = len(self._results) - self._max_results
        if overflow > 0:
            for key in list(self._results.keys())[:overflow]:
                del self._results[key]


# ============================================================================
# Celery executor
# ============================================================================

class CeleryExecutor:
    """Celery-backed executor for scale-out workers.

    Requires the optional ``celery`` package and a broker (``CELERY_BROKER_URL``
    or ``REDIS_URL``). Celery app settings can be extended via
    ``celery_config`` so operators tune concurrency/queues without code
    changes. No secrets live in code: broker credentials come from the
    environment only and are never logged.

    The Celery task body re-uses :class:`InlineExecutor`'s retry/dead-letter
    logic so behaviour is identical to the in-process backend. Handlers must be
    registered on the executor in the worker process as well (worker entrypoint
    calls ``register_default_handlers``).
    """

    #: Celery task name for normal work.
    TASK_NAME = "queue_workers.run"
    #: Celery task name used by the dead-letter queue.
    DLQ_TASK_NAME = "queue_workers.dead_letter"

    def __init__(
        self,
        broker_url: Optional[str] = None,
        result_backend: Optional[str] = None,
        queue_name: str = "default",
        dlq_name: str = "dead_letter",
        retry_policy: Optional[RetryPolicy] = None,
        celery_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            from celery import Celery  # noqa: PLC0415 - lazy on purpose
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "CeleryExecutor requires the 'celery' package (pip install celery). "
                "Use QUEUE_BACKEND=inline or install the optional dependency."
            ) from exc

        broker = broker_url or os.environ.get("CELERY_BROKER_URL") or os.environ.get("REDIS_URL")
        if not broker:  # pragma: no cover - depends on runtime env
            raise RuntimeError(
                "CeleryExecutor requires CELERY_BROKER_URL or REDIS_URL in the environment."
            )
        backend = result_backend or os.environ.get("CELERY_RESULT_BACKEND") or broker

        self._queue_name = queue_name
        self._dlq_name = dlq_name
        self._retry_policy = retry_policy or RetryPolicy()
        self._handlers: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._lock = threading.RLock()
        self._submitted = 0

        self._app = Celery("queue_workers", broker=broker, backend=backend)
        self._app.conf.update(
            task_acks_late=True,
            task_reject_on_worker_lost=True,
            task_default_queue=queue_name,
            broker_transport_options={"queue_max_priority": 10},
            task_routes={
                self.TASK_NAME: {"queue": queue_name},
                self.DLQ_TASK_NAME: {"queue": dlq_name},
            },
            **(celery_config or {}),
        )

        app = self._app
        dlq_task_name = self.DLQ_TASK_NAME
        dlq_name = self._dlq_name

        @app.task(name=self.TASK_NAME, bind=True)
        def _run(task_self, envelope_json: str) -> Dict[str, Any]:  # pragma: no cover - needs broker
            import json

            envelope = TaskEnvelope.from_dict(json.loads(envelope_json))
            handler = _lookup_global_handler(envelope.task_type)
            if handler is None:
                return TaskResult(
                    id=envelope.id,
                    status=TaskStatus.FAILED.value,
                    error_category=PERMANENT_INPUT,
                    error_message=f"No handler registered for task type {envelope.task_type!r}",
                    attempts=envelope.attempts,
                ).to_dict()
            runner = InlineExecutor(retry_policy=_lookup_global_policy() or RetryPolicy())
            runner.register_handler(envelope.task_type, handler)
            result = runner.submit(envelope)
            if result.status == TaskStatus.DEAD_LETTERED.value:
                # Park the original envelope on the dead-letter queue so an
                # operator can inspect and replay it later.
                task_self.send_task(dlq_task_name, args=[envelope_json], queue=dlq_name)
            return result.to_dict()

        @app.task(name=self.DLQ_TASK_NAME)
        def _dead_letter(envelope_json: str) -> None:  # pragma: no cover - needs broker
            import json

            try:
                task_id = json.loads(envelope_json).get("id")
            except Exception:  # noqa: BLE001 - logging only
                task_id = None
            logger.error("task_dead_lettered_on_queue", task_id=task_id)

        # Worker processes resolve handlers/policy through the module-level
        # registry; register this executor there so the task body can find it.
        _set_global_executor(self)

    # ------------------------------------------------------------------
    def register_handler(self, task_type: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
        with self._lock:
            self._handlers[task_type] = resolve_callable(fn)

    def has_handler(self, task_type: str) -> bool:
        with self._lock:
            return task_type in self._handlers

    def submit(self, envelope: TaskEnvelope) -> TaskResult:
        import json

        with self._lock:
            self._submitted += 1
        self._app.send_task(
            self.TASK_NAME,
            args=[json.dumps(envelope.to_dict())],
            queue=self._queue_name,
            priority=max(0, min(10, envelope.priority)),
        )
        return TaskResult(id=envelope.id, status=TaskStatus.QUEUED.value, attempts=envelope.attempts)

    def get(self, task_id: str) -> Optional[TaskResult]:  # pragma: no cover - needs broker
        async_result = self._app.AsyncResult(task_id)
        status_map = {
            "PENDING": TaskStatus.QUEUED.value,
            "STARTED": TaskStatus.RUNNING.value,
            "RETRY": TaskStatus.RETRYING.value,
            "SUCCESS": TaskStatus.SUCCEEDED.value,
            "FAILURE": TaskStatus.FAILED.value,
        }
        if async_result.state == "SUCCESS" and isinstance(async_result.result, dict):
            return TaskResult.from_dict(async_result.result)
        return TaskResult(id=task_id, status=status_map.get(async_result.state, TaskStatus.QUEUED.value))

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "backend": "celery",
                "submitted": self._submitted,
                "queue": self._queue_name,
                "dead_letter_queue": self._dlq_name,
                "handlers": sorted(self._handlers),
            }

    def drain(self) -> List[TaskResult]:
        # Celery state lives in the broker/backend, not in this process.
        return []


# ---------------------------------------------------------------------------
# Module-global registry so Celery worker processes can resolve handlers
# ---------------------------------------------------------------------------

_global_executor: Optional[Any] = None
_global_lock = threading.Lock()


def _set_global_executor(executor: Optional[Any]) -> None:
    global _global_executor
    with _global_lock:
        _global_executor = executor


def _lookup_global_handler(task_type: str) -> Optional[Callable[[Dict[str, Any]], Any]]:
    with _global_lock:
        if _global_executor is None:
            return None
        return _global_executor._handlers.get(task_type)


def _lookup_global_policy() -> Optional[RetryPolicy]:
    with _global_lock:
        if _global_executor is None:
            return None
        return getattr(_global_executor, "_retry_policy", None)


# ============================================================================
# Factory
# ============================================================================

_executor_lock = threading.Lock()
_executor: Optional[Any] = None


def get_executor(refresh: bool = False) -> Any:
    """Return the process-wide executor.

    ``QUEUE_BACKEND`` selects the implementation:

    - ``inline``  -- always the in-process executor (default)
    - ``celery``  -- Celery over Redis; falls back to inline with a warning if
      celery/broker are unavailable, so flows never hard-fail
    """
    global _executor
    with _executor_lock:
        if _executor is not None and not refresh:
            return _executor
        backend = os.environ.get("QUEUE_BACKEND", "inline").strip().lower()
        if backend == "celery":
            try:
                _executor = CeleryExecutor()
                logger.info("queue_backend_selected", backend="celery")
                return _executor
            except Exception as exc:  # pragma: no cover - depends on runtime env
                logger.warning(
                    "queue_backend_fallback",
                    requested="celery",
                    using="inline",
                    error_type=type(exc).__name__,
                )
        _executor = InlineExecutor(dlq_store=get_dlq_store())
        logger.info("queue_backend_selected", backend="inline")
        return _executor


def set_executor(executor: Optional[Any]) -> None:
    """Override the process-wide executor (mainly for tests). ``None`` resets."""
    global _executor
    with _executor_lock:
        _executor = executor
