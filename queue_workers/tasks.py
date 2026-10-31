"""
Task handlers and the public ``enqueue`` entry point.

Handlers wrap the platform's existing async modules (discovery, processor,
uploaders, analytics) so the same work can run inline (in-process) or on
Celery workers without code changes. Imports are lazy and defensive: a task
whose underlying module is unavailable fails with a terminal
``permanent_input`` classification instead of looping retries.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional

from .cache import get_cache
from .envelope import TaskEnvelope, TaskResult, TaskStatus, get_logger, now_iso
from .executor import get_executor
from .retry import PermanentInputError

logger = get_logger(__name__)

#: Default TTL for idempotency keys (completed tasks are not re-executed).
IDEMPOTENCY_TTL_S = 24 * 3600

#: Task types registered by :func:`register_default_handlers`.
TASK_DISCOVERY_POLL = "discovery.poll"
TASK_JOB_PROCESS = "job.process"
TASK_UPLOAD_PERFORM = "upload.perform"
TASK_ANALYTICS_FLUSH = "analytics.flush"

DEFAULT_TASK_TYPES = (
    TASK_DISCOVERY_POLL,
    TASK_JOB_PROCESS,
    TASK_UPLOAD_PERFORM,
    TASK_ANALYTICS_FLUSH,
)


# ============================================================================
# Async bridge (module-local to keep executor imports minimal)
# ============================================================================

def _run(coro: Any) -> Any:
    """Run a coroutine to completion from sync handler code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Called from inside an already-running loop (rare, e.g. notebook or async
    # scheduler driving the inline executor): use a fresh loop on a thread so
    # we never re-enter or block the caller's loop.
    import threading

    result: list = []
    error: list = []

    def _thread() -> None:
        try:
            result.append(asyncio.run(coro))
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            error.append(exc)

    t = threading.Thread(target=_thread, name="queue-workers-task", daemon=True)
    t.start()
    t.join()
    if error:
        raise error[0]
    return result[0]


def _jsonable(value: Any) -> Any:
    """Best-effort conversion of handler output into JSON-safe data."""
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# ============================================================================
# Default handlers (lazy imports of the existing platform modules)
# ============================================================================

async def handle_discovery_poll(payload: Dict[str, Any]) -> Dict[str, Any]:
    """``discovery.poll`` -- run one discovery sweep across enabled sources."""
    from discovery.engine import run_discovery_once

    summary = await run_discovery_once()
    return _jsonable(summary)


async def handle_job_process(payload: Dict[str, Any]) -> Dict[str, Any]:
    """``job.process`` -- claim and process one pending job from the queue.

    Payload (all optional): ``runner_id`` -- identity for job claiming.
    """
    from processor import Processor

    runner_id = payload.get("runner_id")
    processor = Processor(runner_id=runner_id)
    processed = await processor.process_next_job()
    return {"processed": bool(processed), "runner_id": runner_id}


async def handle_upload_perform(payload: Dict[str, Any]) -> Dict[str, Any]:
    """``upload.perform`` -- upload one file to a configured destination.

    Payload: ``file_path`` (required), ``destination_type`` (required),
    ``config`` (optional destination-specific settings dict).
    """
    file_path = payload.get("file_path")
    destination_type = payload.get("destination_type")
    if not file_path or not destination_type:
        raise PermanentInputError(
            "upload.perform requires 'file_path' and 'destination_type' in the payload"
        )

    from uploaders.base import create_uploader

    config: Dict[str, Any] = dict(payload.get("config") or {})
    uploader = create_uploader(str(destination_type), config)
    if uploader is None:
        raise PermanentInputError(f"Unknown destination type: {destination_type!r}")

    result = await uploader.upload(str(file_path), config)
    return _jsonable(result)


async def handle_analytics_flush(payload: Dict[str, Any]) -> Dict[str, Any]:
    """``analytics.flush`` -- flush buffered analytics events."""
    from analytics.collector import get_collector

    collector = get_collector()
    flush = getattr(collector, "flush", None)
    if not callable(flush):
        # Not an error: nothing buffered to push yet.
        return {"flushed": 0, "note": "collector has no flush()"}
    flushed = await flush()
    return {"flushed": int(flushed or 0)}


#: task_type -> handler callable
HANDLERS: Dict[str, Any] = {
    TASK_DISCOVERY_POLL: handle_discovery_poll,
    TASK_JOB_PROCESS: handle_job_process,
    TASK_UPLOAD_PERFORM: handle_upload_perform,
    TASK_ANALYTICS_FLUSH: handle_analytics_flush,
}


def register_default_handlers(executor: Any = None) -> Any:
    """Register the default handlers on ``executor`` (or the process default)."""
    ex = executor if executor is not None else get_executor()
    for task_type, handler in HANDLERS.items():
        ex.register_handler(task_type, handler)
    logger.info("queue_handlers_registered", task_types=sorted(HANDLERS), backend=ex.stats().get("backend"))
    return ex


# ============================================================================
# Public enqueue API
# ============================================================================

def _idempotency_cache_key(task_type: str, key: str) -> str:
    return f"queue:idem:{task_type}:{key}"


def enqueue(
    task_type: str,
    payload: Optional[Dict[str, Any]] = None,
    executor: Any = None,
    idempotency_key: Optional[str] = None,
    max_attempts: Optional[int] = None,
    priority: int = 0,
    timeout_s: Optional[int] = None,
) -> TaskResult:
    """Submit work to the queue and return its :class:`TaskResult`.

    With ``idempotency_key`` set, a task that already completed with the same
    key is not re-executed: the previously stored result is returned instead
    (cached for ``IDEMPOTENCY_TTL_S``). This keeps duplicated scheduler ticks
    or operator double-clicks from doing the same work twice.
    """
    ex = executor if executor is not None else get_executor()
    envelope = TaskEnvelope(
        task_type=task_type,
        payload=dict(payload or {}),
        idempotency_key=idempotency_key,
        priority=priority,
        timeout_s=timeout_s,
    )
    if max_attempts is not None:
        envelope.max_attempts = max(1, int(max_attempts))

    cache = get_cache()
    cache_key = None
    if idempotency_key:
        cache_key = _idempotency_cache_key(task_type, idempotency_key)
        cached = cache.get(cache_key)
        if cached:
            try:
                prior = TaskResult.from_dict(json.loads(cached))
                logger.info(
                    "task_deduplicated",
                    task_id=envelope.id,
                    task_type=task_type,
                    idempotency_key=idempotency_key,
                    original_task_id=prior.id,
                )
                return prior
            except Exception as exc:  # noqa: BLE001 - corrupt cache entry: re-run
                logger.warning(
                    "idempotency_cache_invalid",
                    task_type=task_type,
                    error_type=type(exc).__name__,
                )

    result = ex.submit(envelope)

    if cache_key and result.status == TaskStatus.SUCCEEDED.value:
        try:
            cache.set(
                cache_key,
                json.dumps(result.to_dict(), default=str),
                ttl_s=IDEMPOTENCY_TTL_S,
            )
        except Exception as exc:  # noqa: BLE001 - cache is an optimization only
            logger.warning("idempotency_cache_store_failed", task_type=task_type, error_type=type(exc).__name__)

    return result
