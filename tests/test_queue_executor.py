"""Tests for queue_workers.executor: InlineExecutor behaviour.

The Celery executor is exercised only for construction-failure paths (celery
is an optional dependency and CI does not install it).
"""

import asyncio
import threading

import pytest

from queue_workers.dlq import MemoryDeadLetterStore
from queue_workers.envelope import TaskEnvelope, TaskStatus
from queue_workers.executor import (
    CeleryExecutor,
    InlineExecutor,
    get_executor,
    resolve_callable,
    set_executor,
)
from queue_workers.retry import (
    PERMANENT_INPUT,
    TRANSIENT_NETWORK,
    ComplianceBlockedError,
    RetryPolicy,
    TransientNetworkError,
)


def _no_sleep(_seconds):
    return None


def _make(retry=3, dlq=None):
    policy = RetryPolicy(max_attempts=retry, base_delay_s=0.0, jitter=False)
    return InlineExecutor(retry_policy=policy, dlq_store=dlq, sleep_fn=_no_sleep)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_submit_success():
    ex = _make()
    ex.register_handler("echo", lambda payload: {"got": payload.get("x")})
    result = ex.submit(TaskEnvelope(task_type="echo", payload={"x": 1}))
    assert result.status == TaskStatus.SUCCEEDED.value
    assert result.result == {"got": 1}
    assert result.attempts == 1
    assert ex.get(result.id) is result


def test_unknown_task_type_fails_permanently():
    ex = _make()
    result = ex.submit(TaskEnvelope(task_type="nope"))
    assert result.status == TaskStatus.FAILED.value
    assert result.error_category == PERMANENT_INPUT
    assert "No handler registered" in (result.error_message or "")


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------

def test_transient_error_retries_then_succeeds():
    ex = _make(retry=3)
    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientNetworkError("connection reset")
        return {"ok": True}

    ex.register_handler("flaky", flaky)
    result = ex.submit(TaskEnvelope(task_type="flaky"))
    assert result.status == TaskStatus.SUCCEEDED.value
    assert result.attempts == 3
    assert calls["n"] == 3


def test_retries_exhausted_dead_letters():
    dlq = MemoryDeadLetterStore()
    ex = _make(retry=2, dlq=dlq)

    def always_down(payload):
        raise TransientNetworkError("still down")

    ex.register_handler("down", always_down)
    env = TaskEnvelope(task_type="down")
    result = ex.submit(env)
    assert result.status == TaskStatus.DEAD_LETTERED.value
    assert result.error_category == TRANSIENT_NETWORK
    assert result.attempts == 2
    # Persisted to the DLQ for operator replay
    entry = dlq.get(env.id)
    assert entry is not None
    assert entry.envelope.id == env.id


def test_compliance_error_never_retried():
    ex = _make(retry=5)
    calls = {"n": 0}

    def blocked(payload):
        calls["n"] += 1
        raise ComplianceBlockedError("robots.txt disallows")

    ex.register_handler("blocked", blocked)
    result = ex.submit(TaskEnvelope(task_type="blocked"))
    assert result.status == TaskStatus.FAILED.value  # terminal, not dead-lettered
    assert result.attempts == 1
    assert calls["n"] == 1


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------

def test_async_handler_is_awaited():
    ex = _make()

    async def handler(payload):
        await asyncio.sleep(0)
        return {"async": True}

    ex.register_handler("async", handler)
    result = ex.submit(TaskEnvelope(task_type="async"))
    assert result.status == TaskStatus.SUCCEEDED.value
    assert result.result == {"async": True}


def test_resolve_callable_passthrough_for_sync():
    fn = lambda p: p  # noqa: E731
    assert resolve_callable(fn) is fn


# ---------------------------------------------------------------------------
# Concurrency / bookkeeping
# ---------------------------------------------------------------------------

def test_concurrent_submissions_are_tracked():
    ex = _make()
    ex.register_handler("sleepy", lambda payload: payload.get("i"))
    threads = [
        threading.Thread(target=ex.submit, args=(TaskEnvelope(task_type="sleepy", payload={"i": i}),))
        for i in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = ex.stats()
    assert stats["submitted"] == 20
    assert stats["succeeded"] == 20


def test_stats_and_drain():
    ex = _make()
    ex.register_handler("one", lambda p: 1)
    ex.register_handler("two", lambda p: 2)
    ex.submit(TaskEnvelope(task_type="one"))
    ex.submit(TaskEnvelope(task_type="two"))
    stats = ex.stats()
    assert stats["handlers"] == ["one", "two"]
    assert stats["submitted"] == 2
    drained = ex.drain()
    assert len(drained) == 2
    assert ex.drain() == []
    assert ex.stats()["tracked_results"] == 0


def test_result_store_is_bounded():
    ex = InlineExecutor(retry_policy=RetryPolicy(max_attempts=1), sleep_fn=_no_sleep, max_results=5)
    ex.register_handler("x", lambda p: None)
    for _ in range(10):
        ex.submit(TaskEnvelope(task_type="x"))
    assert ex.stats()["tracked_results"] == 5


# ---------------------------------------------------------------------------
# Celery (optional) + factory
# ---------------------------------------------------------------------------

def test_celery_executor_requires_package_or_broker():
    try:
        import celery  # noqa: F401

        pytest.skip("celery installed; construction path covered elsewhere")
    except ImportError:
        with pytest.raises(RuntimeError, match="celery"):
            CeleryExecutor()


def test_get_executor_default_inline(monkeypatch):
    monkeypatch.delenv("QUEUE_BACKEND", raising=False)
    ex = get_executor(refresh=True)
    assert isinstance(ex, InlineExecutor)
    assert ex.stats()["backend"] == "inline"
    set_executor(None)  # reset for other tests
