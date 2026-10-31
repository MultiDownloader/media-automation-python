"""Tests for queue_workers.envelope: serialization and trace helpers."""

import pytest

from queue_workers.envelope import (
    TaskEnvelope,
    TaskResult,
    TaskStatus,
    format_traceparent,
    new_span_id,
    new_trace_id,
)


def test_envelope_defaults():
    env = TaskEnvelope(task_type="job.process")
    assert env.task_type == "job.process"
    assert env.attempts == 0
    assert env.max_attempts == 3
    assert len(env.id) == 32  # uuid4().hex
    assert len(env.trace_id) == 32
    assert env.payload == {}


def test_envelope_round_trip():
    env = TaskEnvelope(
        task_type="discovery.poll",
        payload={"site": "example.com", "depth": 2},
        max_attempts=5,
        idempotency_key="abc-123",
        priority=3,
        timeout_s=90,
        dead_letter_after=7,
    )
    restored = TaskEnvelope.from_dict(env.to_dict())
    assert restored == env
    assert restored.payload == {"site": "example.com", "depth": 2}
    assert restored.idempotency_key == "abc-123"


def test_envelope_from_dict_tolerates_extra_keys():
    data = TaskEnvelope(task_type="t").to_dict()
    data["future_field"] = "ignored"
    restored = TaskEnvelope.from_dict(data)
    assert restored.task_type == "t"


def test_envelope_trace_helpers():
    env = TaskEnvelope(task_type="t")
    traceparent = env.to_traceparent()
    assert traceparent.startswith("00-")
    parts = traceparent.split("-")
    assert parts[1] == env.trace_id
    assert len(parts[2]) == 16
    assert parts[3] in ("00", "01")


def test_trace_ids_never_all_zero():
    for _ in range(20):
        assert new_trace_id() != "0" * 32
        assert new_span_id() != "0" * 16


def test_format_traceparent():
    value = format_traceparent("a" * 32, "b" * 16, sampled=False)
    assert value == f"00-{'a' * 32}-{'b' * 16}-00"


def test_task_result_round_trip():
    result = TaskResult(
        id="task-1",
        status=TaskStatus.SUCCEEDED.value,
        result={"processed": True},
        attempts=2,
        duration_ms=123,
        started_at="2026-01-01T00:00:00Z",
        finished_at="2026-01-01T00:00:01Z",
    )
    restored = TaskResult.from_dict(result.to_dict())
    assert restored == result
    assert restored.succeeded is True
    assert restored.dead_lettered is False


def test_task_result_status_properties():
    dl = TaskResult(id="x", status=TaskStatus.DEAD_LETTERED.value)
    assert dl.dead_lettered is True
    assert dl.succeeded is False
