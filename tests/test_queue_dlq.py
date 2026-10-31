"""Tests for queue_workers.dlq: dead-letter store semantics + replay."""

import pytest

from queue_workers.dlq import DeadLetterEntry, MemoryDeadLetterStore, get_dlq_store, set_dlq_store
from queue_workers.envelope import TaskEnvelope, TaskResult, TaskStatus
from queue_workers.executor import InlineExecutor
from queue_workers.retry import RetryPolicy, TransientNetworkError


def _no_sleep(_s):
    return None


def _dead_lettered_pair(task_type="down", category="transient_network"):
    env = TaskEnvelope(task_type=task_type)
    result = TaskResult(
        id=env.id,
        status=TaskStatus.DEAD_LETTERED.value,
        error_category=category,
        error_message="boom",
        attempts=3,
    )
    return env, result


# ---------------------------------------------------------------------------
# add / list / get
# ---------------------------------------------------------------------------

def test_add_and_get():
    store = MemoryDeadLetterStore()
    env, result = _dead_lettered_pair()
    entry = store.add(result, envelope=env)
    assert entry.envelope.id == env.id
    fetched = store.get(env.id)
    assert fetched is not None
    assert fetched.result.attempts == 3
    assert fetched.died_at.endswith("Z") or "T" in fetched.died_at


def test_list_newest_first_and_limit():
    store = MemoryDeadLetterStore()
    ids = []
    for i in range(5):
        env, result = _dead_lettered_pair()
        env.id = f"task-{i}"
        result.id = env.id
        store.add(result, envelope=env)
        ids.append(env.id)
    listed = store.list(limit=3)
    assert [e.envelope.id for e in listed] == list(reversed(ids))[:3]


def test_add_without_envelope_synthesizes_one():
    store = MemoryDeadLetterStore()
    _, result = _dead_lettered_pair()
    entry = store.add(result)
    assert entry.envelope.id == result.id
    assert entry.envelope.task_type == "unknown"


def test_max_entries_bound():
    store = MemoryDeadLetterStore(max_entries=3)
    for i in range(6):
        env, result = _dead_lettered_pair()
        env.id = f"e{i}"
        result.id = env.id
        store.add(result, envelope=env)
    assert len(store.list(limit=10)) == 3
    assert store.get("e0") is None  # oldest dropped
    assert store.get("e5") is not None


def test_entry_round_trip():
    env, result = _dead_lettered_pair()
    entry = DeadLetterEntry(envelope=env, result=result)
    restored = DeadLetterEntry.from_dict(entry.to_dict())
    assert restored.envelope == env
    assert restored.result == result


# ---------------------------------------------------------------------------
# purge
# ---------------------------------------------------------------------------

def test_purge_all():
    store = MemoryDeadLetterStore()
    for _ in range(3):
        env, result = _dead_lettered_pair()
        store.add(result, envelope=env)
    assert store.purge() == 3
    assert store.list() == []


def test_purge_older_than():
    now = {"t": 1000.0}
    store = MemoryDeadLetterStore(time_fn=lambda: now["t"])
    env1, r1 = _dead_lettered_pair()
    store.add(r1, envelope=env1)
    now["t"] = 2000.0  # 1000s later
    env2, r2 = _dead_lettered_pair()
    store.add(r2, envelope=env2)
    removed = store.purge(older_than_s=500)
    assert removed == 1
    assert store.get(env1.id) is None
    assert store.get(env2.id) is not None


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------

def test_replay_preserves_id_and_advances_attempts():
    store = MemoryDeadLetterStore()
    env, result = _dead_lettered_pair()
    store.add(result, envelope=env)

    policy = RetryPolicy(max_attempts=1, base_delay_s=0.0, jitter=False)
    ex = InlineExecutor(retry_policy=policy, sleep_fn=_no_sleep)
    ex.register_handler("down", lambda payload: {"recovered": True})

    replayed = store.replay(env.id, ex)
    assert replayed is not None
    assert replayed.id == env.id  # same envelope id: history stays continuous
    assert replayed.status == TaskStatus.SUCCEEDED.value
    assert replayed.attempts == result.attempts + 1  # 3 -> 4


def test_replay_unknown_id_returns_none():
    store = MemoryDeadLetterStore()
    policy = RetryPolicy(max_attempts=1, base_delay_s=0.0, jitter=False)
    ex = InlineExecutor(retry_policy=policy, sleep_fn=_no_sleep)
    assert store.replay("missing", ex) is None


@pytest.mark.parametrize("category", ["compliance_blocked", "auth", "permanent_input"])
def test_replay_refuses_terminal_categories(category):
    store = MemoryDeadLetterStore()
    env, result = _dead_lettered_pair(category=category)
    store.add(result, envelope=env)
    policy = RetryPolicy(max_attempts=1, base_delay_s=0.0, jitter=False)
    ex = InlineExecutor(retry_policy=policy, sleep_fn=_no_sleep)
    ex.register_handler("down", lambda payload: "should not run")
    assert store.replay(env.id, ex) is None


def test_replay_failed_task_goes_through_retry_logic():
    """Replaying a dead-lettered task re-runs it; if it fails again it lands
    right back in the DLQ with continuous attempts."""
    store = MemoryDeadLetterStore()
    env, result = _dead_lettered_pair()
    store.add(result, envelope=env)

    policy = RetryPolicy(max_attempts=1, base_delay_s=0.0, jitter=False)
    ex = InlineExecutor(retry_policy=policy, dlq_store=store, sleep_fn=_no_sleep)

    def still_down(payload):
        raise TransientNetworkError("nope")

    ex.register_handler("down", still_down)
    replayed = store.replay(env.id, ex)
    assert replayed.status == TaskStatus.DEAD_LETTERED.value
    assert replayed.attempts == result.attempts + 1
    # DLQ entry now carries the newest attempt count
    assert store.get(env.id).result.attempts == result.attempts + 1


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_get_dlq_store_default_memory(monkeypatch):
    monkeypatch.delenv("DLQ_BACKEND", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    store = get_dlq_store(refresh=True)
    assert isinstance(store, MemoryDeadLetterStore)
    set_dlq_store(None)  # reset for other tests
