"""
Dead-letter queue (DLQ) storage for the queue/worker layer.

A task that exhausts its retries is not lost: its envelope + final result are
parked here so an operator can inspect it, fix the root cause and replay it.

Two backends are provided:

- :class:`MemoryDeadLetterStore` -- in-process dict; used by default and in
  tests. Never loses state within a process.
- :class:`RedisDeadLetterStore` -- survives restarts; used when ``REDIS_URL``
  is configured and reachable.

Replay always goes through the same executor and preserves the original
envelope id, so attempt history and tracing stay continuous.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .cache import get_redis_client
from .envelope import TaskEnvelope, TaskResult, get_logger, now_iso

logger = get_logger(__name__)

#: Key prefix for Redis-backed entries.
KEY_PREFIX = "dlq:"


# ============================================================================
# Entry
# ============================================================================

@dataclass
class DeadLetterEntry:
    """A parked task: the envelope that failed plus its final result."""

    envelope: TaskEnvelope
    result: TaskResult
    died_at: str = field(default_factory=now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "envelope": self.envelope.to_dict(),
            "result": self.result.to_dict(),
            "died_at": self.died_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeadLetterEntry":
        data = dict(data or {})
        return cls(
            envelope=TaskEnvelope.from_dict(data.get("envelope") or {}),
            result=TaskResult.from_dict(data.get("result") or {"id": "unknown"}),
            died_at=data.get("died_at") or now_iso(),
        )


# ============================================================================
# Store interface
# ============================================================================

class DeadLetterStore:
    """Abstract dead-letter store. Implementations must be thread-safe."""

    def add(self, result: TaskResult, envelope: Optional[TaskEnvelope] = None) -> DeadLetterEntry:
        raise NotImplementedError

    def list(self, limit: int = 50) -> List[DeadLetterEntry]:
        raise NotImplementedError

    def get(self, task_id: str) -> Optional[DeadLetterEntry]:
        raise NotImplementedError

    def replay(self, task_id: str, executor: Any) -> Optional[TaskResult]:
        raise NotImplementedError

    def purge(self, older_than_s: Optional[int] = None) -> int:
        raise NotImplementedError


# ============================================================================
# In-memory implementation
# ============================================================================

class MemoryDeadLetterStore(DeadLetterStore):
    """Thread-safe in-memory dead-letter store.

    Entries are keyed by task id (newest first when listed). ``max_entries``
    bounds memory: the oldest entries are dropped first -- DLQ state for recent
    failures is never lost silently.
    """

    def __init__(self, max_entries: int = 1000, time_fn=time.time) -> None:
        self._entries: Dict[str, DeadLetterEntry] = {}
        self._order: List[str] = []
        self._max_entries = max(1, int(max_entries))
        self._time_fn = time_fn
        self._lock = threading.RLock()
        # died_at is ISO text; keep an epoch stamp per entry for purge().
        self._epoch: Dict[str, float] = {}

    def add(self, result: TaskResult, envelope: Optional[TaskEnvelope] = None) -> DeadLetterEntry:
        env = envelope or TaskEnvelope(task_type="unknown", id=result.id)
        entry = DeadLetterEntry(envelope=env, result=result)
        with self._lock:
            if entry.envelope.id not in self._entries:
                self._order.append(entry.envelope.id)
            self._entries[entry.envelope.id] = entry
            self._epoch[entry.envelope.id] = self._time_fn()
            overflow = len(self._entries) - self._max_entries
            if overflow > 0:
                for task_id in self._order[:overflow]:
                    self._entries.pop(task_id, None)
                    self._epoch.pop(task_id, None)
                self._order = self._order[overflow:]
        logger.warning(
            "dead_letter_recorded",
            task_id=entry.envelope.id,
            task_type=entry.envelope.task_type,
            error_category=entry.result.error_category,
            attempts=entry.result.attempts,
        )
        return entry

    def list(self, limit: int = 50) -> List[DeadLetterEntry]:
        with self._lock:
            ids = list(reversed(self._order))[: max(0, int(limit))]
            return [self._entries[i] for i in ids if i in self._entries]

    def get(self, task_id: str) -> Optional[DeadLetterEntry]:
        with self._lock:
            return self._entries.get(task_id)

    def replay(self, task_id: str, executor: Any) -> Optional[TaskResult]:
        """Re-submit a parked envelope through ``executor``.

        The envelope keeps its original id and carries its attempt history
        forward (``attempts`` is incremented so the replay is visible in
        metrics). Returns the new :class:`TaskResult`, or ``None`` when the
        task id is not parked. Non-retryable terminal entries are refused: the
        user's policy decision must not be silently re-attempted.
        """
        with self._lock:
            entry = self._entries.get(task_id)
        if entry is None:
            return None
        category = entry.result.error_category or ""
        if category in ("compliance_blocked", "auth", "permanent_input"):
            logger.warning(
                "dead_letter_replay_refused",
                task_id=task_id,
                error_category=category,
                reason="terminal_category",
            )
            return None
        envelope = TaskEnvelope.from_dict(entry.envelope.to_dict())
        envelope.attempts = int(entry.result.attempts or envelope.attempts)
        logger.info("dead_letter_replaying", task_id=task_id, attempts=envelope.attempts)
        return executor.submit(envelope)

    def purge(self, older_than_s: Optional[int] = None) -> int:
        """Drop entries. With ``older_than_s`` only entries older than that age."""
        removed = 0
        with self._lock:
            if older_than_s is None:
                removed = len(self._entries)
                self._entries.clear()
                self._epoch.clear()
                self._order.clear()
                return removed
            cutoff = self._time_fn() - max(0, int(older_than_s))
            keep: List[str] = []
            for task_id in self._order:
                if self._epoch.get(task_id, 0) <= cutoff:
                    self._entries.pop(task_id, None)
                    self._epoch.pop(task_id, None)
                    removed += 1
                else:
                    keep.append(task_id)
            self._order = keep
        return removed


# ============================================================================
# Redis implementation
# ============================================================================

class RedisDeadLetterStore(DeadLetterStore):
    """Dead-letter store backed by Redis.

    Entries are JSON strings under ``dlq:<task_id>`` plus a ``dlq:_index`` list
    for ordering. The client comes from :func:`queue_workers.cache.get_redis_client`
    (lazy import; URLs may carry credentials and are never logged).
    """

    def __init__(self, client: Any, max_entries: int = 1000) -> None:
        self._client = client
        self._max_entries = max(1, int(max_entries))
        self._lock = threading.RLock()

    def _key(self, task_id: str) -> str:
        return f"{KEY_PREFIX}{task_id}"

    @property
    def _index_key(self) -> str:
        return f"{KEY_PREFIX}_index"

    def add(self, result: TaskResult, envelope: Optional[TaskEnvelope] = None) -> DeadLetterEntry:
        env = envelope or TaskEnvelope(task_type="unknown", id=result.id)
        entry = DeadLetterEntry(envelope=env, result=result)
        payload = json.dumps(entry.to_dict())
        with self._lock:
            self._client.set(self._key(entry.envelope.id), payload)
            self._client.lrem(self._index_key, 0, entry.envelope.id)
            self._client.lpush(self._index_key, entry.envelope.id)
            self._client.ltrim(self._index_key, 0, self._max_entries - 1)
        logger.warning(
            "dead_letter_recorded",
            task_id=entry.envelope.id,
            task_type=entry.envelope.task_type,
            error_category=entry.result.error_category,
            attempts=entry.result.attempts,
        )
        return entry

    def list(self, limit: int = 50) -> List[DeadLetterEntry]:
        ids = self._client.lrange(self._index_key, 0, max(0, int(limit) - 1)) or []
        entries: List[DeadLetterEntry] = []
        for task_id in ids:
            entry = self.get(task_id if isinstance(task_id, str) else task_id.decode("utf-8"))
            if entry is not None:
                entries.append(entry)
        return entries

    def get(self, task_id: str) -> Optional[DeadLetterEntry]:
        raw = self._client.get(self._key(task_id))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return DeadLetterEntry.from_dict(json.loads(raw))
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.error("dead_letter_decode_failed", task_id=task_id, error_type=type(exc).__name__)
            return None

    def replay(self, task_id: str, executor: Any) -> Optional[TaskResult]:
        entry = self.get(task_id)
        if entry is None:
            return None
        category = entry.result.error_category or ""
        if category in ("compliance_blocked", "auth", "permanent_input"):
            logger.warning(
                "dead_letter_replay_refused",
                task_id=task_id,
                error_category=category,
                reason="terminal_category",
            )
            return None
        envelope = TaskEnvelope.from_dict(entry.envelope.to_dict())
        envelope.attempts = int(entry.result.attempts or envelope.attempts)
        logger.info("dead_letter_replaying", task_id=task_id, attempts=envelope.attempts)
        return executor.submit(envelope)

    def purge(self, older_than_s: Optional[int] = None) -> int:
        ids = self._client.lrange(self._index_key, 0, -1) or []
        removed = 0
        for task_id in ids:
            tid = task_id if isinstance(task_id, str) else task_id.decode("utf-8")
            if older_than_s is None:
                self._client.delete(self._key(tid))
                self._client.lrem(self._index_key, 0, tid)
                removed += 1
                continue
            raw = self._client.get(self._key(tid))
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                died_at = json.loads(raw).get("died_at", "")
                # died_at is ISO 8601 UTC; compare lexicographically by making a
                # cutoff in the same format.
                import datetime as _dt

                cutoff = (_dt.datetime.utcnow() - _dt.timedelta(seconds=max(0, int(older_than_s)))).isoformat() + "Z"
                if died_at <= cutoff:
                    self._client.delete(self._key(tid))
                    self._client.lrem(self._index_key, 0, tid)
                    removed += 1
            except Exception:  # noqa: BLE001 - skip malformed entries
                continue
        return removed


# ============================================================================
# Factory
# ============================================================================

_store_lock = threading.Lock()
_store: Optional[DeadLetterStore] = None


def get_dlq_store(refresh: bool = False) -> DeadLetterStore:
    """Return the process-wide DLQ store.

    ``DLQ_BACKEND`` selects the implementation:

    - ``memory`` -- always in-memory
    - ``redis``  -- Redis via ``REDIS_URL`` (falls back to memory with a
      warning when unavailable so operational flows never hard-fail)
    - ``auto``   (default) -- Redis when ``REDIS_URL`` is set and reachable
    """
    global _store
    with _store_lock:
        if _store is not None and not refresh:
            return _store
        backend = os.environ.get("DLQ_BACKEND", "auto").strip().lower()

        if backend == "memory":
            _store = MemoryDeadLetterStore()
            logger.info("dlq_backend_selected", backend="memory")
            return _store

        if backend in ("redis", "auto"):
            client = get_redis_client(ping=True) if os.environ.get("REDIS_URL") or backend == "redis" else None
            if client is not None:
                _store = RedisDeadLetterStore(client)
                logger.info("dlq_backend_selected", backend="redis")
                return _store
            if backend == "redis":
                logger.warning("dlq_backend_fallback", requested="redis", using="memory")

        _store = MemoryDeadLetterStore()
        logger.info("dlq_backend_selected", backend="memory")
        return _store


def set_dlq_store(store: Optional[DeadLetterStore]) -> None:
    """Override the process-wide DLQ store (mainly for tests). ``None`` resets."""
    global _store
    with _store_lock:
        _store = store
