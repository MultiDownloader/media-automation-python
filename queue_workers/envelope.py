"""
Task envelopes and results for the queue/worker infrastructure.

This module is intentionally dependency-free (stdlib only) so it can be imported
by tests and by every executor implementation without pulling in Celery, Redis or
any of the heavier project dependencies.

Serialization is plain JSON. Payloads MUST NOT contain secrets, API keys or
signed URLs -- callers are responsible for keeping credential material out of
the queue and passing references (ids) instead.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


# ============================================================================
# Time / trace helpers (W3C Trace Context compatible)
# ============================================================================

def now_iso() -> str:
    """Current UTC time in ISO 8601 format with a trailing ``Z``."""
    return datetime.utcnow().isoformat() + "Z"


def new_trace_id() -> str:
    """Return a 32 hex-char W3C trace-id (never all-zero)."""
    tid = uuid.uuid4().hex
    return tid if tid != "0" * 32 else new_trace_id()


def new_span_id() -> str:
    """Return a 16 hex-char W3C span-id (never all-zero)."""
    sid = uuid.uuid4().hex[:16]
    return sid if sid != "0" * 16 else new_span_id()


def format_traceparent(trace_id: str, span_id: str, sampled: bool = True) -> str:
    """Format a W3C ``traceparent`` header value."""
    flags = "01" if sampled else "00"
    return f"00-{trace_id}-{span_id}-{flags}"


# ============================================================================
# Logging helper (structlog when available, stdlib logging otherwise)
# ============================================================================

class _LoggerAdapter:
    """Tiny adapter that logs structured kwargs with structlog or stdlib logging.

    Using this keeps the ``queue_workers`` package import-safe even when
    ``structlog`` is not installed (it is a core dependency of this repo, but the
    queue layer must not assume it).
    """

    __slots__ = ("_log", "_structured")

    def __init__(self, name: str) -> None:
        try:  # pragma: no cover - exercised implicitly
            import structlog

            self._log = structlog.get_logger(name)
            self._structured = True
        except Exception:  # pragma: no cover - fallback path
            self._log = logging.getLogger(name)
            self._structured = False

    def _emit(self, level: str, event: str, **kw: Any) -> None:
        if self._structured:
            getattr(self._log, level)(event, **kw)
        else:
            rendered = " ".join(f"{k}={v}" for k, v in kw.items())
            getattr(self._log, level)(f"{event} {rendered}".strip())

    def debug(self, event: str, **kw: Any) -> None:
        self._emit("debug", event, **kw)

    def info(self, event: str, **kw: Any) -> None:
        self._emit("info", event, **kw)

    def warning(self, event: str, **kw: Any) -> None:
        self._emit("warning", event, **kw)

    def error(self, event: str, **kw: Any) -> None:
        self._emit("error", event, **kw)


def get_logger(name: str) -> _LoggerAdapter:
    """Return an init-safe structured logger."""
    return _LoggerAdapter(name)


# ============================================================================
# Status enum
# ============================================================================

class TaskStatus(str, Enum):
    """Lifecycle states for a queued task."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"


# ============================================================================
# Envelope
# ============================================================================

@dataclass
class TaskEnvelope:
    """A unit of work handed to an executor.

    Field order keeps ``task_type`` first (the only required argument) so the
    object is convenient to construct positionally as well as by keyword.
    """

    task_type: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    payload: Dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    max_attempts: int = 3
    created_at: str = field(default_factory=now_iso)
    idempotency_key: Optional[str] = None
    trace_id: str = field(default_factory=new_trace_id)
    priority: int = 0
    timeout_s: Optional[int] = None
    # Optional extra guard: once ``attempts`` reaches this value a *retryable*
    # failure is routed straight to the dead-letter queue instead of being
    # retried up to ``max_attempts``. ``None`` disables the override.
    dead_letter_after: Optional[int] = None

    # ------------------------------------------------------------------
    # Trace helpers
    # ------------------------------------------------------------------
    def to_traceparent(self, span_id: Optional[str] = None, sampled: bool = True) -> str:
        """Return a W3C ``traceparent`` for this task's trace."""
        return format_traceparent(self.trace_id, span_id or new_span_id(), sampled)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a plain, JSON-serializable representation of the envelope."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskEnvelope":
        """Rebuild an envelope from ``to_dict`` output (tolerant to extra keys)."""
        data = dict(data or {})
        trace_id = data.get("trace_id") or new_trace_id()
        created_at = data.get("created_at") or now_iso()
        return cls(
            task_type=data["task_type"],
            id=data.get("id") or uuid.uuid4().hex,
            payload=dict(data.get("payload") or {}),
            attempts=int(data.get("attempts", 0) or 0),
            max_attempts=int(data.get("max_attempts", 3) or 3),
            created_at=created_at,
            idempotency_key=data.get("idempotency_key"),
            trace_id=trace_id,
            priority=int(data.get("priority", 0) or 0),
            timeout_s=data.get("timeout_s"),
            dead_letter_after=data.get("dead_letter_after"),
        )


# ============================================================================
# Result
# ============================================================================

@dataclass
class TaskResult:
    """Outcome of executing (an attempt of) a :class:`TaskEnvelope`."""

    id: str
    status: str = TaskStatus.QUEUED.value
    result: Any = None
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    attempts: int = 0
    duration_ms: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain, JSON-serializable representation of the result."""
        data = asdict(self)
        # ``status`` is stored as the plain string value for wire stability.
        if isinstance(self.status, TaskStatus):
            data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        """Rebuild a result from ``to_dict`` output (tolerant to extra keys)."""
        data = dict(data or {})
        status = data.get("status", TaskStatus.QUEUED.value)
        if isinstance(status, TaskStatus):
            status = status.value
        return cls(
            id=data["id"],
            status=status,
            result=data.get("result"),
            error_category=data.get("error_category"),
            error_message=data.get("error_message"),
            attempts=int(data.get("attempts", 0) or 0),
            duration_ms=data.get("duration_ms"),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
        )

    @property
    def succeeded(self) -> bool:
        return self.status == TaskStatus.SUCCEEDED.value

    @property
    def dead_lettered(self) -> bool:
        return self.status == TaskStatus.DEAD_LETTERED.value
