"""
Queue/worker infrastructure for the media automation platform.

Layers (bottom-up):

- ``envelope``  -- TaskEnvelope / TaskResult / statuses + W3C trace helpers
- ``retry``     -- RetryPolicy, error classification (terminal vs retryable)
- ``cache``     -- TTL cache (Redis or in-memory) used for idempotency
- ``dlq``       -- dead-letter store (memory or Redis) + replay
- ``executor``  -- InlineExecutor (default) and CeleryExecutor (scale-out)
- ``tasks``     -- default handlers wired to the existing platform modules

Everything degrades gracefully: with only the core dependencies installed the
inline executor and in-memory stores work out of the box; Celery/Redis are
optional accelerators selected via environment variables (see README.md).
"""

from .dlq import DeadLetterStore, MemoryDeadLetterStore, RedisDeadLetterStore, get_dlq_store, set_dlq_store
from .envelope import TaskEnvelope, TaskResult, TaskStatus, format_traceparent, new_trace_id, now_iso
from .executor import CeleryExecutor, InlineExecutor, get_executor, set_executor
from .retry import RetryPolicy, classify_error
from .tasks import enqueue, register_default_handlers

__all__ = [
    "CeleryExecutor",
    "DeadLetterStore",
    "InlineExecutor",
    "MemoryDeadLetterStore",
    "RedisDeadLetterStore",
    "RetryPolicy",
    "TaskEnvelope",
    "TaskResult",
    "TaskStatus",
    "classify_error",
    "enqueue",
    "format_traceparent",
    "get_dlq_store",
    "get_executor",
    "new_trace_id",
    "now_iso",
    "register_default_handlers",
    "set_dlq_store",
    "set_executor",
]
