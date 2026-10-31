"""
Retry policy and error classification for the queue/worker layer.

The category taxonomy is deliberately small and stable so it can be persisted,
dash-boarded and reasoned about:

- ``transient_network``  -- safe to retry (connection resets, timeouts, DNS).
- ``rate_limited``       -- safe to retry with backoff (HTTP 429, throttling).
- ``storage``            -- often retryable (transient object-store / disk issues).
- ``unknown``            -- retryable by default (conservative), but bounded.
- ``compliance_blocked`` -- NEVER retried. The user's policy said no.
- ``auth``               -- NEVER retried. Bad/expired credentials need a human.
- ``permanent_input``    -- NEVER retried. The input is malformed/unsupported.

Compliance, auth and permanent-input failures are terminal by design: retrying
them would either hammer a policy decision that must be respected or loop
forever on input that can never succeed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Tuple


# ============================================================================
# Error categories
# ============================================================================

TRANSIENT_NETWORK = "transient_network"
RATE_LIMITED = "rate_limited"
COMPLIANCE_BLOCKED = "compliance_blocked"
PERMANENT_INPUT = "permanent_input"
AUTH = "auth"
STORAGE = "storage"
UNKNOWN = "unknown"

ALL_CATEGORIES: Tuple[str, ...] = (
    TRANSIENT_NETWORK,
    RATE_LIMITED,
    COMPLIANCE_BLOCKED,
    PERMANENT_INPUT,
    AUTH,
    STORAGE,
    UNKNOWN,
)

#: Categories that must never be retried, regardless of policy configuration.
NON_RETRYABLE_CATEGORIES = frozenset({COMPLIANCE_BLOCKED, PERMANENT_INPUT, AUTH})

#: Categories retried by default.
DEFAULT_RETRY_ON: Tuple[str, ...] = (
    TRANSIENT_NETWORK,
    RATE_LIMITED,
    STORAGE,
    UNKNOWN,
)


# ============================================================================
# Typed errors
# ============================================================================

class TaskError(Exception):
    """Base class for queue-aware errors carrying an error category."""

    category: str = UNKNOWN

    def __init__(self, message: str = "", category: str = None) -> None:
        super().__init__(message)
        if category:
            self.category = category


class TransientNetworkError(TaskError):
    category = TRANSIENT_NETWORK


class RateLimitedError(TaskError):
    category = RATE_LIMITED


class ComplianceBlockedError(TaskError):
    category = COMPLIANCE_BLOCKED


class PermanentInputError(TaskError):
    category = PERMANENT_INPUT


class AuthError(TaskError):
    category = AUTH


class StorageError(TaskError):
    category = STORAGE


# ============================================================================
# Retry policy
# ============================================================================

@dataclass
class RetryPolicy:
    """Exponential-backoff-with-full-jitter retry policy."""

    max_attempts: int = 3
    base_delay_s: float = 1.0
    max_delay_s: float = 60.0
    jitter: bool = True
    retry_on: Tuple[str, ...] = field(default_factory=lambda: DEFAULT_RETRY_ON)

    def compute_delay(self, attempt: int) -> float:
        """Return the delay (seconds) to wait before retrying after ``attempt``.

        ``attempt`` is the 1-based number of the attempt that just failed.
        The exponential window is ``base_delay_s * 2 ** (attempt - 1)`` capped at
        ``max_delay_s``. With ``jitter`` enabled the returned value is a uniform
        sample in ``[0, window]`` (full jitter); without jitter the window itself
        is returned, which makes the sequence monotonically non-decreasing.
        """
        attempt = max(1, int(attempt))
        window = self.base_delay_s * (2 ** (attempt - 1))
        window = min(window, self.max_delay_s)
        if window <= 0:
            return 0.0
        if self.jitter:
            return random.uniform(0.0, window)
        return window

    def should_retry(self, attempt: int, error_category: str) -> bool:
        """Return whether another attempt is allowed.

        ``attempt`` is the number of the attempt that just failed (1-based).
        Compliance / auth / permanent-input categories are always terminal.
        """
        if error_category in NON_RETRYABLE_CATEGORIES:
            return False
        if error_category not in self.retry_on:
            return False
        return attempt < self.max_attempts


# ============================================================================
# Error classification
# ============================================================================

def _status_code(exc: Exception):
    code = getattr(exc, "status_code", None)
    if code is None:
        code = getattr(exc, "status", None)
    if code is None:
        response = getattr(exc, "response", None)
        code = getattr(response, "status_code", None)
    return code


def _contains(haystack: str, needles: Tuple[str, ...]) -> bool:
    return any(n in haystack for n in needles)


def classify_error(exc: Exception) -> str:
    """Map an arbitrary exception to one of :data:`ALL_CATEGORIES`.

    Ordering matters: compliance and auth are checked before the broad
    network/permanent heuristics so a policy block or a 401 is never mistaken
    for something retryable.
    """
    if isinstance(exc, TaskError):
        return exc.category

    explicit = getattr(exc, "error_category", None)
    if explicit in ALL_CATEGORIES:
        return explicit

    name = type(exc).__name__.lower()
    message = str(exc).lower()
    status = _status_code(exc)

    # --- Compliance (terminal, never retried) ---------------------------
    if _contains(name, ("compliance", "policy", "copyright", "dmca")):
        return COMPLIANCE_BLOCKED
    if _contains(
        message,
        (
            "compliance",
            "blocked by policy",
            "content policy",
            "not allowed by policy",
            "dmca",
            "copyright",
            "site_not_allowed",
            "filter_blocked",
        ),
    ):
        return COMPLIANCE_BLOCKED

    # --- Auth (terminal, never retried) ---------------------------------
    if status in (401, 403) or _contains(name, ("auth", "unauthor", "forbidden", "permission")):
        return AUTH
    if _contains(
        message,
        (
            "unauthorized",
            "unauthorised",
            "forbidden",
            "authentication failed",
            "invalid credential",
            "invalid api key",
            "access denied",
            "401 ",
            " 401",
            "403 ",
            " 403",
        ),
    ):
        return AUTH

    # --- Rate limiting (retryable) --------------------------------------
    if status == 429 or _contains(name, ("ratelimit", "rate_limit", "throttl")):
        return RATE_LIMITED
    if _contains(message, ("rate limit", "rate-limit", "too many requests", "429", "throttl")):
        return RATE_LIMITED

    # --- Transient network (retryable) ----------------------------------
    # Checked before OSError/storage because ConnectionError and TimeoutError
    # are both OSError subclasses on modern Python.
    if isinstance(exc, (ConnectionError, TimeoutError, BrokenPipeError, ConnectionResetError)):
        return TRANSIENT_NETWORK
    if _contains(name, ("timeout", "connect", "network", "transport", "dns", "socket")):
        return TRANSIENT_NETWORK
    if _contains(
        message,
        (
            "timed out",
            "timeout",
            "connection reset",
            "connection refused",
            "connection aborted",
            "temporary failure",
            "name resolution",
            "dns",
            "network is unreachable",
        ),
    ):
        return TRANSIENT_NETWORK

    # --- Storage (usually retryable) ------------------------------------
    if isinstance(exc, (OSError, IOError)):
        return STORAGE
    if _contains(name, ("storage", "bucket", "quota", "diskfull", "no_space")):
        return STORAGE
    if _contains(
        message,
        ("no space left", "disk full", "storage", "bucket", "quota exceeded", "enospc"),
    ):
        return STORAGE

    # --- Permanent input (terminal, never retried) ----------------------
    if isinstance(exc, (ValueError, TypeError, KeyError, AttributeError, NotImplementedError)):
        return PERMANENT_INPUT
    if status in (400, 404, 405, 409, 410, 422):
        return PERMANENT_INPUT
    if _contains(
        message,
        ("invalid", "not found", "missing required", "malformed", "unsupported", "does not exist"),
    ):
        return PERMANENT_INPUT

    return UNKNOWN
