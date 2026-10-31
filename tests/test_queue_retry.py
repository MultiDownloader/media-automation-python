"""Tests for queue_workers.retry: backoff, should_retry, classification."""

import pytest

from queue_workers.retry import (
    AUTH,
    COMPLIANCE_BLOCKED,
    NON_RETRYABLE_CATEGORIES,
    PERMANENT_INPUT,
    RATE_LIMITED,
    STORAGE,
    TRANSIENT_NETWORK,
    UNKNOWN,
    AuthError,
    ComplianceBlockedError,
    PermanentInputError,
    RateLimitedError,
    RetryPolicy,
    TransientNetworkError,
    classify_error,
)


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------

def test_backoff_no_jitter_is_monotonic_and_capped():
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=10.0, jitter=False)
    delays = [policy.compute_delay(a) for a in range(1, 6)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 10.0]  # exponential then capped


def test_backoff_jitter_within_bounds():
    policy = RetryPolicy(base_delay_s=2.0, max_delay_s=16.0, jitter=True)
    for attempt in range(1, 6):
        window = min(2.0 * (2 ** (attempt - 1)), 16.0)
        for _ in range(25):
            delay = policy.compute_delay(attempt)
            assert 0.0 <= delay <= window


def test_backoff_attempt_floor():
    policy = RetryPolicy(base_delay_s=1.0, jitter=False)
    assert policy.compute_delay(0) == 1.0
    assert policy.compute_delay(-3) == 1.0


# ---------------------------------------------------------------------------
# should_retry matrix
# ---------------------------------------------------------------------------

def test_should_retry_respects_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    assert policy.should_retry(1, TRANSIENT_NETWORK) is True
    assert policy.should_retry(2, TRANSIENT_NETWORK) is True
    assert policy.should_retry(3, TRANSIENT_NETWORK) is False


@pytest.mark.parametrize("category", sorted(NON_RETRYABLE_CATEGORIES))
def test_terminal_categories_never_retried(category):
    policy = RetryPolicy(max_attempts=10, retry_on=(TRANSIENT_NETWORK, RATE_LIMITED, STORAGE, UNKNOWN, category))
    assert policy.should_retry(1, category) is False


def test_non_configured_category_not_retried():
    policy = RetryPolicy(max_attempts=3, retry_on=(TRANSIENT_NETWORK,))
    assert policy.should_retry(1, RATE_LIMITED) is False


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def test_classify_typed_errors():
    assert classify_error(TransientNetworkError("boom")) == TRANSIENT_NETWORK
    assert classify_error(RateLimitedError("429")) == RATE_LIMITED
    assert classify_error(ComplianceBlockedError("policy")) == COMPLIANCE_BLOCKED
    assert classify_error(PermanentInputError("bad input")) == PERMANENT_INPUT
    assert classify_error(AuthError("nope")) == AUTH


def test_classify_builtin_exceptions():
    assert classify_error(TimeoutError("timed out")) == TRANSIENT_NETWORK
    assert classify_error(ConnectionResetError("reset")) == TRANSIENT_NETWORK
    assert classify_error(ValueError("invalid url")) == PERMANENT_INPUT
    assert classify_error(KeyError("missing")) == PERMANENT_INPUT


def test_classify_status_bearing_exceptions():
    class FakeHttpError(Exception):
        def __init__(self, status_code):
            super().__init__(f"HTTP {status_code}")
            self.status_code = status_code

    assert classify_error(FakeHttpError(401)) == AUTH
    assert classify_error(FakeHttpError(403)) == AUTH
    assert classify_error(FakeHttpError(429)) == RATE_LIMITED
    assert classify_error(FakeHttpError(404)) == PERMANENT_INPUT
    assert classify_error(FakeHttpError(503)) == UNKNOWN  # no message hints


def test_classify_message_heuristics():
    assert classify_error(Exception("Blocked by content policy: adult")) == COMPLIANCE_BLOCKED
    assert classify_error(Exception("Too many requests, slow down")) == RATE_LIMITED
    assert classify_error(Exception("invalid api key supplied")) == AUTH


def test_classify_unknown_default():
    assert classify_error(Exception("something odd happened")) == UNKNOWN
