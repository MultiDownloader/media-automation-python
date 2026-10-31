"""Tests for scraper.compliance: robots, allowlist, rate limiting.

All network access is stubbed -- these tests never touch the wire.
"""

import asyncio
import pytest

from scraper.compliance import (
    Allowlist,
    ComplianceError,
    HostNotAllowed,
    HostRateLimiter,
    RateLimited,
    RobotsDisallowed,
    RobotsPolicy,
    check_compliance,
)


# ---------------------------------------------------------------------------
# RobotsPolicy (stubbed fetcher)
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def _fetcher_returning(status, text=""):
    async def fetcher(url):
        return status, text
    return fetcher


ROBOTS_DISALLOW_ALL = "User-agent: *\nDisallow: /private/\nDisallow: /downloads/\n"
ROBOTS_ALLOW_ALL_PATHS = "User-agent: *\nAllow: /\n"


def test_robots_disallowed_path_refused():
    policy = RobotsPolicy(fetcher=_fetcher_returning(200, ROBOTS_DISALLOW_ALL))
    decision = _run(policy.is_allowed("https://example.com/private/page"))
    assert decision.allowed is False
    assert decision.reason == "robots_disallowed"


def test_robots_allowed_path_passes():
    policy = RobotsPolicy(fetcher=_fetcher_returning(200, ROBOTS_DISALLOW_ALL))
    decision = _run(policy.is_allowed("https://example.com/public/page"))
    assert decision.allowed is True


def test_robots_missing_means_allowed():
    policy = RobotsPolicy(fetcher=_fetcher_returning(404, ""))
    decision = _run(policy.is_allowed("https://example.com/anything"))
    assert decision.allowed is True
    assert decision.reason == "robots_missing"


def test_robots_unreachable_fails_closed():
    async def failing_fetcher(url):
        raise ConnectionError("network down")

    policy = RobotsPolicy(fetcher=failing_fetcher)
    decision = _run(policy.is_allowed("https://example.com/page"))
    assert decision.allowed is False
    assert decision.reason == "robots_unreachable"


def test_robots_server_error_fails_closed():
    policy = RobotsPolicy(fetcher=_fetcher_returning(503, "maintenance"))
    decision = _run(policy.is_allowed("https://example.com/page"))
    assert decision.allowed is False
    assert decision.reason == "robots_unreachable"


def test_robots_caching_prevents_refetch():
    calls = {"n": 0}

    async def counting_fetcher(url):
        calls["n"] += 1
        return 200, ROBOTS_ALLOW_ALL_PATHS

    policy = RobotsPolicy(fetcher=counting_fetcher)
    _run(policy.is_allowed("https://example.com/a"))
    _run(policy.is_allowed("https://example.com/b"))
    _run(policy.is_allowed("https://example.com/c"))
    assert calls["n"] == 1  # cached per host


def test_robots_deny_by_default_ua_specific():
    robots = "User-agent: MediaAutomationBot\nDisallow: /\n\nUser-agent: *\nAllow: /\n"
    policy = RobotsPolicy(fetcher=_fetcher_returning(200, robots))
    decision = _run(policy.is_allowed("https://example.com/page"))
    assert decision.allowed is False  # our UA is fully disallowed


# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------

def test_normalize_host_variants():
    assert Allowlist.normalize("https://Blog.Example.com/path?q=1") == "blog.example.com"
    assert Allowlist.normalize("www.example.com") == "example.com"
    assert Allowlist.normalize("example.com:8080") == "example.com"
    assert Allowlist.normalize("") == ""


def test_empty_allowlist_is_permissive():
    allow = Allowlist([])
    assert allow.is_host_allowed("anything.example.org") is True
    assert "no site configured" in allow.describe_mode()


def test_non_empty_allowlist_restricts():
    allow = Allowlist(["https://www.MySite.com/page"])
    assert allow.is_host_allowed("mysite.com") is True
    assert allow.is_host_allowed("other.com") is False
    assert "1 host(s)" in allow.describe_mode()
    assert allow.hosts == ["mysite.com"]


def test_allowlist_rejects_empty_host():
    allow = Allowlist([])
    assert allow.is_host_allowed("") is False


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

def test_rate_limiter_enforces_min_interval():
    clock = {"t": 0.0}

    async def fake_sleep(seconds):
        clock["t"] += seconds

    limiter = HostRateLimiter(min_interval_s=2.0, time_fn=lambda: clock["t"], sleep_fn=fake_sleep)

    async def scenario():
        first = await limiter.acquire("example.com")
        second = await limiter.acquire("example.com")
        return first, second

    first, second = _run(scenario())
    assert first == 0.0        # nothing waiting the first time
    assert second == pytest.approx(2.0)  # waited one full interval


def test_rate_limiter_hosts_are_independent():
    clock = {"t": 0.0}

    async def fake_sleep(seconds):
        clock["t"] += seconds

    limiter = HostRateLimiter(min_interval_s=5.0, time_fn=lambda: clock["t"], sleep_fn=fake_sleep)

    async def scenario():
        a = await limiter.acquire("a.example.com")
        b = await limiter.acquire("b.example.com")
        return a, b

    a, b = _run(scenario())
    assert a == 0.0 and b == 0.0  # different hosts do not block each other


def test_rate_limiter_stats():
    limiter = HostRateLimiter(min_interval_s=1.0, max_concurrent_per_host=3)
    stats = limiter.stats()
    assert stats["min_interval_s"] == 1.0
    assert stats["max_concurrent_per_host"] == 3


# ---------------------------------------------------------------------------
# check_compliance gate
# ---------------------------------------------------------------------------

def test_check_compliance_refuses_off_allowlist():
    async def scenario():
        await check_compliance(
            "https://blocked.example.com/x",
            RobotsPolicy(fetcher=_fetcher_returning(200, ROBOTS_ALLOW_ALL_PATHS)),
            HostRateLimiter(min_interval_s=0.0),
            Allowlist(["allowed.example.com"]),
        )

    with pytest.raises(HostNotAllowed):
        _run(scenario())


def test_check_compliance_refuses_robots_disallowed():
    async def scenario():
        await check_compliance(
            "https://example.com/private/x",
            RobotsPolicy(fetcher=_fetcher_returning(200, ROBOTS_DISALLOW_ALL)),
            HostRateLimiter(min_interval_s=0.0),
            Allowlist([]),
        )

    with pytest.raises(RobotsDisallowed):
        _run(scenario())


def test_check_compliance_allows_and_holds_slot():
    async def scenario():
        limiter = HostRateLimiter(min_interval_s=0.0)
        decision = await check_compliance(
            "https://example.com/public",
            RobotsPolicy(fetcher=_fetcher_returning(200, ROBOTS_DISALLOW_ALL)),
            limiter,
            Allowlist([]),
        )
        limiter.release("example.com")
        return decision

    decision = _run(scenario())
    assert decision.allowed is True


def test_compliance_error_is_typed():
    assert issubclass(RobotsDisallowed, ComplianceError)
    assert issubclass(HostNotAllowed, ComplianceError)
    assert issubclass(RateLimited, ComplianceError)
