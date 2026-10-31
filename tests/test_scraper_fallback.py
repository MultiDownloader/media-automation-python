"""Tests for scraper.fallback: SmartFetcher chain behaviour with fake tiers.

No real network, no browser: every tier is a stub. Compliance is stubbed via
a RobotsPolicy with a fake fetcher.
"""

import asyncio

import pytest

from scraper.compliance import Allowlist, HostRateLimiter, RobotsPolicy
from scraper.fallback import (
    BrowserTier,
    FetchOutcome,
    FetchResult,
    FetchTier,
    HttpTier,
    SeleniumTier,
    SmartFetcher,
    _classify_status,
)


def _run(coro):
    return asyncio.run(coro)


def _robots_allow_all():
    async def fetcher(url):
        return 404, ""  # no robots.txt -> everything allowed

    return RobotsPolicy(fetcher=fetcher)


def _robots_disallow_all():
    async def fetcher(url):
        return 200, "User-agent: *\nDisallow: /\n"

    return RobotsPolicy(fetcher=fetcher)


class FakeTier:
    """Tier stub: pops a scripted response per call."""

    def __init__(self, name, script):
        self.name = name
        self.script = list(script)
        self.calls = 0

    async def fetch(self, url, profile):
        self.calls += 1
        action = self.script.pop(0) if self.script else {"status": 500}
        if isinstance(action, Exception):
            raise action
        return FetchResult(
            url=url,
            final_url=url,
            status=action.get("status"),
            html=action.get("html", ""),
            error=action.get("error"),
            tier=self.name,
        )


def _fetcher(tiers, chain, max_attempts=1, allowlist=None, robots=None, delay=0.0):
    return SmartFetcher(
        robots=robots or _robots_allow_all(),
        limiter=HostRateLimiter(min_interval_s=0.0),
        allowlist=allowlist,
        chain=chain,
        tier_fetchers=tiers,
        max_attempts_per_tier=max_attempts,
        retry_base_delay_s=delay,
        sleep_fn=lambda _s: None,
        profile_factory=lambda: None,  # fake tiers ignore the profile
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_http_tier_success_short_circuits():
    http = FakeTier("http", [{"status": 200, "html": "<html>ok</html>"}])
    browser = FakeTier("browser", [{"status": 200, "html": "should not run"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/page"))
    assert outcome.ok is True
    assert outcome.tier_used == "http"
    assert outcome.result.html == "<html>ok</html>"
    assert http.calls == 1
    assert browser.calls == 0


def test_transient_http_failure_falls_back_to_browser():
    http = FakeTier("http", [{"status": 503}])
    browser = FakeTier("browser", [{"status": 200, "html": "rendered"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/spa"))
    assert outcome.ok is True
    assert outcome.tier_used == "browser"
    assert [a.tier for a in outcome.attempts] == ["http", "browser"]


def test_transport_error_is_transient_and_falls_back():
    http = FakeTier("http", [TimeoutError("connection timeout")])
    browser = FakeTier("browser", [{"status": 200, "html": "ok"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/flaky"))
    assert outcome.ok is True
    assert outcome.tier_used == "browser"
    assert "TimeoutError" in outcome.attempts[0].error


def test_same_tier_retries_before_advancing():
    http = FakeTier("http", [{"status": 500}, {"status": 200, "html": "second try"}])
    browser = FakeTier("browser", [{"status": 200}])
    fetcher = _fetcher(
        {FetchTier.HTTP: http, FetchTier.BROWSER: browser},
        [FetchTier.HTTP, FetchTier.BROWSER],
        max_attempts=2,
    )
    outcome = _run(fetcher.fetch("https://example.com/flaky2"))
    assert outcome.ok is True
    assert outcome.tier_used == "http"
    assert http.calls == 2
    assert browser.calls == 0


# ---------------------------------------------------------------------------
# Abort conditions
# ---------------------------------------------------------------------------

def test_permanent_status_aborts_chain():
    http = FakeTier("http", [{"status": 404}])
    browser = FakeTier("browser", [{"status": 200, "html": "x"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/gone"))
    assert outcome.ok is False
    assert outcome.tier_used is None
    assert browser.calls == 0  # 404 will not succeed on another tier
    assert any("404" in e for e in outcome.errors)


def test_site_block_is_reported_not_bypassed():
    http = FakeTier("http", [{"status": 403}])
    browser = FakeTier("browser", [{"status": 200, "html": "sneaky"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/protected"))
    assert outcome.ok is False
    assert browser.calls == 0  # we do NOT try to sneak past the block
    assert any("not bypassing" in e for e in outcome.errors)


def test_robots_disallowed_aborts_before_any_tier():
    http = FakeTier("http", [{"status": 200}])
    browser = FakeTier("browser", [{"status": 200}])
    fetcher = _fetcher(
        {FetchTier.HTTP: http, FetchTier.BROWSER: browser},
        [FetchTier.HTTP, FetchTier.BROWSER],
        robots=_robots_disallow_all(),
    )
    outcome = _run(fetcher.fetch("https://example.com/private"))
    assert outcome.ok is False
    assert http.calls == 0 and browser.calls == 0
    assert any("RobotsDisallowed" in e for e in outcome.errors)


def test_off_allowlist_aborts_before_any_tier():
    http = FakeTier("http", [{"status": 200}])
    fetcher = _fetcher({FetchTier.HTTP: http}, [FetchTier.HTTP], allowlist=Allowlist(["allowed.example.com"]))
    outcome = _run(fetcher.fetch("https://blocked.example.com/x"))
    assert outcome.ok is False
    assert http.calls == 0
    assert any("HostNotAllowed" in e for e in outcome.errors)


def test_all_tiers_exhausted():
    http = FakeTier("http", [{"status": 503}])
    browser = FakeTier("browser", [{"status": 502}])
    selenium = FakeTier("selenium", [{"status": 500}])
    fetcher = _fetcher(
        {FetchTier.HTTP: http, FetchTier.BROWSER: browser, FetchTier.SELENIUM: selenium},
        [FetchTier.HTTP, FetchTier.BROWSER, FetchTier.SELENIUM],
    )
    outcome = _run(fetcher.fetch("https://example.com/down"))
    assert outcome.ok is False
    assert outcome.tier_used is None
    assert any("exhausted" in e for e in outcome.errors)
    assert len(outcome.attempts) == 3


def test_unexpected_tier_exception_is_classified():
    http = FakeTier("http", [RuntimeError("tier exploded")])
    browser = FakeTier("browser", [{"status": 200, "html": "ok"}])
    fetcher = _fetcher({FetchTier.HTTP: http, FetchTier.BROWSER: browser}, [FetchTier.HTTP, FetchTier.BROWSER])
    outcome = _run(fetcher.fetch("https://example.com/boom"))
    assert outcome.ok is True  # recovered on the next tier
    assert outcome.attempts[0].error_category == "permanent" or outcome.attempts[0].error_category == "transient"


# ---------------------------------------------------------------------------
# Outcome reporting
# ---------------------------------------------------------------------------

def test_outcome_event_shape():
    http = FakeTier("http", [{"status": 200, "html": "x"}])
    fetcher = _fetcher({FetchTier.HTTP: http}, [FetchTier.HTTP])
    outcome = _run(fetcher.fetch("https://example.com/page"))
    event = outcome.to_event()
    assert event["event"] == "fetch_outcome"
    assert event["url_host"] == "example.com"
    assert event["ok"] is True
    assert event["tier_used"] == "http"
    assert isinstance(event["attempts"], list)
    assert event["compliance"]["allowed"] is True


def test_duration_recorded():
    http = FakeTier("http", [{"status": 200, "html": "x"}])
    fetcher = _fetcher({FetchTier.HTTP: http}, [FetchTier.HTTP])
    outcome = _run(fetcher.fetch("https://example.com/page"))
    assert outcome.duration_ms >= 0


# ---------------------------------------------------------------------------
# Status classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "status,expected",
    [(200, "ok"), (404, "permanent"), (403, "compliance_blocked"), (429, "compliance_blocked"),
     (503, "transient"), (500, "transient"), (410, "permanent")],
)
def test_classify_status(status, expected):
    assert _classify_status(status) == expected


# ---------------------------------------------------------------------------
# Default tiers construct without optional deps (lazy imports)
# ---------------------------------------------------------------------------

def test_default_tiers_are_lazy():
    fetcher = SmartFetcher(robots=_robots_allow_all(), limiter=HostRateLimiter(min_interval_s=0.0))
    assert isinstance(fetcher._tier_fetcher(FetchTier.HTTP), HttpTier)
    assert isinstance(fetcher._tier_fetcher(FetchTier.BROWSER), BrowserTier)
    assert isinstance(fetcher._tier_fetcher(FetchTier.SELENIUM), SeleniumTier)
