"""
Compliance layer for the scraping stack: robots.txt, rate limits, allowlist.

COMPLIANCE STANCE (non-negotiable):
- robots.txt is honoured. When a site's robots.txt disallows a path for our
  user-agent, the fetch is refused -- no exceptions, no "just try anyway".
- When robots.txt exists but cannot be fetched, we DENY (fail closed).
- Every host gets a minimum interval between requests and a concurrency cap.
- The product-level site allowlist is enforced before anything else runs.
- There is NO CAPTCHA solving, anti-bot bypass, Cloudflare-challenge bypass,
  DRM circumvention or paywall bypass anywhere in this package. If a site
  blocks automated access, that is reported as a structured permanent error
  and the source is skipped.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from queue_workers.envelope import get_logger

logger = get_logger(__name__)

#: User-agent token robots.txt rules are matched against.
USER_AGENT = "MediaAutomationBot"

#: How long a fetched robots.txt stays valid (seconds).
DEFAULT_ROBOTS_TTL_S = 6 * 3600


# ============================================================================
# Structured exceptions
# ============================================================================

class ComplianceError(Exception):
    """Base class for compliance refusals (terminal, never retried)."""

    category = "compliance_blocked"


class RobotsDisallowed(ComplianceError):
    """robots.txt disallows this URL for our user-agent."""


class RateLimited(ComplianceError):
    """Per-host rate limit reached; the caller should wait and retry later."""


class HostNotAllowed(ComplianceError):
    """Host is outside the configured site allowlist."""


# ============================================================================
# Decision record
# ============================================================================

@dataclass
class ComplianceDecision:
    """Why a request may (or may not) proceed. Kept for audit/analytics."""

    allowed: bool
    reason: str
    host: str = ""
    robots_allowed: Optional[bool] = None
    rate_limit_wait_s: float = 0.0
    detail: str = ""

    def to_dict(self) -> Dict[str, object]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "host": self.host,
            "robots_allowed": self.robots_allowed,
            "rate_limit_wait_s": self.rate_limit_wait_s,
            "detail": self.detail,
        }


# ============================================================================
# robots.txt
# ============================================================================

@dataclass
class _RobotsEntry:
    parser: RobotFileParser
    fetched_at: float
    status: str = "ok"  # ok | missing | unreachable


class RobotsPolicy:
    """Fetch, cache and evaluate robots.txt per host.

    - ``fetcher``: an optional async callable ``async def fetcher(url) ->
      (status_code, text)`` injected by the caller (usually backed by httpx).
      When omitted, httpx is imported lazily and used directly.
    - Missing robots.txt (404) -> everything allowed for that host.
    - Existing but unreachable (5xx/timeout) -> everything DENIED (fail closed).
    - Entries expire after ``ttl_s`` so rule changes are picked up eventually.
    """

    def __init__(
        self,
        fetcher=None,
        ttl_s: int = DEFAULT_ROBOTS_TTL_S,
        user_agent: str = USER_AGENT,
        time_fn=time.time,
    ) -> None:
        self._fetcher = fetcher
        self._ttl = int(ttl_s)
        self._user_agent = user_agent
        self._time_fn = time_fn
        self._cache: Dict[str, _RobotsEntry] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    async def _default_fetcher(self, url: str):
        import httpx

        async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            return resp.status_code, resp.text

    async def _entry(self, scheme: str, host: str, port: str = "") -> _RobotsEntry:
        origin = f"{scheme}://{host}{port}"
        now = self._time_fn()
        cached = self._cache.get(origin)
        if cached and (now - cached.fetched_at) < self._ttl:
            return cached

        async with self._lock:
            # Double-checked under the lock so concurrent fetches of the same
            # robots.txt do not stampede the host.
            cached = self._cache.get(origin)
            if cached and (self._time_fn() - cached.fetched_at) < self._ttl:
                return cached
            robots_url = f"{origin}/robots.txt"
            fetch = self._fetcher or self._default_fetcher
            try:
                status, text = await fetch(robots_url)
            except Exception as exc:  # noqa: BLE001 - network failure handling
                logger.warning("robots_fetch_error", host=host, error_type=type(exc).__name__)
                entry = _RobotsEntry(parser=RobotFileParser(), fetched_at=self._time_fn(), status="unreachable")
            else:
                parser = RobotFileParser()
                if status == 200:
                    parser.parse(text.splitlines())
                    entry = _RobotsEntry(parser=parser, fetched_at=self._time_fn(), status="ok")
                elif status == 404:
                    # No robots.txt -> nothing is disallowed.
                    parser.allow_all = True  # type: ignore[attr-defined]
                    entry = _RobotsEntry(parser=parser, fetched_at=self._time_fn(), status="missing")
                elif 400 <= status < 500:
                    # Client-side error fetching robots (e.g. 403 on
                    # robots.txt itself): treat like missing rather than
                    # blocking the whole host forever.
                    parser.allow_all = True  # type: ignore[attr-defined]
                    entry = _RobotsEntry(parser=parser, fetched_at=self._time_fn(), status=f"http_{status}")
                else:
                    # Server error (5xx): fail closed until the next attempt.
                    entry = _RobotsEntry(parser=parser, fetched_at=self._time_fn(), status="unreachable")
            self._cache[origin] = entry
            logger.info("robots_fetched", host=host, status=entry.status)
            return entry

    # ------------------------------------------------------------------
    async def is_allowed(self, url: str, user_agent: Optional[str] = None) -> ComplianceDecision:
        """Evaluate ``url`` against the host's robots.txt.

        Returns a :class:`ComplianceDecision`; raises nothing. ``allowed=False``
        with ``reason="robots_unreachable"`` means fail-closed.
        """
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if not host:
            return ComplianceDecision(False, "invalid_url", detail="URL has no hostname")
        port = f":{parsed.port}" if parsed.port else ""
        entry = await self._entry(parsed.scheme or "https", host, port)

        if entry.status == "unreachable":
            return ComplianceDecision(
                False,
                "robots_unreachable",
                host=host,
                robots_allowed=False,
                detail="robots.txt exists but could not be fetched; failing closed",
            )
        if entry.status == "missing" or getattr(entry.parser, "allow_all", False):
            return ComplianceDecision(True, "robots_missing", host=host, robots_allowed=True)

        ua = user_agent or self._user_agent
        allowed = entry.parser.can_fetch(ua, url)
        if allowed:
            return ComplianceDecision(True, "robots_allowed", host=host, robots_allowed=True)
        return ComplianceDecision(
            False,
            "robots_disallowed",
            host=host,
            robots_allowed=False,
            detail=f"robots.txt disallows {url} for {ua}",
        )


# ============================================================================
# Per-host rate limiting
# ============================================================================

class HostRateLimiter:
    """Per-host minimum interval + concurrency cap.

    ``min_interval_s`` guarantees at least that much wall time between two
    requests to the same host; ``max_concurrent_per_host`` caps parallel
    in-flight requests per host. Async-safe.
    """

    def __init__(
        self,
        min_interval_s: float = 2.0,
        max_concurrent_per_host: int = 2,
        time_fn=time.time,
        sleep_fn=None,
    ) -> None:
        self._min_interval = max(0.0, float(min_interval_s))
        self._max_concurrent = max(1, int(max_concurrent_per_host))
        self._time_fn = time_fn
        self._sleep = sleep_fn or asyncio.sleep
        self._next_allowed: Dict[str, float] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()

    async def _semaphore(self, host: str) -> asyncio.Semaphore:
        async with self._lock:
            sem = self._semaphores.get(host)
            if sem is None:
                sem = asyncio.Semaphore(self._max_concurrent)
                self._semaphores[host] = sem
            return sem

    async def acquire(self, host: str) -> float:
        """Wait until a request to ``host`` may proceed. Returns the wait time."""
        host = (host or "").lower()
        sem = await self._semaphore(host)
        await sem.acquire()
        waited = 0.0
        try:
            while True:
                now = self._time_fn()
                next_ok = self._next_allowed.get(host, 0.0)
                if now >= next_ok:
                    self._next_allowed[host] = now + self._min_interval
                    return waited
                delay = next_ok - now
                waited += delay
                await self._sleep(delay)
        except BaseException:
            sem.release()
            raise

    def release(self, host: str) -> None:
        sem = self._semaphores.get((host or "").lower())
        if sem is not None:
            sem.release()

    async def __aenter__(self):  # pragma: no cover - convenience
        return self

    def stats(self) -> Dict[str, object]:
        return {
            "hosts": len(self._next_allowed),
            "min_interval_s": self._min_interval,
            "max_concurrent_per_host": self._max_concurrent,
        }


# ============================================================================
# Site allowlist
# ============================================================================

class Allowlist:
    """The user-configured site allowlist (product-level scope).

    An EMPTY allowlist means permissive mode (matching the product rule shown
    in the UI: "no site configured -- all enabled sources allowed"), which the
    caller should surface to the user. Non-empty means: only these hosts.
    """

    def __init__(self, allowed_hosts: Optional[Iterable[str]] = None) -> None:
        self._hosts: Set[str] = {self.normalize(h) for h in (allowed_hosts or []) if self.normalize(h)}

    @staticmethod
    def normalize(host: str) -> str:
        """Normalize a host or URL to a bare lowercase hostname.

        ``https://Blog.Example.com/path`` -> ``blog.example.com``;
        ``www.`` prefixes are stripped so matches behave like the UI shows.
        """
        value = (host or "").strip().lower()
        if not value:
            return ""
        if "://" in value:
            value = urlparse(value).hostname or ""
        value = value.split("/")[0].split(":")[0]
        if value.startswith("www."):
            value = value[4:]
        return value

    def is_host_allowed(self, host: str) -> bool:
        normalized = self.normalize(host)
        if not normalized:
            return False
        if not self._hosts:
            return True  # permissive mode
        return normalized in self._hosts

    def describe_mode(self) -> str:
        if not self._hosts:
            return "no site configured â€” all enabled sources allowed"
        return f"{len(self._hosts)} host(s) allowlisted"

    @property
    def hosts(self) -> List[str]:
        return sorted(self._hosts)

    def __len__(self) -> int:
        return len(self._hosts)


# ============================================================================
# One-shot gate used by every fetch tier
# ============================================================================

async def check_compliance(
    url: str,
    robots: RobotsPolicy,
    limiter: HostRateLimiter,
    allowlist: Optional[Allowlist] = None,
    user_agent: Optional[str] = None,
) -> ComplianceDecision:
    """Run allowlist -> robots -> rate-limit checks for ``url``.

    Raises :class:`HostNotAllowed` / :class:`RobotsDisallowed` when refused.
    On success the rate-limiter slot has already been acquired; the caller must
    call ``limiter.release(host)`` when the request finishes.
    """
    from urllib.parse import urlparse as _urlparse

    host = (_urlparse(url).hostname or "").lower()
    if allowlist is not None and not allowlist.is_host_allowed(host):
        decision = ComplianceDecision(False, "host_not_allowed", host=host)
        logger.warning("compliance_refused", url_host=host, reason=decision.reason)
        raise HostNotAllowed(f"Host {host!r} is not on the site allowlist")

    decision = await robots.is_allowed(url, user_agent=user_agent)
    if not decision.allowed:
        logger.warning("compliance_refused", url_host=host, reason=decision.reason)
        raise RobotsDisallowed(decision.detail or f"robots.txt disallows {url}")

    waited = await limiter.acquire(host)
    decision.rate_limit_wait_s = waited
    logger.info("compliance_allowed", url_host=host, rate_limit_wait_s=round(waited, 3))
    return decision
