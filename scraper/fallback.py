"""
Smart multi-tier fetcher with a fallback chain.

Tiers, cheapest first:

1. ``HTTP``      -- plain httpx GET with honest headers (fast, cheap)
2. ``BROWSER``   -- Playwright (renders JS, sniffs direct media links)
3. ``SELENIUM``  -- headless Chrome via Selenium (last resort)

Rules:
- Every tier runs AFTER the compliance gate (allowlist -> robots -> rate
  limit). Compliance refusals abort the chain immediately -- they are policy
  decisions, not errors to route around.
- Transient errors (timeouts, resets, 5xx) advance to the next tier.
- Permanent errors (404/400/410, unsupported URL) abort with what we have.
- A site that blocks automated access (403/429 with bot page, challenge
  markup) is reported as a structured permanent failure. We do NOT try to
  defeat the protection; the source is simply marked unfetchable.

Optional dependencies (playwright, selenium, bs4) are imported lazily; without
them the chain just skips those tiers and the HTTP tier still works.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from queue_workers.envelope import get_logger
from .compliance import (
    Allowlist,
    ComplianceDecision,
    ComplianceError,
    HostRateLimiter,
    RobotsDisallowed,
    RobotsPolicy,
    check_compliance,
)
from .fingerprint import FingerprintProfile, build_profile, default_headers

logger = get_logger(__name__)


# ============================================================================
# Tier enum
# ============================================================================

class FetchTier(str, Enum):
    HTTP = "http"
    BROWSER = "browser"
    SELENIUM = "selenium"


#: Default chain: HTTP -> BROWSER -> SELENIUM.
DEFAULT_CHAIN: Tuple[FetchTier, ...] = (FetchTier.HTTP, FetchTier.BROWSER, FetchTier.SELENIUM)


# ============================================================================
# Result types
# ============================================================================

@dataclass
class FetchResult:
    """Uniform result shape shared by all tiers."""

    url: str
    final_url: str
    status: Optional[int]
    html: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    sniffed_media: List[Dict[str, Any]] = field(default_factory=list)
    blocked_requests: int = 0
    tier: str = FetchTier.HTTP.value
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 400

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status": self.status,
            "html_bytes": len(self.html.encode("utf-8", errors="replace")),
            "headers": dict(self.headers),
            "sniffed_media": list(self.sniffed_media),
            "blocked_requests": self.blocked_requests,
            "tier": self.tier,
            "error": self.error,
        }


@dataclass
class AttemptRecord:
    tier: str
    ok: bool
    error_category: Optional[str] = None
    error: Optional[str] = None
    duration_ms: int = 0
    status: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tier": self.tier,
            "ok": self.ok,
            "error_category": self.error_category,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "status": self.status,
        }


@dataclass
class FetchOutcome:
    """The chain's final outcome: which tier delivered, and every attempt."""

    url: str
    tier_used: Optional[str]
    result: Optional[FetchResult]
    attempts: List[AttemptRecord] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_ms: int = 0
    compliance: Optional[ComplianceDecision] = None

    @property
    def ok(self) -> bool:
        return self.result is not None and self.result.ok

    def to_event(self) -> Dict[str, Any]:
        """Analytics-friendly dict (no PII, no signed URLs)."""
        return {
            "event": "fetch_outcome",
            "url_host": (urlparse(self.url).hostname or ""),
            "ok": self.ok,
            "tier_used": self.tier_used,
            "attempts": [a.to_dict() for a in self.attempts],
            "duration_ms": self.duration_ms,
            "compliance": self.compliance.to_dict() if self.compliance else None,
        }


# ============================================================================
# Error classification helpers (local, lightweight)
# ============================================================================

#: Statuses that mean "this will never work" -> abort the chain.
PERMANENT_STATUSES = frozenset({400, 401, 403, 404, 405, 410, 422, 451})

#: Statuses that mean "come back later" -> retryable at the same or next tier.
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def _classify_status(status: int) -> str:
    if status == 429:
        # The site is telling us to back off. The per-host limiter already
        # paces us; a 429 means the origin wants more. Stop the chain and
        # report -- this is throttling, not something to route around.
        return "compliance_blocked"
    if status in PERMANENT_STATUSES:
        # 403 usually means bot protection; report it as compliance so
        # nobody downstream tries to "fix" it.
        if status == 403:
            return "compliance_blocked"
        return "permanent"
    if status in TRANSIENT_STATUSES:
        return "transient"
    if status >= 500:
        return "transient"
    return "ok"


def _is_transient(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    if isinstance(exc, (TimeoutError, ConnectionError, ConnectionResetError)):
        return True
    return any(k in name or k in message for k in ("timeout", "reset", "refused", "aborted", "unreachable", "dns"))


# ============================================================================
# Tier fetchers (each returns FetchResult, raises nothing)
# ============================================================================

class HttpTier:
    """Plain HTTP fetch with honest headers and a per-profile UA."""

    name = FetchTier.HTTP.value

    def __init__(self, timeout_s: float = 20.0, follow_redirects: bool = True) -> None:
        self._timeout_s = timeout_s
        self._follow = follow_redirects

    async def fetch(self, url: str, profile: FingerprintProfile) -> FetchResult:
        import httpx

        headers = default_headers(profile)
        try:
            async with httpx.AsyncClient(follow_redirects=self._follow, timeout=self._timeout_s) as client:
                resp = await client.get(url, headers=headers)
            content_type = resp.headers.get("content-type", "")
            html = resp.text if "html" in content_type or not content_type else ""
            return FetchResult(
                url=url,
                final_url=str(resp.url),
                status=resp.status_code,
                html=html,
                headers=dict(resp.headers),
                tier=self.name,
            )
        except Exception as exc:  # noqa: BLE001 - reported in result
            logger.warning("http_tier_error", host=urlparse(url).hostname, error_type=type(exc).__name__)
            return FetchResult(url=url, final_url=url, status=None, error=f"{type(exc).__name__}: {exc}", tier=self.name)


class BrowserTier:
    """Playwright-backed tier (optional dependency)."""

    name = FetchTier.BROWSER.value

    def __init__(self, fetcher: Optional[Any] = None, wait_for: str = "domcontentloaded") -> None:
        self._fetcher = fetcher
        self._wait_for = wait_for

    async def fetch(self, url: str, profile: FingerprintProfile) -> FetchResult:
        from .browser import PlaywrightFetcher

        owns = self._fetcher is None
        fetcher = self._fetcher or PlaywrightFetcher()
        try:
            await fetcher.start()
            inner = await fetcher.fetch(url, wait_for=self._wait_for, profile=profile)
            return FetchResult(
                url=url,
                final_url=inner.final_url,
                status=inner.status,
                html=inner.html,
                headers=inner.headers,
                sniffed_media=[m.to_dict() for m in inner.sniffed_media],
                blocked_requests=inner.blocked_requests,
                tier=self.name,
                error=inner.error,
            )
        except RuntimeError as exc:
            # Missing optional dependency: tier unavailable, chain continues.
            return FetchResult(url=url, final_url=url, status=None, error=f"{type(exc).__name__}: {exc}", tier=self.name)
        finally:
            if owns:
                await fetcher.stop()


class SeleniumTier:
    """Selenium-backed last resort (optional dependency)."""

    name = FetchTier.SELENIUM.value

    def __init__(self, fetcher: Optional[Any] = None) -> None:
        self._fetcher = fetcher

    async def fetch(self, url: str, profile: FingerprintProfile) -> FetchResult:
        from .selenium_fetch import SeleniumFetcher

        fetcher = self._fetcher or SeleniumFetcher()
        try:
            inner = await _maybe_await(fetcher.fetch(url, profile=profile))
            return FetchResult(
                url=url,
                final_url=getattr(inner, "final_url", url),
                status=getattr(inner, "status", None),
                html=getattr(inner, "html", ""),
                headers=getattr(inner, "headers", {}),
                tier=self.name,
                error=getattr(inner, "error", None),
            )
        except RuntimeError as exc:
            return FetchResult(url=url, final_url=url, status=None, error=f"{type(exc).__name__}: {exc}", tier=self.name)


async def _maybe_await(value: Any) -> Any:
    import inspect

    if inspect.isawaitable(value):
        return await value
    return value


# ============================================================================
# Smart fetcher
# ============================================================================

class SmartFetcher:
    """Ordered fallback chain over the tiers with compliance gating.

    ``tier_fetchers`` may override the default tiers (used heavily in tests).
    ``sleep_fn`` is injectable so retry delays don't slow down the test suite.
    """

    def __init__(
        self,
        robots: Optional[RobotsPolicy] = None,
        limiter: Optional[HostRateLimiter] = None,
        allowlist: Optional[Allowlist] = None,
        chain: Optional[List[FetchTier]] = None,
        tier_fetchers: Optional[Dict[FetchTier, Any]] = None,
        max_attempts_per_tier: int = 2,
        retry_base_delay_s: float = 1.0,
        sleep_fn: Optional[Callable[[float], Any]] = None,
        profile_factory: Optional[Callable[[], FingerprintProfile]] = None,
    ) -> None:
        self._robots = robots or RobotsPolicy()
        self._limiter = limiter or HostRateLimiter()
        self._allowlist = allowlist
        self._chain = list(chain or DEFAULT_CHAIN)
        self._max_attempts = max(1, int(max_attempts_per_tier))
        self._base_delay = max(0.0, float(retry_base_delay_s))
        self._sleep = sleep_fn
        self._profile_factory = profile_factory or build_profile
        self._tiers: Dict[FetchTier, Any] = dict(tier_fetchers or {})
        self._default_tiers: Dict[FetchTier, Any] = {
            FetchTier.HTTP: HttpTier(),
            FetchTier.BROWSER: BrowserTier(),
            FetchTier.SELENIUM: SeleniumTier(),
        }

    def _tier_fetcher(self, tier: FetchTier) -> Any:
        return self._tiers.get(tier) or self._default_tiers[tier]

    async def _sleep_async(self, seconds: float) -> None:
        if seconds <= 0:
            return
        if self._sleep is not None:
            result = self._sleep(seconds)
            import inspect

            if inspect.isawaitable(result):
                await result
            return
        import asyncio

        await asyncio.sleep(seconds)

    # ------------------------------------------------------------------
    async def fetch(self, url: str) -> FetchOutcome:
        """Fetch ``url`` through the chain; never raises for fetch errors."""
        t0 = time.perf_counter()
        outcome = FetchOutcome(url=url, tier_used=None, result=None)

        # 1) Compliance gate (allowlist -> robots -> rate limit).
        host = (urlparse(url).hostname or "").lower()
        try:
            outcome.compliance = await check_compliance(
                url, self._robots, self._limiter, self._allowlist
            )
        except (ComplianceError, RobotsDisallowed) as exc:
            outcome.errors.append(f"{type(exc).__name__}: {exc}")
            outcome.duration_ms = int((time.perf_counter() - t0) * 1000)
            logger.warning("fetch_refused_by_compliance", host=host, error_type=type(exc).__name__)
            return outcome
        finally:
            # The limiter slot is held for the whole chain to avoid hammering
            # the same host from multiple tiers back-to-back.
            pass

        try:
            # 2) Walk the chain.
            for tier in self._chain:
                fetcher = self._tier_fetcher(tier)
                profile = self._profile_factory()
                for attempt in range(1, self._max_attempts + 1):
                    ta = time.perf_counter()
                    try:
                        result = await _maybe_await(fetcher.fetch(url, profile))
                    except Exception as exc:  # noqa: BLE001 - tier blew up
                        # An unexpected exception from a tier is treated as
                        # transient: the next tier (often a different engine)
                        # may still deliver. Only HTTP statuses can abort the
                        # chain as permanent.
                        outcome.attempts.append(
                            AttemptRecord(
                                tier=tier.value,
                                ok=False,
                                error_category="transient",
                                error=f"{type(exc).__name__}: {exc}",
                                duration_ms=int((time.perf_counter() - ta) * 1000),
                            )
                        )
                        outcome.errors.append(f"{tier.value}: {type(exc).__name__}: {exc}")
                        if attempt < self._max_attempts:
                            await self._sleep_async(self._base_delay * (2 ** (attempt - 1)))
                        continue

                    status = getattr(result, "status", None)
                    error = getattr(result, "error", None)
                    outcome.attempts.append(
                        AttemptRecord(
                            tier=tier.value,
                            ok=bool(getattr(result, "ok", False)),
                            error=error,
                            duration_ms=int((time.perf_counter() - ta) * 1000),
                            status=status,
                        )
                    )

                    if getattr(result, "ok", False):
                        outcome.tier_used = tier.value
                        outcome.result = result
                        return self._finish(outcome, t0)

                    if error and status is None:
                        # Transport-level failure -> transient, next attempt/tier.
                        if attempt < self._max_attempts:
                            await self._sleep_async(self._base_delay * (2 ** (attempt - 1)))
                            continue
                        break  # next tier

                    if status is not None:
                        category = _classify_status(status)
                        if category == "permanent":
                            outcome.errors.append(f"{tier.value}: HTTP {status}")
                            return self._finish(outcome, t0)
                        if category == "compliance_blocked":
                            outcome.errors.append(
                                f"{tier.value}: HTTP {status} (access blocked by the site; not bypassing)"
                            )
                            return self._finish(outcome, t0)
                        # transient
                        if attempt < self._max_attempts:
                            await self._sleep_async(self._base_delay * (2 ** (attempt - 1)))
                            continue
                        break  # next tier

                    # Unknown shape without error/status: treat as transient.
                    break

            outcome.errors.append("all tiers exhausted")
            return self._finish(outcome, t0)
        finally:
            self._limiter.release(host)

    def _finish(self, outcome: FetchOutcome, t0: float) -> FetchOutcome:
        outcome.duration_ms = int((time.perf_counter() - t0) * 1000)
        if outcome.ok:
            logger.info(
                "fetch_succeeded",
                host=urlparse(outcome.url).hostname,
                tier=outcome.tier_used,
                duration_ms=outcome.duration_ms,
            )
        else:
            logger.warning(
                "fetch_failed",
                host=urlparse(outcome.url).hostname,
                duration_ms=outcome.duration_ms,
                errors=outcome.errors[:3],
            )
        return outcome
