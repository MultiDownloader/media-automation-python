"""
Playwright-based fetcher with request interception, ad/popup blocking and
direct media-link sniffing.

What it does:
- reuses one browser + a small pool of contexts (fast, low memory);
- intercepts every request: blocks ad/tracker/popup hosts from
  :class:`AdBlockRules` and optionally heavy resource types (images, media,
  fonts, stylesheets) for text-only fetches;
- closes popups/new tabs so a click-bait ad tab never steals the session;
- auto-dismisses JS dialogs (alert/confirm/prompt);
- sniffs direct media links from network responses (``.mp4``, ``.m3u8``,
  ``.mpd``, ``video/*`` ...), which downstream code can use as download
  candidates WITHOUT any player automation;
- records bounded request/response traces for debugging.

What it deliberately does NOT do: solve captchas, bypass bot detection or
tunnel around access-control systems. A blocked page is reported as
``status`` + empty content, never "fought".
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from queue_workers.envelope import get_logger
from .fingerprint import FingerprintProfile, build_profile, default_headers

logger = get_logger(__name__)

#: Default rules file (relative to this package).
DEFAULT_RULES_FILE = Path(__file__).parent / "rules" / "adblock_patterns.json"

#: Bounded capture buffer per fetch (requests/responses kept for debugging).
MAX_CAPTURE_ENTRIES = 200

#: File extensions that count as direct media links.
MEDIA_EXTENSIONS = (".mp4", ".m3u8", ".mpd", ".mkv", ".webm", ".mov", ".avi", ".ts")

#: Content-types that count as direct media links.
MEDIA_CONTENT_TYPES = (
    "video/",
    "audio/",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "application/dash+xml",
)


# ============================================================================
# Result types
# ============================================================================

@dataclass
class SniffedMedia:
    """A media URL observed on the network (direct download candidate)."""

    url: str
    content_type: str = ""
    kind: str = "video"  # video | audio | manifest (hls/dash)
    status: Optional[int] = None
    size_hint: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "content_type": self.content_type,
            "kind": self.kind,
            "status": self.status,
            "size_hint": self.size_hint,
        }


@dataclass
class FetchResult:
    """Outcome of one browser fetch."""

    url: str
    final_url: str
    status: Optional[int]
    html: str
    sniffed_media: List[SniffedMedia] = field(default_factory=list)
    blocked_requests: int = 0
    total_requests: int = 0
    headers: Dict[str, str] = field(default_factory=dict)
    timings: Dict[str, float] = field(default_factory=dict)
    error: Optional[str] = None
    captured: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 400

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status": self.status,
            "html_bytes": len(self.html.encode("utf-8", errors="replace")),
            "sniffed_media": [m.to_dict() for m in self.sniffed_media],
            "blocked_requests": self.blocked_requests,
            "total_requests": self.total_requests,
            "timings": dict(self.timings),
            "error": self.error,
        }


# ============================================================================
# Media sniffing helpers
# ============================================================================

def classify_media_url(url: str, content_type: str = "") -> Optional[str]:
    """Return ``"manifest"`` / ``"video"`` / ``"audio"`` when this looks like
    direct media, else ``None``.

    Manifests (HLS ``.m3u8``, DASH ``.mpd``) are classified separately because
    the downloader treats them differently from plain files.
    """
    ct = (content_type or "").lower().split(";")[0].strip()
    path = urlparse(url).path.lower()

    if ct in ("application/vnd.apple.mpegurl", "application/x-mpegurl") or path.endswith(".m3u8"):
        return "manifest"
    if ct == "application/dash+xml" or path.endswith(".mpd"):
        return "manifest"
    if ct.startswith("video/") or any(path.endswith(ext) for ext in MEDIA_EXTENSIONS if ext != ".ts"):
        return "video"
    if ct.startswith("audio/") or path.endswith(".mp3") or path.endswith(".ogg") or path.endswith(".m4a"):
        return "audio"
    # HLS segments (.ts) are pieces of a manifest, not standalone downloads.
    return None


# ============================================================================
# Ad-block rules
# ============================================================================

class AdBlockRules:
    """URL blocklist for ads/trackers/popups, loaded from JSON + defaults.

    Patterns are simple substrings matched against the full URL (lowercased):
    ``"doubleclick.net"`` blocks any host containing that string. ``None`` or
    empty pattern lists simply block nothing.
    """

    #: Small inline fallback used when the JSON file is missing/unreadable.
    DEFAULT_PATTERNS: Tuple[str, ...] = (
        "doubleclick.net",
        "googlesyndication.com",
        "googleadservices.com",
        "adservice.google",
        "pagead2.googlesyndication",
        "popads.net",
        "propellerads.com",
        "taboola.com",
        "outbrain.com",
        "adsystem.com",
        "amazon-adsystem.com",
        "scorecardresearch.com",
        "quantserve.com",
    )

    #: Resource types that are blocked for text-only fetches.
    HEAVY_RESOURCE_TYPES = frozenset({"image", "media", "font"})

    def __init__(
        self,
        patterns: Optional[List[str]] = None,
        block_heavy_resources: bool = False,
        rules_file: Optional[os.PathLike] = None,
    ) -> None:
        self.block_heavy_resources = block_heavy_resources
        loaded: List[str] = []
        if patterns is not None:
            # Explicitly provided patterns win over everything else.
            loaded = [str(p).lower() for p in patterns if isinstance(p, str) and p.strip()]
        if not loaded:
            path = Path(rules_file) if rules_file else DEFAULT_RULES_FILE
            try:
                if path.exists():
                    data = json.loads(path.read_text(encoding="utf-8"))
                    entries = data.get("patterns") if isinstance(data, dict) else data
                    if isinstance(entries, list):
                        loaded = [str(p).lower() for p in entries if isinstance(p, str) and p.strip()]
            except Exception as exc:  # noqa: BLE001 - rules are an optimization
                logger.warning("adblock_rules_unreadable", path=str(path), error_type=type(exc).__name__)
        if not loaded:
            loaded = list(self.DEFAULT_PATTERNS)
        self._patterns: Tuple[str, ...] = tuple(loaded)

    @property
    def patterns(self) -> Tuple[str, ...]:
        return self._patterns

    def should_block(self, url: str, resource_type: str = "") -> bool:
        """Whether this request should be aborted before it is sent."""
        if resource_type in self.HEAVY_RESOURCE_TYPES and self.block_heavy_resources:
            return True
        low = url.lower()
        return any(pattern in low for pattern in self._patterns)


# ============================================================================
# Playwright fetcher
# ============================================================================

class PlaywrightFetcher:
    """Async Playwright wrapper with interception and media sniffing.

    Usage::

        async with PlaywrightFetcher() as fetcher:
            result = await fetcher.fetch("https://example.com/page")

    ``playwright`` is imported lazily; construct-time errors carry a clear
    install hint. Contexts are created per fetch from one shared browser to
    isolate cookies while still paying the browser-startup cost only once.
    """

    def __init__(
        self,
        rules: Optional[AdBlockRules] = None,
        headless: bool = True,
        max_contexts: int = 3,
        screenshot_dir: Optional[os.PathLike] = None,
        default_timeout_ms: int = 30000,
    ) -> None:
        self._rules = rules if rules is not None else AdBlockRules()
        self._headless = headless
        self._max_contexts = max(1, int(max_contexts))
        self._screenshot_dir = Path(screenshot_dir) if screenshot_dir else None
        self._default_timeout_ms = int(default_timeout_ms)
        self._playwright = None
        self._browser = None
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "PlaywrightFetcher requires the 'playwright' package "
                "(pip install playwright && playwright install chromium). "
                "Use the HTTP tier (SmartFetcher) meanwhile."
            ) from exc
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self._headless)
        self._started = True
        logger.info("browser_started", headless=self._headless)

    async def stop(self) -> None:
        if not self._started:
            return
        try:
            if self._browser is not None:
                await self._browser.close()
            if self._playwright is not None:
                await self._playwright.stop()
        finally:
            self._browser = None
            self._playwright = None
            self._started = False
            logger.info("browser_stopped")

    async def __aenter__(self) -> "PlaywrightFetcher":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------
    async def fetch(
        self,
        url: str,
        wait_for: str = "domcontentloaded",
        timeout_ms: Optional[int] = None,
        profile: Optional[FingerprintProfile] = None,
        block_resources: bool = False,
    ) -> FetchResult:
        """Fetch ``url`` in a real browser context.

        ``wait_for``: ``domcontentloaded`` (fast) or ``networkidle`` (lets
        XHR-rendered pages settle). ``block_resources`` additionally drops
        images/media/fonts for text-only extraction. Never raises for page
        errors -- inspect ``FetchResult.error`` / ``status``.
        """
        if not self._started:
            await self.start()

        profile = profile or build_profile()
        timeout = int(timeout_ms or self._default_timeout_ms)
        timings: Dict[str, float] = {}
        t_start = time.perf_counter()

        headers = default_headers(profile)
        context = await self._browser.new_context(
            user_agent=headers.pop("User-Agent"),
            viewport=dict(profile.viewport),
            locale=profile.locale,
            timezone_id=profile.timezone_id,
            extra_http_headers=headers,
        )
        page = await context.new_page()
        page.set_default_timeout(timeout)

        blocked = 0
        total = 0
        sniffed: List[SniffedMedia] = []
        captured: List[Dict[str, Any]] = []

        # --- popup / dialog hygiene ------------------------------------
        def _on_page(popup) -> None:
            # Any extra page (popup/ad tab) is closed immediately; the original
            # page keeps the session.
            if popup is not page:
                logger.info("popup_blocked", url=popup.url[:200])
                asyncio_create_task_safe(popup.close())

        context.on("page", _on_page)

        def _on_dialog(dialog) -> None:
            asyncio_create_task_safe(dialog.dismiss())

        page.on("dialog", _on_dialog)

        # --- network interception --------------------------------------
        def _route(route, request) -> None:
            nonlocal blocked, total
            total += 1
            rtype = request.resource_type
            url = request.url
            block = self._rules.should_block(url, rtype)
            if not block and block_resources and rtype in AdBlockRules.HEAVY_RESOURCE_TYPES:
                # Text-only fetch: skip images/media/fonts entirely.
                block = True
            if block:
                blocked += 1
                asyncio_create_task_safe(route.abort())
                return
            if len(captured) < MAX_CAPTURE_ENTRIES:
                captured.append({"type": rtype, "url": url[:300], "method": request.method})
            asyncio_create_task_safe(route.continue_())

        await page.route("**/*", _route)

        def _on_response(response) -> None:
            ct = ""
            try:
                ct = (response.headers or {}).get("content-type", "")
            except Exception:  # noqa: BLE001 - headers may be unavailable
                ct = ""
            kind = classify_media_url(response.url, ct)
            if kind:
                if len(sniffed) < MAX_CAPTURE_ENTRIES and not any(m.url == response.url for m in sniffed):
                    size = None
                    try:
                        raw = (response.headers or {}).get("content-length")
                        size = int(raw) if raw else None
                    except Exception:  # noqa: BLE001 - optional header
                        size = None
                    sniffed.append(
                        SniffedMedia(
                            url=response.url,
                            content_type=ct,
                            kind=kind,
                            status=response.status,
                            size_hint=size,
                        )
                    )
                    logger.info("media_sniffed", url=response.url[:200], kind=kind)

        page.on("response", _on_response)

        # --- navigate ---------------------------------------------------
        result = FetchResult(url=url, final_url=url, status=None, html="")
        t_nav = time.perf_counter()
        try:
            response = await page.goto(url, wait_until=wait_for, timeout=timeout)
            timings["navigation_ms"] = round((time.perf_counter() - t_nav) * 1000, 1)
            result.status = response.status if response is not None else None
            result.final_url = page.url
            if response is not None:
                try:
                    result.headers = dict(await response.all_headers())
                except Exception:  # noqa: BLE001 - response may be gone
                    result.headers = {}
            result.html = await page.content()
        except Exception as exc:  # noqa: BLE001 - reported in the result
            result.error = f"{type(exc).__name__}: {exc}"
            logger.warning("browser_fetch_error", url=url[:200], error_type=type(exc).__name__)
            if self._screenshot_dir is not None:
                await self._safe_screenshot(page, url)
        finally:
            timings["total_ms"] = round((time.perf_counter() - t_start) * 1000, 1)
            result.timings = timings
            result.sniffed_media = sniffed
            result.blocked_requests = blocked
            result.total_requests = total
            result.captured = captured[:MAX_CAPTURE_ENTRIES]
            await context.close()

        return result

    # ------------------------------------------------------------------
    async def _safe_screenshot(self, page, url: str) -> None:
        try:
            self._screenshot_dir.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-z0-9]+", "-", url.lower())[:80].strip("-")
            path = self._screenshot_dir / f"{slug}-{int(time.time())}.png"
            await page.screenshot(path=str(path), full_page=False)
            logger.info("error_screenshot_saved", path=str(path))
        except Exception as exc:  # noqa: BLE001 - screenshots are best-effort
            logger.warning("error_screenshot_failed", error_type=type(exc).__name__)


# ---------------------------------------------------------------------------
# Small helper: schedule a coroutine on the running loop without awaiting it
# here (route/dialog handlers are sync callbacks).
# ---------------------------------------------------------------------------

def asyncio_create_task_safe(coro) -> None:
    import asyncio

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - defensive
        coro.close()
        return
    loop.create_task(coro)
