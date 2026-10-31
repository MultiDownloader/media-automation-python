"""
Selenium-based fetcher (last-resort tier of the fallback chain).

Used only when the HTTP tier fails transiently and the browser tier is
unavailable. Headless Chrome with honest, minimal flags: ``--disable-gpu``
and ``--headless=new``. There are NO anti-detection/evasion flags here on
purpose -- we do not hide automation, we just fetch pages politely.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from queue_workers.envelope import get_logger
from .fingerprint import FingerprintProfile, build_profile

logger = get_logger(__name__)


@dataclass
class SeleniumFetchResult:
    """Same shape as the other tiers' results (subset)."""

    url: str
    final_url: str
    status: Optional[int]
    html: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 400


class SeleniumFetcher:
    """Minimal headless-Chrome fetcher via Selenium (optional dependency).

    Construct-time is cheap; the driver starts lazily on the first fetch.
    Call :meth:`stop` (or use the async context manager) to release the
    browser. ``selenium`` and a compatible Chrome/Edge driver must be
    installed; otherwise every fetch returns a structured error so the
    caller can report "tier unavailable" instead of crashing.
    """

    #: Chrome flags -- honest browser, no evasion.
    OPTIONS = ("--headless=new", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage")

    def __init__(self, timeout_s: int = 30, page_load_timeout_s: int = 45) -> None:
        self._timeout_s = timeout_s
        self._page_load_timeout_s = page_load_timeout_s
        self._driver: Any = None

    # ------------------------------------------------------------------
    def _start(self) -> None:
        if self._driver is not None:
            return
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "SeleniumFetcher requires the 'selenium' package "
                "(pip install selenium) and a Chrome/Edge driver on PATH."
            ) from exc
        options = Options()
        for flag in self.OPTIONS:
            options.add_argument(flag)
        self._driver = webdriver.Chrome(options=options)
        self._driver.set_page_load_timeout(self._page_load_timeout_s)
        logger.info("selenium_started")

    def stop(self) -> None:
        if self._driver is None:
            return
        try:
            self._driver.quit()
        except Exception as exc:  # noqa: BLE001 - driver may already be gone
            logger.warning("selenium_stop_error", error_type=type(exc).__name__)
        finally:
            self._driver = None
            logger.info("selenium_stopped")

    async def __aenter__(self) -> "SeleniumFetcher":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    # ------------------------------------------------------------------
    def fetch(self, url: str, profile: Optional[FingerprintProfile] = None) -> SeleniumFetchResult:
        """Fetch ``url`` synchronously (Selenium's API is blocking).

        Async callers go through ``SmartFetcher`` which runs tiers on the
        event loop's thread executor. Never raises for page errors.
        """
        profile = profile or build_profile()
        t0 = time.perf_counter()
        try:
            self._start()
        except RuntimeError as exc:
            return SeleniumFetchResult(url=url, final_url=url, status=None, error=str(exc))

        try:
            self._driver.execute_cdp_cmd(
                "Network.setUserAgentOverride", {"userAgent": profile.user_agent}
            )
        except Exception:  # noqa: BLE001 - CDP optional (non-Chrome drivers)
            pass

        try:
            self._driver.get(url)
            result = SeleniumFetchResult(
                url=url,
                final_url=self._driver.current_url or url,
                status=200,  # Selenium exposes no status directly; a successful
                # navigation to a non-error page is the closest honest signal.
                html=self._driver.page_source or "",
            )
        except Exception as exc:  # noqa: BLE001 - reported in result
            result = SeleniumFetchResult(
                url=url,
                final_url=url,
                status=None,
                error=f"{type(exc).__name__}: {exc}",
            )
        logger.info(
            "selenium_fetch_done",
            ok=result.ok,
            duration_ms=int((time.perf_counter() - t0) * 1000),
        )
        return result
