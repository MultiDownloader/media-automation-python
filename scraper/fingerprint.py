"""
Honest fingerprint profiles for web fetching.

SCOPE (important): this module exists so legitimate automated traffic looks
*consistent* -- a realistic, versioned browser user-agent paired with matching
Accept / Accept-Language / sec-ch-ua headers and viewport. Inconsistent
fingerprints (curl's UA with a browser's Accept header) cause avoidable
blocking of traffic that sites would otherwise happily serve.

This is NOT bot-detection evasion. There is no canvas spoofing, no WebGL
tampering, no WebDriver flag hiding, no challenge solving. If a site still
refuses automated access, the fetch fails cleanly and is reported -- it is
never "fought".
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

#: Curated, versioned desktop user-agents (Chrome / Firefox / Safari / Edge).
DESKTOP_USER_AGENTS: List[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) "
        "Gecko/20100101 Firefox/133.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) "
        "Gecko/20100101 Firefox/132.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/18.1 Safari/605.1.15"
    ),
]

#: Curated, versioned mobile user-agents (Android Chrome / iOS Safari).
MOBILE_USER_AGENTS: List[str] = [
    (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    ),
    (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1"
    ),
]

#: viewport presets keyed by (device, form-factor).
_VIEWPORTS = {
    ("desktop", "laptop"): (1366, 768),
    ("desktop", "large"): (1920, 1080),
    ("mobile", "phone"): (393, 852),
}

#: Locale/timezone presets kept consistent with each other.
_LOCALES = [
    ("en-US", "America/New_York", "en-US,en"),
    ("en-GB", "Europe/London", "en-GB,en"),
    ("de-DE", "Europe/Berlin", "de-DE,de,en"),
    ("fr-FR", "Europe/Paris", "fr-FR,fr,en"),
    ("en-IN", "Asia/Kolkata", "en-IN,en,hi"),
]


# ============================================================================
# Profile
# ============================================================================

@dataclass
class FingerprintProfile:
    """A consistent, honest browser fingerprint for one fetch."""

    user_agent: str
    viewport: Dict[str, int] = field(default_factory=lambda: {"width": 1366, "height": 768})
    locale: str = "en-US"
    timezone_id: str = "America/New_York"
    languages: str = "en-US,en"
    platform: str = "Win32"
    device: str = "desktop"
    extra_headers: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "user_agent": self.user_agent,
            "viewport": dict(self.viewport),
            "locale": self.locale,
            "timezone_id": self.timezone_id,
            "languages": self.languages,
            "platform": self.platform,
            "device": self.device,
            "extra_headers": dict(self.extra_headers),
        }


def _platform_for(ua: str) -> str:
    if "Windows" in ua:
        return "Win32"
    if "Macintosh" in ua:
        return "MacIntel"
    if "Linux" in ua or "Android" in ua:
        return "Linux x86_64"
    if "iPhone" in ua:
        return "iPhone"
    return "Win32"


def _browser_for(ua: str) -> str:
    if "Edg/" in ua:
        return "edg"
    if "Firefox/" in ua:
        return "firefox"
    if "Chrome/" in ua:
        return "chrome"
    if "Safari/" in ua:
        return "safari"
    return "chrome"


# ============================================================================
# User-agent pool
# ============================================================================

class UserAgentPool:
    """Rotation over the curated UA sets with optional deterministic seeding.

    ``rotate`` returns the next UA (round-robin); ``pick`` returns a random
    one (uniform, or deterministic when ``seed`` is given). Tracking cookies
    or per-UA state are deliberately NOT persisted: rotation must not become a
    fingerprinting vector of its own.
    """

    def __init__(
        self,
        desktop: Sequence[str] = tuple(DESKTOP_USER_AGENTS),
        mobile: Sequence[str] = tuple(MOBILE_USER_AGENTS),
        include_mobile: bool = True,
    ) -> None:
        self._agents: List[str] = list(desktop) + (list(mobile) if include_mobile else [])
        if not self._agents:
            raise ValueError("UserAgentPool requires at least one user-agent string")
        self._index = 0

    def __len__(self) -> int:
        return len(self._agents)

    def rotate(self) -> str:
        ua = self._agents[self._index % len(self._agents)]
        self._index += 1
        return ua

    def pick(self, seed: Optional[int] = None, mobile: Optional[bool] = None) -> str:
        candidates = self._agents
        if mobile is True:
            candidates = [a for a in self._agents if "Mobile" in a or "iPhone" in a]
        elif mobile is False:
            candidates = [a for a in self._agents if not ("Mobile" in a or "iPhone" in a)]
        if not candidates:
            candidates = self._agents
        if seed is None:
            return random.choice(candidates)
        return candidates[int(seed) % len(candidates)]

    def all(self) -> List[str]:
        return list(self._agents)


# ============================================================================
# Profile builder
# ============================================================================

def build_profile(seed: Optional[int] = None, mobile: Optional[bool] = None) -> FingerprintProfile:
    """Build a consistent profile: UA, viewport, locale and headers agree.

    ``seed`` makes the choice deterministic (useful for tests and for keeping
    one source on one stable profile). ``mobile=True/False`` restricts the
    device class; ``None`` picks either.
    """
    pool = UserAgentPool()
    ua = pool.pick(seed=seed, mobile=mobile)
    is_mobile = bool(mobile) or ("Mobile" in ua or "iPhone" in ua)

    if is_mobile:
        viewport = {"width": _VIEWPORTS[("mobile", "phone")][0], "height": _VIEWPORTS[("mobile", "phone")][1]}
        device = "mobile"
    else:
        key = ("desktop", "laptop") if (seed or 0) % 2 == 0 else ("desktop", "large")
        viewport = {"width": _VIEWPORTS[key][0], "height": _VIEWPORTS[key][1]}
        device = "desktop"

    locale, tz, languages = _LOCALES[(seed or 0) % len(_LOCALES)]

    return FingerprintProfile(
        user_agent=ua,
        viewport=viewport,
        locale=locale,
        timezone_id=tz,
        languages=languages,
        platform=_platform_for(ua),
        device=device,
    )


# ============================================================================
# Header builder
# ============================================================================

def _sec_ch_ua(profile: FingerprintProfile) -> str:
    browser = _browser_for(profile.user_agent)
    if browser == "chrome":
        return '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'
    if browser == "edg":
        return '"Microsoft Edge";v="130", "Chromium";v="130", "Not_A Brand";v="24"'
    # Safari/Firefox do not send sec-ch-ua at all.
    return ""


def default_headers(profile: FingerprintProfile) -> Dict[str, str]:
    """Request headers consistent with ``profile``.

    sec-ch-ua-family headers are only included for browsers that actually
    send them (Chromium); adding them to Firefox/Safari traffic would make the
    fingerprint *inconsistent*, which is exactly what we are avoiding.
    """
    browser = _browser_for(profile.user_agent)
    is_mobile = profile.device == "mobile"

    headers: Dict[str, str] = {
        "User-Agent": profile.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": profile.languages,
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    if browser == "chrome" or browser == "edg":
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "none"
        headers["Sec-Fetch-User"] = "?1"
        headers["sec-ch-ua"] = _sec_ch_ua(profile)
        headers["sec-ch-ua-mobile"] = "?1" if is_mobile else "?0"
        headers["sec-ch-ua-platform"] = f'"{profile.platform}"'
    elif browser == "firefox":
        headers["DNT"] = "1"
    # Safari: keep headers minimal, matching what real Safari sends.

    headers.update(profile.extra_headers)
    return headers


def viewport_dict(profile: FingerprintProfile) -> Dict[str, int]:
    """Convenience for Playwright's ``context viewport=...`` argument."""
    return dict(profile.viewport)
