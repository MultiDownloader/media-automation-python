"""
Compliant web-fetch/scraping stack for the media automation platform.

Layers:
- ``compliance``  -- robots.txt + per-host rate limits + site allowlist gate
- ``fingerprint`` -- honest, consistent browser fingerprints (UA + headers)
- ``browser``     -- Playwright fetcher: request interception, ad/popup
                     blocking, direct media-link sniffing
- ``parsers``     -- BeautifulSoup helpers with a stdlib fallback parser
- ``fallback``    -- SmartFetcher: HTTP -> BROWSER -> SELENIUM fallback chain
- ``cache``       -- Redis/memory response cache

COMPLIANCE: this package never solves captchas, never bypasses bot detection,
Cloudflare challenges, DRM or paywalls. Blocked means blocked: it is reported
as a structured permanent failure so the source can be disabled, not "fought".
See README.md in this directory for details.
"""

from .cache import MemoryResponseCache, get_response_cache
from .compliance import (
    Allowlist,
    ComplianceDecision,
    ComplianceError,
    HostNotAllowed,
    HostRateLimiter,
    RateLimited,
    RobotsDisallowed,
    RobotsPolicy,
    check_compliance,
)
from .fallback import FetchOutcome, FetchResult, FetchTier, SmartFetcher
from .fingerprint import FingerprintProfile, UserAgentPool, build_profile, default_headers
from .parsers import MediaLink, extract_links, extract_meta, find_media_links, select_all_texts, select_text

__all__ = [
    "Allowlist",
    "ComplianceDecision",
    "ComplianceError",
    "FetchOutcome",
    "FetchResult",
    "FetchTier",
    "FingerprintProfile",
    "HostNotAllowed",
    "HostRateLimiter",
    "MemoryResponseCache",
    "MediaLink",
    "RateLimited",
    "RobotsDisallowed",
    "RobotsPolicy",
    "SmartFetcher",
    "UserAgentPool",
    "build_profile",
    "check_compliance",
    "default_headers",
    "extract_links",
    "extract_meta",
    "find_media_links",
    "get_response_cache",
    "select_all_texts",
    "select_text",
]
