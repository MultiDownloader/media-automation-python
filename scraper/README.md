# Scraper Stack (`python/scraper/`)

Compliant web-fetch layer for the media automation platform: robots.txt +
rate limits + allowlist gating, honest browser fingerprints, Playwright
fetching with request interception / ad-popup blocking / direct media-link
sniffing, HTML parsing with a zero-dependency fallback, an HTTP→browser→selenium
fallback chain and a response cache.

## Compliance stance (non-negotiable)

- **robots.txt is honoured.** Disallowed paths are refused. A robots.txt that
  exists but cannot be fetched (5xx/timeout) fails **closed**.
- **Per-host rate limits**: minimum interval between requests + concurrency
  cap (`HostRateLimiter`).
- **Site allowlist first**: nothing outside the user's configured hosts is
  ever fetched. Empty allowlist = permissive mode (matches the UI warning).
- **No CAPTCHA solving, no anti-bot / Cloudflare-challenge bypass, no DRM or
  paywall circumvention.** A 403/429 or challenge page is reported as a
  structured `compliance_blocked` outcome and the source should be disabled —
  never "fought".
- Fingerprints are *consistent, realistic browser defaults* (UA + matching
  headers + viewport), not evasion. See `fingerprint.py`'s module docstring.

## Layers

| Module | Purpose |
|---|---|
| `compliance.py` | `RobotsPolicy`, `HostRateLimiter`, `Allowlist`, `check_compliance()` |
| `fingerprint.py` | `UserAgentPool`, `FingerprintProfile`, `build_profile()`, `default_headers()` |
| `browser.py` | `PlaywrightFetcher` (interception, popup/dialog handling, `SniffedMedia`), `AdBlockRules` |
| `rules/adblock_patterns.json` | Curated ad/tracker/popup domain list (38 entries) |
| `parsers.py` | `select_text`, `select_all_texts`, `extract_meta`, `extract_links`, `find_media_links` — bs4 when installed, stdlib `html.parser` fallback otherwise |
| `fallback.py` | `SmartFetcher`: HTTP → BROWSER → SELENIUM chain with per-tier retry, error classification and a uniform `FetchOutcome` |
| `selenium_fetch.py` | `SeleniumFetcher` — headless Chrome last resort (no evasion flags) |
| `cache.py` | `ResponseCache` — Redis or in-memory LRU with TTL |

## Usage

```python
import asyncio
from scraper import SmartFetcher, Allowlist

async def main():
    fetcher = SmartFetcher(allowlist=Allowlist(["myblog.example.com"]))
    outcome = await fetcher.fetch("https://myblog.example.com/posts/1")
    if outcome.ok:
        print(outcome.tier_used, len(outcome.result.html))
        for media in outcome.result.sniffed_media:   # direct links found on the network
            print(media["kind"], media["url"])
    else:
        print("failed:", outcome.errors)             # e.g. robots / 403 not bypassed

asyncio.run(main())
```

Browser fetcher directly (ad/popup blocking + sniffing):

```python
from scraper.browser import PlaywrightFetcher, AdBlockRules

async with PlaywrightFetcher(rules=AdBlockRules(block_heavy_resources=True)) as f:
    result = await fetcher.fetch("https://example.com/page", wait_for="networkidle")
    result.sniffed_media       # [SniffedMedia(url=..., kind="manifest"), ...]
    result.blocked_requests    # ads/trackers aborted before they ran
```

## Optional dependencies

Everything degrades gracefully; CI runs the full test suite with **none** of
these installed (browser logic is covered with fakes):

```bash
pip install beautifulsoup4 playwright selenium
playwright install chromium
```

| Missing package | Effect |
|---|---|
| `playwright` | browser tier reports "tier unavailable", chain continues |
| `selenium` | selenium tier reports "tier unavailable" |
| `beautifulsoup4` | parsers use the stdlib fallback (same results for supported selectors) |
| `redis` | caches fall back to in-memory stores |

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_URL` | unset | enables Redis-backed `ResponseCache` when reachable |

## Tests

```bash
cd python
python -m pytest tests/test_scraper_*.py -v
```

- `test_scraper_compliance.py` — robots (allow/disallow/missing/unreachable),
  allowlist normalization, rate-limiter pacing, the `check_compliance` gate
- `test_scraper_fingerprint.py` — profile consistency, header/UA agreement,
  deterministic seeding, mobile presets
- `test_scraper_parsers.py` — stdlib fallback parsing forced on (CI path)
- `test_scraper_fallback.py` — chain behaviour with fake tiers: fallback on
  transient, abort on permanent, abort on robots/allowlist, 403/429 reported
  and never bypassed
- `test_scraper_rules.py` — ad-block pattern matching + media URL classification
