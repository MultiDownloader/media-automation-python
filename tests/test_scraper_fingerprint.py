"""Tests for scraper.fingerprint: profile consistency and UA rotation."""

from scraper.fingerprint import (
    DESKTOP_USER_AGENTS,
    MOBILE_USER_AGENTS,
    FingerprintProfile,
    UserAgentPool,
    build_profile,
    default_headers,
    viewport_dict,
)


def test_pool_contains_desktop_and_mobile():
    pool = UserAgentPool()
    assert len(pool) == len(DESKTOP_USER_AGENTS) + len(MOBILE_USER_AGENTS)
    assert all("Mozilla" in ua for ua in pool.all())


def test_pool_desktop_only():
    pool = UserAgentPool(include_mobile=False)
    assert len(pool) == len(DESKTOP_USER_AGENTS)
    assert all("Mobile" not in ua for ua in pool.all())


def test_pool_rotation_is_round_robin():
    pool = UserAgentPool(include_mobile=False)
    picks = [pool.rotate() for _ in range(len(pool) * 2)]
    assert picks[: len(pool)] == picks[len(pool) :]  # wraps around


def test_pool_pick_seed_is_deterministic():
    pool = UserAgentPool()
    assert pool.pick(seed=7) == pool.pick(seed=7)
    assert pool.pick(seed=7) != pool.pick(seed=8) or len(pool) <= 2


def test_pool_pick_mobile_filter():
    pool = UserAgentPool()
    for seed in range(10):
        assert "Mobile" in pool.pick(seed=seed, mobile=True) or "iPhone" in pool.pick(seed=seed, mobile=True)


def test_pool_rejects_empty():
    import pytest

    with pytest.raises(ValueError):
        UserAgentPool(desktop=[], mobile=[], include_mobile=False)


def test_build_profile_defaults():
    profile = build_profile()
    assert isinstance(profile, FingerprintProfile)
    assert profile.user_agent.startswith("Mozilla")
    assert profile.viewport["width"] > 0 and profile.viewport["height"] > 0
    assert "-" in profile.locale
    assert "/" in profile.timezone_id  # e.g. America/New_York


def test_build_profile_seed_deterministic():
    a = build_profile(seed=42)
    b = build_profile(seed=42)
    assert a.to_dict() == b.to_dict()


def test_build_profile_mobile_viewport():
    profile = build_profile(seed=1, mobile=True)
    assert profile.device == "mobile"
    assert profile.viewport["width"] < 500  # phone-sized


def test_headers_match_chrome_profile():
    profile = build_profile(seed=0, mobile=False)
    if "Chrome/" not in profile.user_agent:
        import pytest

        pytest.skip("seed picked a non-Chrome UA")
    headers = default_headers(profile)
    assert headers["User-Agent"] == profile.user_agent
    assert "sec-ch-ua" in headers
    assert headers["Accept-Language"] == profile.languages
    assert headers["sec-ch-ua-mobile"] == "?0"


def test_headers_firefox_omits_chromium_only_headers():
    firefox = next(ua for ua in DESKTOP_USER_AGENTS if "Firefox/" in ua)
    profile = FingerprintProfile(user_agent=firefox, platform="Win32", device="desktop")
    headers = default_headers(profile)
    assert "sec-ch-ua" not in headers  # Firefox never sends client hints
    assert headers["DNT"] == "1"
    assert headers["User-Agent"] == firefox


def test_headers_safari_minimal():
    safari = next(ua for ua in DESKTOP_USER_AGENTS if "Safari/" in ua and "Chrome/" not in ua)
    profile = FingerprintProfile(user_agent=safari, platform="MacIntel", device="desktop")
    headers = default_headers(profile)
    assert "sec-ch-ua" not in headers
    assert "DNT" not in headers
    assert "Accept" in headers


def test_platform_consistent_with_ua():
    mac = next(ua for ua in DESKTOP_USER_AGENTS if "Macintosh" in ua)
    headers = default_headers(FingerprintProfile(user_agent=mac, platform="MacIntel", device="desktop"))
    assert "MacIntel" in headers["sec-ch-ua-platform"]


def test_extra_headers_merged():
    profile = build_profile(seed=0)
    profile.extra_headers = {"X-Custom": "1"}
    headers = default_headers(profile)
    assert headers["X-Custom"] == "1"


def test_viewport_dict_is_copy():
    profile = build_profile(seed=0)
    view = viewport_dict(profile)
    view["width"] = 1
    assert profile.viewport["width"] != 1
