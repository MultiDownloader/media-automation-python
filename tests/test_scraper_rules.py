"""Tests for scraper.browser rules + media classification (no browser needed).

AdBlockRules and classify_media_url are pure logic; the Playwright fetcher
itself is an optional dependency and is not launched in CI.
"""

from pathlib import Path

from scraper.browser import (
    DEFAULT_RULES_FILE,
    AdBlockRules,
    SniffedMedia,
    classify_media_url,
)


# ---------------------------------------------------------------------------
# AdBlockRules: JSON loading
# ---------------------------------------------------------------------------

def test_default_rules_file_exists_and_loads():
    assert DEFAULT_RULES_FILE.exists()
    rules = AdBlockRules()
    assert len(rules.patterns) > 10
    assert any("doubleclick" in p for p in rules.patterns)


def test_rules_load_from_package_json():
    rules = AdBlockRules(rules_file=DEFAULT_RULES_FILE)
    data_rules = AdBlockRules(rules_file=Path(DEFAULT_RULES_FILE))
    assert rules.patterns == data_rules.patterns


def test_rules_fall_back_to_defaults_on_bad_file(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json", encoding="utf-8")
    rules = AdBlockRules(rules_file=bad)
    assert rules.patterns == AdBlockRules.DEFAULT_PATTERNS


def test_rules_fall_back_on_missing_file(tmp_path):
    missing = tmp_path / "nope.json"
    rules = AdBlockRules(rules_file=missing)
    assert rules.patterns == AdBlockRules.DEFAULT_PATTERNS


def test_custom_patterns_only():
    rules = AdBlockRules(patterns=["only.example.com"])
    assert rules.patterns == ("only.example.com",)


# ---------------------------------------------------------------------------
# AdBlockRules: matching
# ---------------------------------------------------------------------------

def test_should_block_known_ad_hosts():
    rules = AdBlockRules()
    assert rules.should_block("https://pagead2.googlesyndication.com/pagead/js/ads.js") is True
    assert rules.should_block("https://ad.doubleclick.net/track") is True
    assert rules.should_block("https://cdn.taboola.com/dbt/px.js") is True
    assert rules.should_block("https://amazon-adsystem.com/aax2/apstag.js") is True


def test_should_allow_content_hosts():
    rules = AdBlockRules()
    assert rules.should_block("https://example.com/videos/episode1.mp4") is False
    assert rules.should_block("https://cdn.example.com/static/player.js") is False
    assert rules.should_block("https://example.com/article") is False


def test_heavy_resources_blocked_only_when_enabled():
    light = AdBlockRules(block_heavy_resources=False)
    heavy = AdBlockRules(block_heavy_resources=True)
    image = "https://example.com/img/photo.jpg"
    assert light.should_block(image, "image") is False
    assert heavy.should_block(image, "image") is True
    assert heavy.should_block("https://example.com/page", "document") is False


def test_matching_is_case_insensitive():
    rules = AdBlockRules(patterns=["DoubleClick.NET"])
    assert rules.should_block("https://DOUBLECLICK.net/x") is True


def test_empty_patterns_block_nothing():
    rules = AdBlockRules(patterns=[])
    # Falls back to defaults by design (an empty list is almost certainly a
    # broken config); verify the default set is actually active.
    assert rules.should_block("https://x.doubleclick.net/a") is True


# ---------------------------------------------------------------------------
# classify_media_url
# ---------------------------------------------------------------------------

def test_classify_hls_manifest():
    assert classify_media_url("https://cdn.example.com/live/index.m3u8") == "manifest"
    assert classify_media_url("https://cdn.example.com/x", "application/vnd.apple.mpegurl") == "manifest"
    assert classify_media_url("https://cdn.example.com/x", "application/x-mpegURL; charset=utf-8") == "manifest"


def test_classify_dash_manifest():
    assert classify_media_url("https://cdn.example.com/manifest.mpd") == "manifest"
    assert classify_media_url("https://cdn.example.com/x", "application/dash+xml") == "manifest"


def test_classify_video_files():
    for url in (
        "https://cdn.example.com/movie.mp4",
        "https://cdn.example.com/movie.mkv?v=2",
        "https://cdn.example.com/clip.webm",
    ):
        assert classify_media_url(url) == "video"
    assert classify_media_url("https://cdn.example.com/x", "video/mp4") == "video"


def test_classify_audio_files():
    assert classify_media_url("https://cdn.example.com/theme.mp3") == "audio"
    assert classify_media_url("https://cdn.example.com/x", "audio/mpeg") == "audio"


def test_classify_non_media():
    assert classify_media_url("https://example.com/page.html") is None
    assert classify_media_url("https://example.com/app.js", "application/javascript") is None
    assert classify_media_url("https://example.com/segment.ts") is None  # HLS segment, not standalone


# ---------------------------------------------------------------------------
# SniffedMedia shape (used by the browser tier and downstream downloaders)
# ---------------------------------------------------------------------------

def test_sniffed_media_to_dict():
    media = SniffedMedia(url="https://cdn.example.com/a.mp4", content_type="video/mp4", kind="video", status=200)
    assert media.to_dict() == {
        "url": "https://cdn.example.com/a.mp4",
        "content_type": "video/mp4",
        "kind": "video",
        "status": 200,
        "size_hint": None,
    }
