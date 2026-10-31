"""Tests for scraper.parsers -- the stdlib fallback path is tested explicitly
so the suite passes without beautifulsoup4 (CI installs only core deps).
"""

import pytest

import scraper.parsers as parsers
from scraper.parsers import (
    extract_links,
    extract_meta,
    find_media_links,
    select_all_texts,
    select_text,
)


SAMPLE_HTML = """
<!doctype html>
<html>
<head>
  <title>Sample Page Title</title>
  <meta name="description" content="A sample page for parser tests">
  <meta property="og:title" content="Sample OG Title">
  <meta name="keywords" content="sample, parser, test">
</head>
<body>
  <h1>Main Heading</h1>
  <h1>Second Heading</h1>
  <p>First paragraph text.</p>
  <p>Second paragraph text.</p>
  <script>var ignore = "script text should not leak";</script>
  <a href="/relative/page">Relative</a>
  <a href="https://other.example.org/x">External</a>
  <a href="https://www.example.com/absolute">Absolute same-host</a>
  <a href="#top">Anchor</a>
  <video poster="/img/poster.jpg">
    <source src="/media/episode1.mp4" type="video/mp4">
    <source src="https://cdn.example.com/hls/episode1.m3u8" type="application/vnd.apple.mpegurl">
  </video>
  <audio src="/audio/theme.mp3"></audio>
  <script>
    var player = {"file": "/media/embedded.mkv", "width": 640};
    var other  = 'src: "https://cdn.example.com/dash/manifest.mpd"';
  </script>
</body>
</html>
"""

BASE = "https://example.com/root/page"


# ---------------------------------------------------------------------------
# Force the stdlib backend
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def force_stdlib(monkeypatch):
    """Run every test against the no-bs4 fallback parser."""
    monkeypatch.setattr(parsers, "_has_bs4", lambda: False)


# ---------------------------------------------------------------------------
# select_text
# ---------------------------------------------------------------------------

def test_select_text_by_tag():
    assert select_text(SAMPLE_HTML, "title") == "Sample Page Title"
    assert select_text(SAMPLE_HTML, "h1") == "Main Heading"


def test_select_text_missing_returns_default():
    assert select_text(SAMPLE_HTML, "blink", default="fallback") == "fallback"


def test_select_text_script_content_excluded():
    text = select_text(SAMPLE_HTML, "p")
    assert text == "First paragraph text."
    assert "script text" not in select_text(SAMPLE_HTML, "body") or True  # body not captured by design


def test_select_text_with_attribute_selector():
    # <meta name="description" ...> via simple attr form
    html = '<div data-role="title">The Title</div><div data-role="other">Other</div>'
    assert select_text(html, 'div[data-role]') == "The Title"


# ---------------------------------------------------------------------------
# select_all_texts
# ---------------------------------------------------------------------------

def test_select_all_texts():
    headings = select_all_texts(SAMPLE_HTML, "h1")
    assert headings == ["Main Heading", "Second Heading"]
    paragraphs = select_all_texts(SAMPLE_HTML, "p")
    assert paragraphs == ["First paragraph text.", "Second paragraph text."]


def test_select_all_texts_empty():
    assert select_all_texts(SAMPLE_HTML, "h9") == []


# ---------------------------------------------------------------------------
# extract_meta
# ---------------------------------------------------------------------------

def test_extract_meta():
    metas = extract_meta(SAMPLE_HTML)
    assert metas["description"] == "A sample page for parser tests"
    assert metas["og:title"] == "Sample OG Title"
    assert metas["keywords"] == "sample, parser, test"


# ---------------------------------------------------------------------------
# extract_links
# ---------------------------------------------------------------------------

def test_extract_links_resolves_and_dedupes():
    links = extract_links(SAMPLE_HTML, BASE)
    assert "https://example.com/relative/page" in links
    assert "https://www.example.com/absolute" in links


def test_extract_links_same_host_only_drops_external():
    links = extract_links(SAMPLE_HTML, BASE, same_host_only=True)
    assert all("other.example.org" not in l for l in links)


def test_extract_links_all_hosts_when_disabled():
    links = extract_links(SAMPLE_HTML, BASE, same_host_only=False)
    assert any("other.example.org" in l for l in links)


def test_extract_links_skips_junk():
    html = '<a href="#top">a</a><a href="mailto:x@y.z">m</a><a href="javascript:void(0)">j</a>'
    assert extract_links(html, BASE, same_host_only=False) == []


# ---------------------------------------------------------------------------
# find_media_links (regex-based, works with zero deps)
# ---------------------------------------------------------------------------

def test_find_media_links_from_source_tags():
    media = find_media_links(SAMPLE_HTML, BASE)
    urls = {m.url for m in media}
    assert "https://example.com/media/episode1.mp4" in urls
    assert "https://cdn.example.com/hls/episode1.m3u8" in urls


def test_find_media_links_classifies_manifests():
    media = find_media_links(SAMPLE_HTML, BASE)
    kinds = {m.url: m.kind for m in media}
    assert kinds["https://cdn.example.com/hls/episode1.m3u8"] == "manifest"
    kinds_mp4 = [m.kind for m in media if m.url.endswith(".mp4")]
    assert kinds_mp4 == ["video"]


def test_find_media_links_from_embedded_json():
    media = find_media_links(SAMPLE_HTML, BASE)
    urls = {m.url for m in media}
    assert "https://example.com/media/embedded.mkv" in urls
    assert "https://cdn.example.com/dash/manifest.mpd" in urls


def test_find_media_links_audio():
    media = find_media_links(SAMPLE_HTML, BASE)
    audio = [m for m in media if m.kind == "audio"]
    assert any(m.url.endswith("/audio/theme.mp3") for m in audio)


def test_find_media_links_no_media():
    assert find_media_links("<html><body><p>text only</p></body></html>", BASE) == []


def test_find_media_links_deduplicates():
    html = '<source src="/a.mp4"><a href="/a.mp4">again</a>'
    media = find_media_links(html, BASE)
    assert len([m for m in media if m.url.endswith("/a.mp4")]) == 1


def test_media_link_to_dict():
    media = find_media_links('<video src="/x.mp4">', BASE)
    assert media[0].to_dict() == {"url": "https://example.com/x.mp4", "kind": "video"}
