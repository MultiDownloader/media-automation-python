"""
HTML parsing helpers for the scraping stack.

Two parser backends:

- BeautifulSoup (``beautifulsoup4``, optional) when installed -- full CSS
  selector support.
- stdlib :class:`html.parser.HTMLParser` fallback -- no third-party deps at
  all, so this module works in CI and in minimal worker images. The fallback
  supports the subset the platform actually uses: element text by tag/attr,
  meta tags, links and media anchors.

``find_media_links`` is regex+attribute based and needs no parser at all.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from queue_workers.envelope import get_logger

logger = get_logger(__name__)


def _has_bs4() -> bool:
    try:
        import bs4  # noqa: F401
    except ImportError:
        return False
    return True


# ============================================================================
# Meta / link extraction (stdlib fallback parser)
# ============================================================================

@dataclass
class _StdlibCollector(HTMLParser):
    """Collects text, meta tags and anchors from HTML without third-party deps."""

    texts: List[str] = field(default_factory=list)
    metas: Dict[str, str] = field(default_factory=dict)
    links: List[str] = field(default_factory=list)
    media_attrs: List[str] = field(default_factory=list)
    _capture_depth: int = 0
    _buf: List[str] = field(default_factory=list)

    #: attributes that commonly hold direct media URLs
    MEDIA_ATTRS = ("src", "data-src", "data-video", "data-hls", "data-url", "poster")

    def __post_init__(self) -> None:
        # dataclass generates __init__, so the HTMLParser options must be set
        # here rather than passed to the constructor.
        super().__init__(convert_charrefs=True)

    def handle_starttag(self, tag: str, attrs) -> None:
        attr = {k.lower(): (v or "") for k, v in attrs}
        if tag == "meta":
            key = attr.get("name") or attr.get("property") or attr.get("itemprop")
            content = attr.get("content")
            if key and content:
                self.metas[key.lower()] = content
        elif tag == "a":
            href = attr.get("href")
            if href:
                self.links.append(href)
        if tag in ("source", "video", "audio", "track"):
            for name in self.MEDIA_ATTRS:
                value = attr.get(name)
                if value:
                    self.media_attrs.append(value)
        if tag in ("script", "style"):
            self._capture_depth += 1
        elif tag in ("title", "h1", "h2", "h3", "p", "span", "li", "td"):
            self._capture_depth += 1
            self._buf.append("")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._capture_depth = max(0, self._capture_depth - 1)
        elif tag in ("title", "h1", "h2", "h3", "p", "span", "li", "td"):
            self._capture_depth = max(0, self._capture_depth - 1)
            if self._buf:
                text = "".join(self._buf).strip()
                if text:
                    self.texts.append(text)
                self._buf.pop()

    def handle_data(self, data: str) -> None:
        if self._capture_depth > 0 and self._buf:
            self._buf[-1] += data


def _stdlib_collect(html: str) -> _StdlibCollector:
    collector = _StdlibCollector()
    collector.feed(html)
    return collector


# ============================================================================
# Public API
# ============================================================================

def select_text(html: str, selector: str, default: Optional[str] = None) -> Optional[str]:
    """First matching element's text.

    With bs4 installed ``selector`` is any CSS selector. Without bs4 the
    selector is interpreted as a simple ``tag`` or ``tag[attr]``/``tag.class``
    form (what the platform's own parsers use).
    """
    if _has_bs4():
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        node = soup.select_one(selector)
        if node is None:
            return default
        text = node.get_text(" ", strip=True)
        return text or default

    tag, attr, value = _parse_simple_selector(selector)
    collector = _stdlib_collect(html)
    if attr is None:
        # Re-parse keeping per-tag text: simplest reliable approach is a second
        # pass with a tag-aware collector.
        return _stdlib_text_by_tag(html, tag) or default
    # Attribute selectors need a dedicated scan.
    return _stdlib_attr_text(html, tag, attr, value) or default


def select_all_texts(html: str, selector: str) -> List[str]:
    """All matching elements' texts, in document order."""
    if _has_bs4():
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        return [n.get_text(" ", strip=True) for n in soup.select(selector) if n.get_text(strip=True)]

    tag, attr, value = _parse_simple_selector(selector)
    if attr is None:
        return _stdlib_texts_by_tag(html, tag)
    return _stdlib_all_attr_text(html, tag, attr, value)


def extract_meta(html: str) -> Dict[str, str]:
    """All ``<meta>`` tags as ``{name/property: content}`` (lowercased keys)."""
    if _has_bs4():
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        out: Dict[str, str] = {}
        for node in soup.find_all("meta"):
            key = node.get("name") or node.get("property") or node.get("itemprop")
            content = node.get("content")
            if key and content:
                out[str(key).lower()] = str(content)
        return out
    return dict(_stdlib_collect(html).metas)


def extract_links(html: str, base_url: str, same_host_only: bool = True) -> List[str]:
    """Absolute links found in ``html``.

    Relative URLs are resolved against ``base_url``. With
    ``same_host_only`` the result keeps only links on the page's own host
    (``www.`` prefixes are treated as equal), which is what discovery wants.
    """
    raw: List[str]
    if _has_bs4():
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        raw = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    else:
        raw = list(_stdlib_collect(html).links)

    base_host = (urlparse(base_url).hostname or "").lower()
    out: List[str] = []
    seen = set()
    for href in raw:
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        if same_host_only:
            host = (urlparse(absolute).hostname or "").lower()
            if host.startswith("www."):
                host = host[4:]
            target = base_host[4:] if base_host.startswith("www.") else base_host
            if host != target:
                continue
        if absolute not in seen:
            seen.add(absolute)
            out.append(absolute)
    return out


#: Regex for media file extensions in URLs.
_MEDIA_EXT_RE = re.compile(r"\.(mp4|m4v|mkv|webm|mov|avi|mp3|m4a|ogg|opus|m3u8|mpd)(\?|#|$)", re.I)

#: Regex for HLS/DASH manifests specifically.
_MANIFEST_RE = re.compile(r"\.(m3u8|mpd)(\?|#|$)", re.I)


@dataclass
class MediaLink:
    """A media URL found in raw HTML."""

    url: str
    kind: str  # manifest | video | audio

    def to_dict(self) -> Dict[str, str]:
        return {"url": self.url, "kind": self.kind}


def find_media_links(html: str, base_url: str) -> List[MediaLink]:
    """Direct media URLs present in the HTML (no browser needed).

    Sources, in order: ``<source>``/``<video>``/``<audio>`` attributes,
    JSON-embedded URLs (``"file":"â€¦"``/``src: "â€¦"``) and bare URL strings in
    inline scripts. Results are absolute and de-duplicated.
    """
    candidates: List[str] = []

    # 1) Tag attributes via the parser backends.
    if _has_bs4():
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["source", "video", "audio", "track"]):
            for attr in _StdlibCollector.MEDIA_ATTRS:
                value = tag.get(attr)
                if value:
                    candidates.append(str(value))
    else:
        candidates.extend(_stdlib_collect(html).media_attrs)

    # 2) URLs embedded in scripts / JSON blobs.
    candidates.extend(re.findall(r'''["'](?:file|src|url|source|hls|dash)["']\s*:\s*["']([^"']+)["']''', html))
    # 3) Bare media-looking URLs anywhere in the document.
    candidates.extend(re.findall(r'''https?://[^\s"'<>]+''', html))

    out: List[MediaLink] = []
    seen = set()
    for cand in candidates:
        if not cand or cand.startswith("data:"):
            continue
        absolute = urljoin(base_url, cand.strip())
        if not _MEDIA_EXT_RE.search(absolute):
            continue
        kind = "manifest" if _MANIFEST_RE.search(absolute) else (
            "audio" if re.search(r"\.(mp3|m4a|ogg|opus)(\?|#|$)", absolute, re.I) else "video"
        )
        if absolute not in seen:
            seen.add(absolute)
            out.append(MediaLink(url=absolute, kind=kind))
    return out


# ============================================================================
# stdlib-selector helpers (kept tiny on purpose)
# ============================================================================

def _parse_simple_selector(selector: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Parse ``tag``, ``tag.class`` or ``tag[attr=value]`` into parts."""
    selector = selector.strip()
    m = re.match(r"^([a-zA-Z][\w-]*)\[([\w-]+)(?:=['\"]?([^'\"]*)['\"]?)?\]$", selector)
    if m:
        return m.group(1).lower(), m.group(2).lower(), m.group(3)
    m = re.match(r"^([a-zA-Z][\w-]*)\.([\w-]+)$", selector)
    if m:
        # class selectors degrade to "tag with any class": use the tag only
        return m.group(1).lower(), "class", m.group(2)
    return selector, None, None


def _stdlib_text_by_tag(html: str, tag: str) -> str:
    texts = _stdlib_texts_by_tag(html, tag)
    return texts[0] if texts else ""


def _stdlib_texts_by_tag(html: str, tag: str) -> List[str]:
    if _has_bs4():  # pragma: no cover - convenience path
        return select_all_texts(html, tag)
    pattern = re.compile(rf"<{tag}\b[^>]*>(.*?)</{tag}>", re.I | re.S)
    return [re.sub(r"<[^>]+>", " ", m).strip() for m in pattern.findall(html) if m.strip()]


def _stdlib_attr_text(html: str, tag: str, attr: str, value: Optional[str]) -> str:
    matches = _stdlib_all_attr_text(html, tag, attr, value)
    return matches[0] if matches else ""


def _stdlib_all_attr_text(html: str, tag: str, attr: str, value: Optional[str]) -> List[str]:
    pattern = re.compile(rf"<{tag}\b([^>]*)>(.*?)</{tag}>", re.I | re.S)
    attr_re = re.compile(rf"""{attr}\s*=\s*["']([^"']*)["']""", re.I)
    out: List[str] = []
    for attrs, inner in pattern.findall(html):
        m = attr_re.search(attrs)
        if not m:
            continue
        if value is not None and value not in (m.group(1) or ""):
            continue
        text = re.sub(r"<[^>]+>", " ", inner).strip()
        out.append(text)
    return out
