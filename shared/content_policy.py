"""Content policy engine — Python mirror of contentPolicy.ts.

A release is processed/downloaded ONLY when ALL of these hold:
  1. Its source host is on the site allowlist (when the allowlist is
     non-empty; an empty allowlist = "no site configured yet" and allows
     configured sources, surfaced as a warning).
  2. None of the content filters it matches are disabled.

The user's select/deselect decision is authoritative — nothing else
(not approval gates, not defaults) may override a disabled filter.

Keep in sync with:
  cloudflare/workers/api/src/analytics/contentPolicy.py -> .ts
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional
from urllib.parse import urlparse


@dataclass
class ContentFilter:
    filter_key: str
    label: str
    category: str
    enabled: int
    default_enabled: int
    match_keywords: list[str] = field(default_factory=list)
    match_media_types: list[str] = field(default_factory=list)


@dataclass
class AllowlistRow:
    host: str
    url: str
    enabled: int = 1


@dataclass
class PolicyDecision:
    allowed: bool
    matched_filters: list[str] = field(default_factory=list)
    reason: Optional[str] = None  # 'site_not_allowed' | 'filter_blocked'
    blocked_by: list[str] = field(default_factory=list)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "allowed": self.allowed,
            "matched_filters": self.matched_filters,
        }
        if not self.allowed:
            d.update(
                reason=self.reason,
                blocked_by=self.blocked_by,
                message=self.message,
            )
        return d


def normalize_host(url: str) -> Optional[str]:
    """Extract a lowercase hostname from a URL. None if unparseable."""
    try:
        if "://" not in url:
            url = "https://" + url
        host = urlparse(url).hostname
        return host.lower().replace("www.", "", 1) if host else None
    except Exception:
        return None


def _parse_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(x).lower() for x in raw]
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return [str(x).lower() for x in v] if isinstance(v, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def load_filters(conn: sqlite3.Connection) -> list[ContentFilter]:
    """Load all content filters from the DB (empty DB = permissive)."""
    try:
        rows = conn.execute(
            "SELECT filter_key, label, category, enabled, default_enabled,"
            " match_keywords, match_media_types FROM content_filters"
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        ContentFilter(
            filter_key=r[0], label=r[1], category=r[2], enabled=r[3],
            default_enabled=r[4],
            match_keywords=_parse_list(r[5]),
            match_media_types=_parse_list(r[6]),
        )
        for r in rows
    ]


def load_allowlist(conn: sqlite3.Connection) -> tuple[list[str], bool]:
    """Return (enabled hosts, allowlist_configured)."""
    try:
        rows = conn.execute(
            "SELECT host FROM site_allowlist WHERE enabled = 1"
        ).fetchall()
    except sqlite3.Error:
        return [], False
    hosts = [r[0].lower().replace("www.", "", 1) for r in rows]
    return hosts, len(hosts) > 0


def classify_release(
    title: str, media_type: str, filters: Iterable[ContentFilter],
    original_title: str = "",
) -> list[str]:
    """Which filter keys does this release match?"""
    haystack = f"{title} {original_title}".lower()
    matched: list[str] = []
    for f in filters:
        by_media = str(media_type).lower() in f.match_media_types
        by_keyword = any(kw and kw in haystack for kw in f.match_keywords)
        if by_media or by_keyword:
            matched.append(f.filter_key)
    return matched


def evaluate_policy(
    release_host: Optional[str],
    title: str,
    media_type: str,
    filters: Iterable[ContentFilter],
    allowlist_hosts: Iterable[str],
    allowlist_configured: bool,
    original_title: str = "",
) -> PolicyDecision:
    """Full policy evaluation for one release. See module docstring."""
    filters = list(filters)
    matched = classify_release(title, media_type, filters, original_title)

    # 1) Site allowlist — only the user's own site(s) when configured.
    if allowlist_configured:
        host = (release_host or "").lower().replace("www.", "", 1)
        if host not in [h.lower() for h in allowlist_hosts]:
            return PolicyDecision(
                allowed=False,
                matched_filters=matched,
                reason="site_not_allowed",
                message=f'Host "{host or "unknown"}" is not on the site allowlist',
            )

    # 2) Content filters — a disabled filter ALWAYS blocks (user decision).
    disabled = [f for f in filters if f.enabled != 1]
    blocked_by = [f.filter_key for f in disabled if f.filter_key in matched]
    if blocked_by:
        labels = ", ".join(f.label for f in disabled if f.filter_key in blocked_by)
        return PolicyDecision(
            allowed=False,
            matched_filters=matched,
            reason="filter_blocked",
            blocked_by=blocked_by,
            message=f"Blocked by disabled filter(s): {labels}",
        )

    return PolicyDecision(allowed=True, matched_filters=matched)


def evaluate_for_db(
    conn: sqlite3.Connection,
    source_url: str,
    title: str,
    media_type: str,
    original_title: str = "",
) -> PolicyDecision:
    """Convenience: load policy config from DB and evaluate one release."""
    filters = load_filters(conn)
    allowlist_hosts, configured = load_allowlist(conn)
    return evaluate_policy(
        release_host=normalize_host(source_url),
        title=title,
        media_type=media_type,
        filters=filters,
        allowlist_hosts=allowlist_hosts,
        allowlist_configured=configured,
        original_title=original_title,
    )


async def evaluate_for_async_db(
    db: Any,
    source_url: str,
    title: str,
    media_type: str,
    original_title: str = "",
) -> PolicyDecision:
    """Same as evaluate_for_db but for the async DB wrapper (fetchall)."""
    try:
        rows = await db.fetchall(
            "SELECT filter_key, label, category, enabled, default_enabled,"
            " match_keywords, match_media_types FROM content_filters"
        )
        filters = [
            ContentFilter(
                filter_key=r["filter_key"], label=r["label"],
                category=r["category"], enabled=r["enabled"],
                default_enabled=r["default_enabled"],
                match_keywords=_parse_list(r["match_keywords"]),
                match_media_types=_parse_list(r["match_media_types"]),
            )
            for r in rows
        ]
    except Exception:
        filters = []

    try:
        al = await db.fetchall("SELECT host FROM site_allowlist WHERE enabled = 1")
        allowlist_hosts = [r["host"].lower().replace("www.", "", 1) for r in al]
    except Exception:
        allowlist_hosts = []

    return evaluate_policy(
        release_host=normalize_host(source_url),
        title=title,
        media_type=media_type,
        filters=filters,
        allowlist_hosts=allowlist_hosts,
        allowlist_configured=len(allowlist_hosts) > 0,
        original_title=original_title,
    )
