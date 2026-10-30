"""
RSS/Atom feed source adapter.
Parses RSS feeds for new media releases.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import feedparser
import structlog

from .base import RSSSource, SourceConfig
from ..shared.types import MediaType, RawRelease

logger = structlog.get_logger(__name__)


class GenericRSSSource(RSSSource):
    """Generic RSS feed parser for media releases."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.url = config.config.get("url")
        self.media_type = MediaType(config.config.get("media_type", "movie"))
        self.title_pattern = config.config.get("title_pattern")
        self.year_pattern = config.config.get("year_pattern", r"\b(19|20)\d{2}\b")
        self.season_episode_pattern = config.config.get("season_episode_pattern", r"[Ss](\d{1,2})[Ee](\d{1,2})")
        self.date_format = config.config.get("date_format")

        if not self.url:
            raise ValueError("RSS URL required")

        self.config.config["url"] = self.url

    def parse_entry(self, entry: Any, feed_url: str) -> RawRelease:
        """Parse RSS entry into RawRelease."""
        title = entry.get("title", "Unknown")
        link = entry.get("link", feed_url)
        guid = entry.get("id") or entry.get("guid") or link
        published = entry.get("published") or entry.get("updated")
        summary = entry.get("summary") or entry.get("description", "")

        # Parse date
        release_date = None
        if published:
            try:
                dt = feedparser._parse_date(published)
                if dt:
                    release_date = datetime(*dt[:6]).isoformat()
            except Exception:
                pass

        # Extract year from title
        year = None
        year_match = re.search(self.year_pattern, title)
        if year_match:
            try:
                year = int(year_match.group())
            except ValueError:
                pass

        # Extract season/episode
        season = None
        episode = None
        se_match = re.search(self.season_episode_pattern, title, re.IGNORECASE)
        if se_match:
            try:
                season = int(se_match.group(1))
                episode = int(se_match.group(2))
            except ValueError:
                pass

        # Apply title pattern if configured
        clean_title = title
        if self.title_pattern:
            match = re.search(self.title_pattern, title)
            if match:
                clean_title = match.group(1) if match.groups() else match.group(0)

        # Build source data
        source_data = {
            "feed_url": feed_url,
            "entry_link": link,
            "summary": summary,
            "published": published,
            "author": entry.get("author"),
            "tags": [tag.term for tag in entry.get("tags", [])] if hasattr(entry, "tags") else [],
        }

        return RawRelease(
            source_id=self.config.id,
            source_url=link,
            source_id_external=guid,
            title=clean_title.strip(),
            original_title=title,
            year=year,
            season=season,
            episode=episode,
            media_type=self.media_type,
            release_date=release_date,
            source_data=source_data,
        )


class HorribleSubsRSSSource(RSSSource):
    """Specialized parser for HorribleSubs/SubsPlease-style anime RSS feeds."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.url = config.config.get("url")

        if not self.url:
            raise ValueError("RSS URL required")

        self.config.config["url"] = self.url

    def parse_entry(self, entry: Any, feed_url: str) -> RawRelease:
        """Parse anime RSS entry with [Group] Title - Episode [Quality] format."""
        title = entry.get("title", "Unknown")
        link = entry.get("link", feed_url)
        guid = entry.get("id") or entry.get("guid") or link
        published = entry.get("published") or entry.get("updated")

        # Parse date
        release_date = None
        if published:
            try:
                dt = feedparser._parse_date(published)
                if dt:
                    release_date = datetime(*dt[:6]).isoformat()
            except Exception:
                pass

        # Parse title format: [Group] Series Title - 01 [1080p]
        # Or: [SubsPlease] Spy x Family - 01 [1080p]
        group = None
        series_title = title
        episode = None
        quality = None

        # Extract group
        group_match = re.match(r"\[([^\]]+)\]", title)
        if group_match:
            group = group_match.group(1)
            series_title = title[group_match.end():].strip()

        # Extract episode number
        ep_match = re.search(r'[-\s](\d{1,4})(?:\s|$|\[)', series_title)
        if ep_match:
            try:
                episode = int(ep_match.group(1))
                series_title = series_title[:ep_match.start()].strip()
            except ValueError:
                pass

        # Extract quality
        qual_match = re.search(r'\[(\d{3,4}p)\]', title)
        if qual_match:
            quality = qual_match.group(1)

        # Extract year if present
        year = None
        year_match = re.search(r'\b(19|20)\d{2}\b', series_title)
        if year_match:
            try:
                year = int(year_match.group())
                series_title = series_title[:year_match.start()].strip()
            except ValueError:
                pass

        source_data = {
            "feed_url": feed_url,
            "entry_link": link,
            "group": group,
            "quality": quality,
            "published": published,
        }

        return RawRelease(
            source_id=self.config.id,
            source_url=link,
            source_id_external=guid,
            title=series_title.strip(),
            original_title=title,
            year=year,
            season=1,  # Assume season 1 for anime
            episode=episode,
            media_type=MediaType.ANIME,
            release_date=release_date,
            source_data=source_data,
        )


class NyaaRSSSource(RSSSource):
    """Parser for Nyaa.si RSS feeds."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.url = config.config.get("url")
        self.category = config.config.get("category")  # e.g., "anime", "audio"

        if not self.url:
            raise ValueError("RSS URL required")

        self.config.config["url"] = self.url

    def parse_entry(self, entry: Any, feed_url: str) -> RawRelease:
        """Parse Nyaa RSS entry."""
        title = entry.get("title", "Unknown")
        link = entry.get("link", feed_url)
        guid = entry.get("id") or entry.get("guid") or link
        published = entry.get("published") or entry.get("updated")
        size = entry.get("nyaa_size") or entry.get("size")
        seeders = entry.get("nyaa_seeders")
        leechers = entry.get("nyaa_leechers")
        category = entry.get("nyaa_category") or self.category
        info_hash = entry.get("nyaa_infohash")

        # Parse date
        release_date = None
        if published:
            try:
                dt = feedparser._parse_date(published)
                if dt:
                    release_date = datetime(*dt[:6]).isoformat()
            except Exception:
                pass

        # Parse Nyaa title format: [Group] Title - Episode [Quality]
        # Similar to HorribleSubs but with more variations
        media_type = MediaType.ANIME
        if category and "audio" in category.lower():
            media_type = MediaType.MOVIE  # Or could be MUSIC

        group = None
        series_title = title
        episode = None
        quality = None

        group_match = re.match(r"\[([^\]]+)\]", title)
        if group_match:
            group = group_match.group(1)
            series_title = title[group_match.end():].strip()

        ep_match = re.search(r'[-\s](\d{1,4})(?:\s|$|\[)', series_title)
        if ep_match:
            try:
                episode = int(ep_match.group(1))
                series_title = series_title[:ep_match.start()].strip()
            except ValueError:
                pass

        qual_match = re.search(r'\[(\d{3,4}p)\]', title)
        if qual_match:
            quality = qual_match.group(1)

        year = None
        year_match = re.search(r'\b(19|20)\d{2}\b', series_title)
        if year_match:
            try:
                year = int(year_match.group())
            except ValueError:
                pass

        source_data = {
            "feed_url": feed_url,
            "entry_link": link,
            "size": size,
            "seeders": seeders,
            "leechers": leechers,
            "category": category,
            "info_hash": info_hash,
            "group": group,
            "quality": quality,
            "published": published,
        }

        return RawRelease(
            source_id=self.config.id,
            source_url=link,
            source_id_external=guid,
            title=series_title.strip(),
            original_title=title,
            year=year,
            season=1,
            episode=episode,
            media_type=media_type,
            release_date=release_date,
            source_data=source_data,
        )