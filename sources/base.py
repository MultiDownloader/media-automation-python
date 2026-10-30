"""
Base classes for source adapters.
Each source implements a consistent interface for discovery.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import urlparse

import httpx
import structlog

from ..shared.config import get_settings
from ..shared.types import (
    MediaType,
    RawRelease,
    SourceConfig,
    SourceStatus,
)

logger = structlog.get_logger(__name__)


@dataclass
class SourceResult:
    """Result of a source poll operation."""
    releases: List[RawRelease]
    items_found: int
    items_new: int
    errors: List[str]
    latency_ms: int
    status: SourceStatus


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""
    rate_per_minute: int
    tokens: float = field(default=0, init=False)
    last_update: float = field(default_factory=time.time, init=False)

    def __post_init__(self):
        self.tokens = self.rate_per_minute

    async def acquire(self) -> None:
        while True:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate_per_minute, self.tokens + elapsed * (self.rate_per_minute / 60.0))
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return

            await asyncio.sleep(0.1)


class BaseSource(ABC):
    """Base class for all source adapters."""

    def __init__(self, config: SourceConfig):
        self.config = config
        self.settings = get_settings()
        self.rate_limiter = RateLimiter(config.rate_limit_rpm)
        self.client: Optional[httpx.AsyncClient] = None
        self._health_status = SourceStatus.INACTIVE
        self._last_error: Optional[str] = None
        self._error_count = 0

    async def __aenter__(self) -> "BaseSource":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_seconds, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=5),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.client:
            await self.client.aclose()

    @abstractmethod
    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        """Fetch new releases from the source.
        
        Yields:
            RawRelease objects for each discovered item.
        """
        pass

    @abstractmethod
    async def check_health(self) -> bool:
        """Check if source is accessible.
        
        Returns:
            True if source is healthy, False otherwise.
        """
        pass

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.config.get(key, default)

    def normalize_title(self, title: str) -> str:
        """Normalize title for matching."""
        # Remove common suffixes/prefixes
        import re
        normalized = title.strip()
        # Remove quality tags
        normalized = re.sub(r'\[.*?\]|\(.*?\)', '', normalized)
        # Remove year at end
        normalized = re.sub(r'\s+\d{4}$', '', normalized)
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    def create_search_key(self, title: str, year: Optional[int] = None) -> str:
        """Create search key for duplicate detection."""
        import re
        key = title.lower()
        key = re.sub(r'[^a-z0-9\s]', '', key)
        key = re.sub(r'\s+', ' ', key).strip()
        if year:
            key += f" {year}"
        return key

    async def poll(self) -> SourceResult:
        """Poll source for new releases."""
        start = time.time()
        releases = []
        items_found = 0
        items_new = 0
        errors = []
        status = SourceStatus.ACTIVE

        try:
            async for release in self.fetch_releases():
                items_found += 1
                releases.append(release)

                # Limit items per poll
                if len(releases) >= self.config.max_items_per_poll:
                    logger.warning(
                        "source_poll_limit_reached",
                        source=self.config.name,
                        limit=self.config.max_items_per_poll,
                    )
                    break

            self._health_status = SourceStatus.ACTIVE
            self._error_count = 0
            self._last_error = None

        except Exception as e:
            self._health_status = SourceStatus.ERROR
            self._error_count += 1
            self._last_error = str(e)
            errors.append(str(e))
            logger.error(
                "source_poll_failed",
                source=self.config.name,
                error=str(e),
                error_count=self._error_count,
            )

            if self._error_count >= 5:
                status = SourceStatus.ERROR

        latency = int((time.time() - start) * 1000)

        return SourceResult(
            releases=releases,
            items_found=items_found,
            items_new=items_new,  # Will be updated by caller after dedup
            errors=errors,
            latency_ms=latency,
            status=status,
        )

    @property
    def health_status(self) -> SourceStatus:
        return self._health_status

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    @property
    def error_count(self) -> int:
        return self._error_count


class APISource(BaseSource):
    """Base class for API-based sources."""

    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        url = self.get_config("url")
        if not url:
            raise ValueError("API URL not configured")

        headers = self.get_config("headers", {})
        params = self.get_config("params", {})
        pagination = self.get_config("pagination", {})

        page = 1
        max_pages = pagination.get("max_pages", 10)

        while page <= max_pages:
            await self.rate_limiter.acquire()

            current_params = {**params}
            if "page_param" in pagination:
                current_params[pagination["page_param"]] = page

            try:
                response = await self.client.get(url, headers=headers, params=current_params)
                response.raise_for_status()
                data = response.json()

                items = self.extract_items(data)
                if not items:
                    break

                for item in items:
                    yield self.parse_item(item)

                # Check for more pages
                if not self.has_more_pages(data, page):
                    break

                page += 1

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    self._health_status = SourceStatus.RATE_LIMITED
                raise

    @abstractmethod
    def extract_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract release items from API response."""
        pass

    @abstractmethod
    def parse_item(self, item: Dict[str, Any]) -> RawRelease:
        """Parse raw API item into RawRelease."""
        pass

    def has_more_pages(self, data: Dict[str, Any], current_page: int) -> bool:
        """Check if there are more pages."""
        pagination = self.get_config("pagination", {})
        if "total_pages_path" in pagination:
            # Navigate to total pages in response
            import jsonpath
            total = jsonpath.jsonpath(data, pagination["total_pages_path"])
            if total and current_page < total[0]:
                return True
        return False

    async def check_health(self) -> bool:
        url = self.get_config("health_check_url") or self.get_config("url")
        if not url:
            return False

        try:
            await self.rate_limiter.acquire()
            response = await self.client.head(url)
            return response.status_code < 500
        except Exception:
            return False


class RSSSource(BaseSource):
    """RSS/Atom feed source."""

    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        import feedparser

        url = self.get_config("url")
        if not url:
            raise ValueError("RSS URL not configured")

        await self.rate_limiter.acquire()

        # Use httpx to fetch, then feedparser to parse
        response = await self.client.get(url)
        response.raise_for_status()

        feed = feedparser.parse(response.text)

        for entry in feed.entries[: self.config.max_items_per_poll]:
            yield self.parse_entry(entry, url)

    @abstractmethod
    def parse_entry(self, entry: Any, feed_url: str) -> RawRelease:
        """Parse RSS entry into RawRelease."""
        pass

    async def check_health(self) -> bool:
        url = self.get_config("url")
        if not url:
            return False

        try:
            await self.rate_limiter.acquire()
            response = await self.client.head(url)
            return response.status_code < 500
        except Exception:
            return False


class ParserSource(BaseSource):
    """Custom parser source for HTML scraping or other methods."""

    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        url = self.get_config("url")
        if not url:
            raise ValueError("Parser URL not configured")

        await self.rate_limiter.acquire()

        response = await self.client.get(url)
        response.raise_for_status()

        items = self.parse_content(response.text, url)

        for item in items[: self.config.max_items_per_poll]:
            yield item

    @abstractmethod
    def parse_content(self, html: str, url: str) -> List[RawRelease]:
        """Parse HTML/content into RawRelease list."""
        pass

    async def check_health(self) -> bool:
        url = self.get_config("url")
        if not url:
            return False

        try:
            await self.rate_limiter.acquire()
            response = await self.client.head(url)
            return response.status_code < 500
        except Exception:
            return False