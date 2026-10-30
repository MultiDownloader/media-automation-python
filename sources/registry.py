"""
Source registry and factory for managing source adapters.
"""

from __future__ import annotations

import asyncio
import structlog
from typing import Any, Dict, List, Optional, Type

from .base import BaseSource, SourceConfig, SourceResult, SourceStatus
from .tmdb import TMDbSource, TMDbAnimeSource
from .rss import GenericRSSSource, HorribleSubsRSSSource, NyaaRSSSource
from ..shared.config import get_settings
from ..shared.types import SourceConfig as SharedSourceConfig
from ..shared.content_policy import normalize_host, load_allowlist
from ..shared.database import Database

logger = structlog.get_logger(__name__)

# Map of source type identifiers to adapter classes
SOURCE_ADAPTERS: Dict[str, Type[BaseSource]] = {
    "tmdb": TMDbSource,
    "tmdb_anime": TMDbAnimeSource,
    "rss": GenericRSSSource,
    "horriblesubs": HorribleSubsRSSSource,
    "nyaa": NyaaRSSSource,
}


class SourceRegistry:
    """Registry for managing source adapters."""

    def __init__(self):
        self._adapters: Dict[str, Type[BaseSource]] = SOURCE_ADAPTERS.copy()
        self._instances: Dict[str, BaseSource] = {}

    def register(self, name: str, adapter_class: Type[BaseSource]) -> None:
        """Register a new source adapter."""
        if not issubclass(adapter_class, BaseSource):
            raise ValueError(f"{adapter_class} must be a subclass of BaseSource")
        self._adapters[name] = adapter_class
        logger.info("source_adapter_registered", name=name, adapter=adapter_class.__name__)

    def get_adapter(self, name: str) -> Optional[Type[BaseSource]]:
        """Get adapter class by name."""
        return self._adapters.get(name)

    def list_adapters(self) -> List[str]:
        """List available adapter names."""
        return list(self._adapters.keys())

    def create_source(self, config: SourceConfig) -> BaseSource:
        """Create a source instance from configuration."""
        adapter_class = self._adapters.get(config.type)
        if not adapter_class:
            raise ValueError(f"Unknown source type: {config.type}")

        # For RSS sources, determine specific adapter based on URL or config
        if config.type == "rss":
            adapter_class = self._resolve_rss_adapter(config)

        return adapter_class(config)

    def _resolve_rss_adapter(self, config: SourceConfig) -> Type[BaseSource]:
        """Determine specific RSS adapter based on URL."""
        url = config.config.get("url", "").lower()

        if "horriblesubs" in url or "subsplease" in url:
            return HorribleSubsRSSSource
        elif "nyaa" in url:
            return NyaaRSSSource

        return GenericRSSSource


class SourceManager:
    """Manages source polling and health monitoring."""

    def __init__(self, registry: SourceRegistry = None):
        self.registry = registry or SourceRegistry()
        self.settings = get_settings()
        self._running = False
        self._poll_tasks: Dict[str, asyncio.Task] = {}

    async def poll_source(self, config: SourceConfig) -> SourceResult:
        """Poll a single source for new releases."""
        logger.info("source_poll_start", source=config.name, type=config.type)

        async with self.registry.create_source(config) as source:
            result = await source.poll()

        logger.info(
            "source_poll_complete",
            source=config.name,
            items_found=result.items_found,
            items_new=result.items_new,
            latency_ms=result.latency_ms,
            status=result.status.value,
        )

        return result

    async def poll_all_sources(
        self, allowed_hosts: Optional[list[str]] = None
    ) -> Dict[str, SourceResult]:
        """Poll all enabled sources, optionally filtered by site allowlist.

        Args:
            allowed_hosts: If provided, only poll sources whose config URL
                host is in this list. None or empty = no filtering.
        """
        from ..shared.config import get_config

        config = get_config()
        enabled_sources = config.settings.enabled_sources

        # If allowlist is provided, pre-filter sources
        if allowed_hosts:
            allowed_set = {h.lower() for h in allowed_hosts}
        else:
            allowed_set = None

        results = {}
        for source_name in enabled_sources:
            source_config = self._get_source_config(source_name)
            if not source_config:
                continue

            # Allowlist check: get the source's URL host
            if allowed_set is not None:
                source_url = source_config.config.get("url") or source_config.config.get("base_url", "")
                host = normalize_host(source_url) if source_url else None
                if host and host not in allowed_set:
                    logger.info(
                        "source_skipped_not_in_allowlist",
                        source=source_name,
                        host=host,
                        allowed_hosts=list(allowed_set),
                    )
                    results[source_name] = SourceResult(
                        releases=[],
                        items_found=0,
                        items_new=0,
                        errors=[],
                        latency_ms=0,
                        status=SourceStatus.INACTIVE,
                    )
                    continue

            try:
                results[source_name] = await self.poll_source(source_config)
            except Exception as e:
                logger.error("source_poll_error", source=source_name, error=str(e))
                results[source_name] = SourceResult(
                    releases=[],
                    items_found=0,
                    items_new=0,
                    errors=[str(e)],
                    latency_ms=0,
                    status=SourceStatus.ERROR,
                )

        return results

    def _get_source_config(self, name: str) -> Optional[SourceConfig]:
        """Get source configuration by name.
        
        In production, this would fetch from database.
        For now, create from settings.
        """
        if name == "tmdb":
            return SourceConfig.create(
                name="TMDb",
                type="tmdb",
                config={
                    "api_key": self.settings.tmdb_api_key,
                    "language": self.settings.tmdb_language,
                },
                poll_interval_minutes=self.settings.source_poll_interval,
                max_items_per_poll=self.settings.source_max_items,
                rate_limit_rpm=self.settings.source_rate_limit,
                timeout_seconds=self.settings.source_timeout,
            )
        elif name == "rss":
            # Parse RSS_SOURCES from settings
            import json
            rss_sources = json.loads(self.settings.rss_sources or "[]")
            if rss_sources:
                first = rss_sources[0]
                return SourceConfig.create(
                    name=first.get("name", "RSS Feed"),
                    type="rss",
                    config=first,
                    poll_interval_minutes=self.settings.source_poll_interval,
                    max_items_per_poll=self.settings.source_max_items,
                    rate_limit_rpm=self.settings.source_rate_limit,
                    timeout_seconds=self.settings.source_timeout,
                )
        return None

    async def check_source_health(self, config: SourceConfig) -> bool:
        """Check health of a source."""
        async with self.registry.create_source(config) as source:
            return await source.check_health()

    async def start_polling(self) -> None:
        """Start background polling for all sources."""
        self._running = True
        logger.info("source_polling_started")

        while self._running:
            try:
                await self.poll_all_sources()
            except Exception as e:
                logger.error("polling_cycle_error", error=str(e))

            # Wait for next poll interval
            await asyncio.sleep(self.settings.source_poll_interval * 60)

    def stop_polling(self) -> None:
        """Stop background polling."""
        self._running = False
        logger.info("source_polling_stopped")


# Global registry instance
registry = SourceRegistry()