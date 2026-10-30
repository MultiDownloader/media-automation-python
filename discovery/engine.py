"""
Discovery engine for coordinating source polling and release normalization.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import structlog
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from ..shared.config import get_config
from ..shared.types import (
    MediaType,
    NormalizedRelease,
    RawRelease,
    SourceConfig,
)
from ..sources.registry import SourceManager, registry as source_registry
from ..shared.database import Database
from ..analytics import (
    ActorType,
    EventCategory,
    Outcome,
    SourceCheckResult,
    TraceStatus,
    get_collector,
)
from ..shared.content_policy import evaluate_for_async_db

logger = structlog.get_logger(__name__)


class DiscoveryEngine:
    """Main discovery engine coordinating sources and deduplication."""

    def __init__(self, db: Database):
        self.db = db
        self.config = get_config()
        self.source_manager = SourceManager(source_registry)
        self._seen_hashes: Set[str] = set()
        # Analytics collector: never required for operational success
        # (all collector methods swallow-and-log their own failures)
        self._analytics = get_collector("discovery-engine")

    async def run_discovery_cycle(self) -> Dict[str, Any]:
        """Run a complete discovery cycle across all sources."""
        logger.info("discovery_cycle_start")

        stats = {
            "sources_polled": 0,
            "releases_found": 0,
            "releases_new": 0,
            "releases_duplicate": 0,
            "releases_blocked": 0,
            "errors": [],
        }

        # Load site allowlist for this cycle
        try:
            al_rows = await self.db.fetchall(
                "SELECT host FROM site_allowlist WHERE enabled = 1"
            )
            allowed_hosts = [r["host"] for r in al_rows]
        except Exception:
            allowed_hosts = []

        # Root trace for this discovery cycle
        trace_ctx = await self._analytics.start_trace("discovery.cycle", "discovery-engine")
        cycle_started = asyncio.get_event_loop().time()

        try:
            # Poll all sources, respecting site allowlist
            results = await self.source_manager.poll_all_sources(allowed_hosts=allowed_hosts)
            stats["sources_polled"] = len(results)

            # Process releases from each source
            for source_name, result in results.items():
                src_started = asyncio.get_event_loop().time()
                src_found = 0
                src_new = 0
                src_dup = 0

                if result.errors:
                    stats["errors"].extend([f"{source_name}: {e}" for e in result.errors])

                for release in result.releases:
                    stats["releases_found"] += 1
                    src_found += 1

                    try:
                        is_new = await self.process_release(release, source_name)
                        if is_new:
                            stats["releases_new"] += 1
                            src_new += 1
                        else:
                            # Check if it was blocked by policy (we don't have that info here)
                            # For now, count as duplicate - the analytics event will show details
                            stats["releases_duplicate"] += 1
                            src_dup += 1
                    except Exception as e:
                        logger.error(
                            "release_processing_failed",
                            source=source_name,
                            release=release.title,
                            error=str(e),
                        )
                        stats["errors"].append(f"{source_name}/{release.title}: {e}")

                # Record per-source check run (source intelligence)
                duration_ms = int((asyncio.get_event_loop().time() - src_started) * 1000)
                has_errors = len(result.errors) > 0
                await self._analytics.record_source_check(SourceCheckResult(
                    source_id=source_name,
                    run_type="scheduled",
                    status="failed" if has_errors and src_found == 0 else
                           ("partial" if has_errors else "completed"),
                    started_at=datetime.utcnow().isoformat() + "Z",
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    duration_ms=duration_ms,
                    items_discovered=src_found,
                    items_new=src_new,
                    items_duplicate=src_dup,
                    error_category="source_error" if has_errors else None,
                    error_message=result.errors[0] if has_errors else None,
                    trace_id=trace_ctx.trace_id,
                ))
                await self._analytics.emit(
                    event_type="source.check_run",
                    category=EventCategory.SOURCE,
                    outcome=Outcome.FAILURE if has_errors and src_found == 0 else
                            (Outcome.PARTIAL if has_errors else Outcome.SUCCESS),
                    actor_type=ActorType.SCHEDULED,
                    source_id=source_name,
                    trace_id=trace_ctx.trace_id,
                    duration_ms=duration_ms,
                    context={
                        "items_discovered": src_found,
                        "items_new": src_new,
                        "items_duplicate": src_dup,
                    },
                )

        except Exception as e:
            logger.error("discovery_cycle_failed", error=str(e))
            stats["errors"].append(str(e))
            await self._analytics.finish_trace(
                trace_ctx.trace_id,
                status=TraceStatus.FAILED,
                duration_ms=int((asyncio.get_event_loop().time() - cycle_started) * 1000),
            )
        else:
            await self._analytics.finish_trace(
                trace_ctx.trace_id,
                status=TraceStatus.COMPLETED,
                duration_ms=int((asyncio.get_event_loop().time() - cycle_started) * 1000),
            )

        # Cycle-level health snapshot for the System Health dashboard
        await self._analytics.record_health_snapshot(
            snapshot_type="discovery_cycle",
            component="discovery-engine",
            status="failed" if stats["errors"] else "ok",
            metric_name="releases_new",
            metric_value=float(stats["releases_new"]),
            details={"sources_polled": stats["sources_polled"], "errors": len(stats["errors"])},
        )

        logger.info("discovery_cycle_complete", **stats)
        return stats

    async def process_release(self, raw: RawRelease, source_name: str) -> bool:
        """Process a raw release: normalize, deduplicate, and store."""
        # Create normalized release
        normalized = NormalizedRelease.from_raw(raw)

        # Content policy gate — the user's site allowlist + filter decisions
        # are authoritative. Blocked releases are recorded (auditable, visible
        # in the dashboard as rejected) but never queued for processing.
        decision = await evaluate_for_async_db(
            self.db,
            source_url=normalized.source_url,
            title=normalized.title,
            media_type=normalized.media_type.value,
        )
        if not decision.allowed:
            logger.info(
                "release_blocked_by_policy",
                title=normalized.title,
                reason=decision.reason,
                blocked_by=decision.blocked_by,
            )
            await self.save_release(normalized, approval_status="rejected")
            await self._analytics.emit(
                event_type="filter.blocked",
                category=EventCategory.ENTITY,
                outcome=Outcome.SUCCESS,
                source_id=source_name,
                context={
                    "release_id": normalized.id,
                    "title": normalized.title,
                    **decision.to_dict(),
                },
            )
            return False

        # Check for duplicates
        if await self.is_duplicate(normalized):
            logger.debug("release_duplicate", title=normalized.title, source=source_name)
            await self._analytics.emit(
                event_type="entity.duplicate_detected",
                category=EventCategory.ENTITY,
                outcome=Outcome.SUCCESS,
                source_id=source_name,
                context={"title": normalized.title, "search_key": normalized.search_key},
            )
            return False

        # Save to database
        await self.save_release(normalized)

        # Queue for metadata matching
        await self.queue_metadata_matching(normalized)

        await self._analytics.emit(
            event_type="entity.discovered",
            category=EventCategory.ENTITY,
            outcome=Outcome.SUCCESS,
            source_id=source_name,
            context={
                "release_id": normalized.id,
                "title": normalized.title,
                "media_type": normalized.media_type.value,
            },
        )

        return True

    async def is_duplicate(self, release: NormalizedRelease) -> bool:
        """Check if release is a duplicate."""
        # Check by search key (title + year)
        existing = await self.db.fetchone(
            "SELECT id FROM releases WHERE search_key = ? AND media_type = ?",
            (release.search_key, release.media_type.value),
        )
        if existing:
            return True

        # Check by source external ID
        existing = await self.db.fetchone(
            "SELECT id FROM releases WHERE source_id = ? AND source_id_external = ?",
            (release.source_id, release.source_id_external),
        )
        if existing:
            return True

        # Content hash for fuzzy dedup
        content_hash = self.compute_content_hash(release)
        if content_hash in self._seen_hashes:
            return True

        self._seen_hashes.add(content_hash)
        return False

    def compute_content_hash(self, release: NormalizedRelease) -> str:
        """Compute content hash for fuzzy deduplication."""
        content = f"{release.normalized_title}|{release.year}|{release.season}|{release.episode}|{release.media_type.value}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def save_release(self, release: NormalizedRelease, approval_status: str = "pending") -> None:
        """Save normalized release to database."""
        await self.db.execute(
            """INSERT INTO releases (
                id, source_id, source_url, source_id_external, title, original_title,
                year, season, episode, media_type, release_date, normalized_title,
                search_key, status, approval_status, discovered_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                release.id,
                release.source_id,
                release.source_url,
                release.source_id_external,
                release.title,
                release.original_title,
                release.year,
                release.season,
                release.episode,
                release.media_type.value,
                release.release_date,
                release.normalized_title,
                release.search_key,
                release.status,
                approval_status,
                release.discovered_at,
            ),
        )

    async def queue_metadata_matching(self, release: NormalizedRelease) -> None:
        """Queue release for metadata matching."""
        # In a real implementation, this would create a job or send to queue
        logger.info("release_queued_for_metadata", release_id=release.id, title=release.title)

    async def match_metadata(self, release_id: str) -> Optional[Dict[str, Any]]:
        """Match release against metadata providers (TMDb, etc.)."""
        release = await self.db.fetchone("SELECT * FROM releases WHERE id = ?", (release_id,))
        if not release:
            return None

        # Try TMDb match
        match = await self.match_tmdb(release)
        if match:
            return match

        return None

    async def match_tmdb(self, release: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Match release against TMDb."""
        # This would use the TMDb API to search for matches
        # For now, return a mock structure
        return {
            "provider": "tmdb",
            "provider_id": "12345",
            "provider_type": release["media_type"],
            "confidence": 0.95,
            "match_method": "fuzzy",
        }


async def run_discovery_once() -> Dict[str, Any]:
    """Run a single discovery cycle (for CLI/scheduled runs)."""
    from ..shared.database import get_database

    db = get_database()
    await db.connect()

    try:
        engine = DiscoveryEngine(db)
        return await engine.run_discovery_cycle()
    finally:
        await db.close()