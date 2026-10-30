"""
Scheduler and runner for Python workers.
Handles job polling, discovery cycles, and worker lifecycle.
"""

from __future__ import annotations

import asyncio
import signal
import structlog
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..shared.config import get_settings
from ..shared.types import Job, JobStatus
from ..discovery.engine import run_discovery_once
from ..processor import JobProcessor, run_worker
from ..shared.database import Database, get_database

logger = structlog.get_logger(__name__)


class JobScheduler:
    """Scheduler for coordinating discovery and job processing."""

    def __init__(self, runner_id: str = None, db: Database = None):
        self.settings = get_settings()
        self.runner_id = runner_id or f"scheduler-{uuid.uuid4().hex[:8]}"
        self.db = db or get_database()
        self.processor = JobProcessor(self.db, self.runner_id)
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._last_discovery = None
        self._discovery_interval = self.settings.source_poll_interval * 60  # seconds

    async def start(self) -> None:
        """Start the scheduler."""
        await self.db.connect()
        self._running = True

        logger.info("scheduler_started", runner_id=self.runner_id)

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._discovery_loop()),
            asyncio.create_task(self._job_processing_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._health_check_loop()),
        ]

        # Wait for all tasks
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop the scheduler gracefully."""
        logger.info("scheduler_stopping", runner_id=self.runner_id)
        self._running = False

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        # Wait for cancellation
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Stop processor
        self.processor.stop()

        await self.db.close()
        logger.info("scheduler_stopped", runner_id=self.runner_id)

    async def _discovery_loop(self) -> None:
        """Periodic discovery cycle."""
        while self._running:
            try:
                # Check if it's time for discovery
                now = datetime.utcnow()
                if self._last_discovery is None or (now - self._last_discovery).total_seconds() >= self._discovery_interval:
                    logger.info("running_discovery_cycle")
                    result = await run_discovery_once()
                    logger.info("discovery_cycle_complete", **result)
                    self._last_discovery = now

            except Exception as e:
                logger.error("discovery_loop_error", error=str(e))

            await asyncio.sleep(60)  # Check every minute

    async def _job_processing_loop(self) -> None:
        """Process jobs continuously."""
        while self._running:
            try:
                # Process one job
                await self.processor.process_next_job()

            except Exception as e:
                logger.error("job_processing_loop_error", error=str(e))
                await asyncio.sleep(5)

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of old data."""
        while self._running:
            try:
                await self._cleanup_old_jobs()
                await self._cleanup_temp_files()
            except Exception as e:
                logger.error("cleanup_loop_error", error=str(e))

            await asyncio.sleep(3600)  # Every hour

    async def _cleanup_old_jobs(self) -> None:
        """Clean up old completed/failed jobs."""
        # Keep last 10000 jobs, delete older completed ones
        await self.db.execute(
            """DELETE FROM jobs
               WHERE id IN (
                   SELECT id FROM jobs
                   WHERE status IN ('PUBLISHED', 'FAILED', 'CANCELLED')
                   AND completed_at < datetime('now', '-30 days')
                   ORDER BY completed_at ASC
                   LIMIT 1000
               )"""
        )

    async def _cleanup_temp_files(self) -> None:
        """Clean up temporary download files."""
        import os
        from pathlib import Path

        temp_dir = Path(self.settings.temp_dir)
        if not temp_dir.exists():
            return

        # Delete files older than 24 hours
        now = datetime.utcnow().timestamp()
        for file_path in temp_dir.rglob("*"):
            if file_path.is_file():
                try:
                    mtime = file_path.stat().st_mtime
                    if now - mtime > 86400:  # 24 hours
                        file_path.unlink()
                except Exception:
                    pass

    async def _health_check_loop(self) -> None:
        """Periodic health checks."""
        while self._running:
            try:
                # Check disk space
                await self._check_disk_space()

                # Update runner heartbeat
                await self._update_runner_heartbeat()

            except Exception as e:
                logger.error("health_check_error", error=str(e))

            await asyncio.sleep(300)  # Every 5 minutes

    async def _check_disk_space(self) -> None:
        """Check available disk space."""
        import shutil

        total, used, free = shutil.disk_usage(self.settings.temp_dir)
        free_gb = free / (1024**3)

        if free_gb < self.settings.min_disk_space_gb:
            logger.warning("low_disk_space", free_gb=free_gb, required_gb=self.settings.min_disk_space_gb)

    async def _update_runner_heartbeat(self) -> None:
        """Update runner heartbeat in database."""
        # Could store runner status in a runners table
        pass

    async def run_single_discovery(self) -> Dict[str, Any]:
        """Run a single discovery cycle (for manual trigger)."""
        return await run_discovery_once()

    async def run_single_job(self, job_id: str) -> bool:
        """Run a specific job by ID."""
        job_data = await self.db.fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
        if not job_data:
            return False

        job = Job(**job_data)
        return await self.processor.process_job(job)

    async def retry_failed_jobs(self, max_jobs: int = 10) -> int:
        """Retry failed jobs that haven't exceeded max retries."""
        jobs = await self.db.fetchall(
            """SELECT * FROM jobs
               WHERE status = 'FAILED' AND retry_count < max_retries
               ORDER BY updated_at ASC
               LIMIT ?""",
            (max_jobs,),
        )

        count = 0
        for job_data in jobs:
            job = Job(**job_data)
            if job.can_retry():
                await self.db.execute(
                    """UPDATE jobs SET status = 'QUEUED', error_message = NULL,
                       assigned_runner = NULL, lease_expires_at = NULL
                       WHERE id = ?""",
                    (job.id,),
                )
                count += 1

        return count


async def run_scheduler(runner_id: str = None) -> None:
    """Run the scheduler as a standalone process."""
    scheduler = JobScheduler(runner_id)

    # Handle signals
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("signal_received", signal="SIGTERM")
        asyncio.create_task(scheduler.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        await scheduler.start()
    except KeyboardInterrupt:
        await scheduler.stop()


async def run_discovery_only() -> Dict[str, Any]:
    """Run discovery only (for cron/CLI)."""
    return await run_discovery_once()


async def run_worker_only(runner_id: str = None, max_jobs: int = 0) -> None:
    """Run worker only (for CLI)."""
    await run_worker(runner_id, max_jobs)