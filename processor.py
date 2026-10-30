"""
Job processor - orchestrates the complete media processing pipeline.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import structlog
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .shared.config import get_settings
from .shared.types import (
    Job,
    JobStatus,
    JobEvent,
    JobCheckpoint,
    DownloadRequest,
    MediaFile,
    UploadRecord,
    PublishRequest,
)
from .downloader.ytdlp import YTDLPDownloader, DirectDownloader, create_downloader
from .media_validation.validator import MediaValidator
from .uploaders.base import BaseUploader, create_uploader
from .shared.database import Database, get_database
from .analytics import (
    ActorType,
    EventCategory,
    JobAttemptRecord,
    Outcome,
    TraceStatus,
    UploadAttemptRecord,
    get_collector,
)
from .shared.content_policy import evaluate_for_async_db

logger = structlog.get_logger(__name__)


class JobProcessor:
    """Main job processor handling the complete pipeline."""

    def __init__(self, db: Database = None, runner_id: str = None):
        self.settings = get_settings()
        self.db = db or get_database()
        self.runner_id = runner_id or f"runner-{uuid.uuid4().hex[:8]}"
        self.downloader = YTDLPDownloader()
        self.direct_downloader = DirectDownloader()
        self.validator = MediaValidator()
        self._running = False
        self._current_job: Optional[Job] = None
        self._shutdown_requested = False
        self._analytics = get_collector("job-processor")

    async def start(self) -> None:
        """Start the job processor."""
        await self.db.connect()
        self._running = True
        logger.info("job_processor_started", runner_id=self.runner_id)

        while self._running and not self._shutdown_requested:
            try:
                await self.process_next_job()
            except Exception as e:
                logger.error("job_processor_error", error=str(e))
                await asyncio.sleep(5)

        logger.info("job_processor_stopped", runner_id=self.runner_id)

    def stop(self) -> None:
        """Stop the job processor gracefully."""
        self._running = False
        self._shutdown_requested = True
        logger.info("job_processor_stop_requested", runner_id=self.runner_id)

    async def process_next_job(self) -> bool:
        """Claim and process the next available job."""
        # Claim job
        job = await self.claim_job()
        if not job:
            # No jobs available, wait
            await asyncio.sleep(self.settings.source_poll_interval)
            return False

        self._current_job = job
        success = await self.process_job(job)
        self._current_job = None
        return success

    async def claim_job(self) -> Optional[Job]:
        """Claim an available job."""
        # Find jobs that are queued or pending metadata
        job_data = await self.db.fetchone(
            """SELECT * FROM jobs
               WHERE status IN ('QUEUED', 'METADATA_PENDING', 'READY', 'APPROVED')
               AND (assigned_runner IS NULL OR lease_expires_at < datetime('now'))
               ORDER BY priority DESC, created_at ASC
               LIMIT 1"""
        )

        if not job_data:
            return None

        job = Job(**job_data)

        # Claim it
        lease_expiry = datetime.utcnow().isoformat() + "Z"
        await self.db.execute(
            """UPDATE jobs SET
               assigned_runner = ?, lease_expires_at = ?, status = 'QUEUED',
               updated_at = datetime('now')
               WHERE id = ? AND (assigned_runner IS NULL OR lease_expires_at < datetime('now'))""",
            (self.runner_id, lease_expiry, job.id),
        )

        # Verify claim
        claimed = await self.db.fetchone("SELECT * FROM jobs WHERE id = ? AND assigned_runner = ?", (job.id, self.runner_id))
        if not claimed:
            return None  # Another runner got it

        job.assigned_runner = self.runner_id
        job.lease_expires_at = lease_expiry
        job.status = JobStatus.QUEUED

        await self.log_event(job.id, "status_change", JobStatus.DISCOVERED, JobStatus.QUEUED, "Job claimed by runner")

        return job

    async def process_job(self, job: Job) -> bool:
        """Process a job through the complete pipeline."""
        logger.info("job_processing_start", job_id=job.id, release_id=job.release_id)

        # Content policy gate — fail fast if the release no longer matches
        # the user's site allowlist or filter decisions (the user is authoritative).
        release = await self.db.fetchone("SELECT * FROM releases WHERE id = ?", (job.release_id,))
        if release:
            decision = await evaluate_for_async_db(
                self.db,
                source_url=release["source_url"],
                title=release["title"],
                media_type=release["media_type"],
                original_title=release.get("original_title"),
            )
            if not decision.allowed:
                logger.warning(
                    "job_blocked_by_policy",
                    job_id=job.id,
                    release_id=job.release_id,
                    reason=decision.reason,
                    blocked_by=decision.blocked_by,
                )
                await self.finalize_job(job, JobStatus.FAILED, f"Blocked by policy: {decision.message}")
                await self._analytics.emit(
                    event_type="filter.blocked",
                    category=EventCategory.JOB,
                    outcome=Outcome.SUCCESS,
                    job_id=job.id,
                    context=decision.to_dict(),
                )
                return False

        # Attempt number = prior retries + 1 (per-job execution attempt)
        attempt_number = job.retry_count + 1
        trace_ctx = await self._analytics.start_trace(
            f"job.pipeline.{job.id}", "job-processor", correlation_id=job.id
        )
        attempt_record = JobAttemptRecord.create(
            job_id=job.id,
            attempt_number=attempt_number,
            status="running",
            runner_id=self.runner_id,
            started_at=datetime.utcnow().isoformat() + "Z",
            trace_id=trace_ctx.trace_id,
        )
        loop = asyncio.get_event_loop()
        started_at = loop.time()

        try:
            # Update status to downloading
            await self.update_job_status(job, JobStatus.DOWNLOADING, "Starting download")

            # Step 1: Download
            if not await self.step_download(job):
                await self._finalize_attempt(attempt_record, "failed", started_at, loop)
                return False

            # Step 2: Validate
            await self.update_job_status(job, JobStatus.VALIDATING, "Validating media")
            if not await self.step_validate(job):
                await self._finalize_attempt(attempt_record, "failed", started_at, loop)
                return False

            # Step 3: Upload
            await self.update_job_status(job, JobStatus.UPLOADING, "Uploading media")
            if not await self.step_upload(job):
                await self._finalize_attempt(attempt_record, "failed", started_at, loop)
                return False

            # Step 4: Publish
            await self.update_job_status(job, JobStatus.PUBLISHED, "Publishing complete")
            await self.finalize_job(job, JobStatus.PUBLISHED)

            await self._finalize_attempt(attempt_record, "completed", started_at, loop)
            await self._analytics.finish_trace(trace_ctx.trace_id, TraceStatus.COMPLETED)

            logger.info("job_completed", job_id=job.id)
            return True

        except Exception as e:
            logger.error("job_failed", job_id=job.id, error=str(e))
            attempt_record.error_category = "processing_error"
            attempt_record.error_message = str(e)
            await self._finalize_attempt(attempt_record, "failed", started_at, loop)
            await self._analytics.finish_trace(trace_ctx.trace_id, TraceStatus.FAILED)
            await self.handle_job_failure(job, str(e))
            return False

    async def _finalize_attempt(
        self,
        attempt: JobAttemptRecord,
        status: str,
        started_at: float,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Record a completed job attempt + emit event (never raises)."""
        attempt.status = status
        attempt.completed_at = datetime.utcnow().isoformat() + "Z"
        attempt.duration_ms = int((loop.time() - started_at) * 1000)
        await self._analytics.record_job_attempt(attempt)
        await self._analytics.emit(
            event_type=f"job.attempt.{status}",
            category=EventCategory.JOB,
            outcome=Outcome.SUCCESS if status == "completed" else Outcome.FAILURE,
            actor_type=ActorType.SERVICE,
            actor_id=self.runner_id,
            job_id=attempt.job_id,
            trace_id=attempt.trace_id,
            duration_ms=attempt.duration_ms,
            error_category=attempt.error_category,
            error_message=attempt.error_message,
            context={"attempt_number": attempt.attempt_number},
        )

    async def step_download(self, job: Job) -> bool:
        """Download media for the job."""
        # Get release info
        release = await self.db.fetchone("SELECT * FROM releases WHERE id = ?", (job.release_id,))
        if not release:
            await self.handle_job_failure(job, "Release not found")
            return False

        # Build download request
        request = DownloadRequest(
            job_id=job.id,
            release_id=job.release_id,
            source_url=release["source_url"],
            source_type=job.metadata.get("source_type", "ytdlp"),
            quality_preference=job.metadata.get("quality"),
            subtitles=job.metadata.get("subtitles", False),
            metadata=job.metadata,
        )

        # Select downloader
        if request.source_type == "direct":
            downloader = self.direct_downloader
        else:
            downloader = self.downloader

        # Run download
        result = await downloader.download(request)

        if not result.success:
            await self.handle_job_failure(job, result.error or "Download failed")
            return False

        # Save media file
        media_file = result.media_file
        await self.save_media_file(media_file)
        job.metadata["media_file_id"] = media_file.id

        await self.save_checkpoint(job.id, "download_complete", {"media_file_id": media_file.id})
        await self.update_job_progress(job, 30, "Download complete")

        return True

    async def step_validate(self, job: Job) -> bool:
        """Validate downloaded media."""
        media_file_id = job.metadata.get("media_file_id")
        if not media_file_id:
            await self.handle_job_failure(job, "No media file to validate")
            return False

        media_file = await self.db.fetchone("SELECT * FROM media_files WHERE id = ?", (media_file_id,))
        if not media_file:
            await self.handle_job_failure(job, "Media file not found")
            return False

        # Re-validate (already done during download, but double-check)
        mf = MediaFile(**media_file)
        validation = await self.validator.validate(mf)

        # Update validation status
        await self.db.execute(
            """UPDATE media_files SET
               validation_status = ?, validation_details = ?, validated_at = datetime('now'),
               updated_at = datetime('now')
               WHERE id = ?""",
            (validation.valid and "valid" or "invalid", json.dumps({
                "checks": validation.checks,
                "details": validation.details,
                "errors": validation.errors,
                "warnings": validation.warnings,
            }), media_file_id),
        )

        if not validation.valid:
            await self.handle_job_failure(job, f"Validation failed: {', '.join(validation.errors)}")
            return False

        await self.save_checkpoint(job.id, "validation_complete", {"validation": "passed"})
        await self.update_job_progress(job, 60, "Validation complete")

        return True

    async def step_upload(self, job: Job) -> bool:
        """Upload media to configured destinations."""
        # Get enabled destinations
        destinations = await self.db.fetchall(
            "SELECT * FROM destinations WHERE enabled = 1"
        )

        if not destinations:
            logger.warning("no_destinations_configured", job_id=job.id)
            # Mark as complete even without destinations
            return True

        media_file_id = job.metadata.get("media_file_id")
        media_file = await self.db.fetchone("SELECT * FROM media_files WHERE id = ?", (media_file_id,))

        # Upload to each destination
        for dest in destinations:
            uploader = create_uploader(dest["type"], dest["config"])
            if not uploader:
                logger.warning("no_uploader_for_destination", destination=dest["name"])
                continue

            # Create upload record
            upload_id = str(uuid.uuid4())
            await self.db.execute(
                """INSERT INTO uploads (
                    id, job_id, release_id, destination_id, destination_type,
                    local_file_id, status, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', '{}')""",
                (upload_id, job.id, job.release_id, dest["id"], dest["type"], media_file_id),
            )

            try:
                await self.update_job_progress(job, 70, f"Uploading to {dest['name']}")

                # Update upload status
                await self.db.execute(
                    "UPDATE uploads SET status = 'uploading', started_at = datetime('now') WHERE id = ?",
                    (upload_id,),
                )

                # Upload
                upload_started = asyncio.get_event_loop().time()
                upload_status = "completed"
                upload_error: Optional[str] = None
                try:
                    async with uploader:
                        result = await uploader.upload(media_file["local_path"], dest["config"])
                except Exception as e:
                    upload_status = "failed"
                    upload_error = str(e)
                    result = {}

                # Record upload attempt for publishing analytics
                upload_duration_ms = int(
                    (asyncio.get_event_loop().time() - upload_started) * 1000
                )
                await self._analytics.record_upload_attempt(UploadAttemptRecord.create(
                    upload_id=upload_id,
                    entity_id=job.release_id,
                    destination_id=dest["id"],
                    job_id=job.id,
                    attempt_number=1,
                    status=upload_status,
                    started_at=datetime.utcnow().isoformat() + "Z",
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    duration_ms=upload_duration_ms,
                    total_bytes=media_file.get("file_size") if media_file else None,
                    remote_object_id=result.get("remote_id"),
                    remote_url=result.get("remote_url"),
                    error_category="upload_error" if upload_error else None,
                    error_message=upload_error,
                ))
                await self._analytics.emit(
                    event_type=f"upload.{upload_status}",
                    category=EventCategory.UPLOAD,
                    outcome=Outcome.SUCCESS if upload_status == "completed" else Outcome.FAILURE,
                    actor_type=ActorType.SERVICE,
                    actor_id=self.runner_id,
                    job_id=job.id,
                    destination_id=dest["id"],
                    duration_ms=upload_duration_ms,
                    error_message=upload_error,
                )

                if upload_status == "failed":
                    logger.error("upload_failed", job_id=job.id, destination=dest["name"], error=upload_error)
                    await self.db.execute(
                        """UPDATE uploads SET
                           status = 'failed', error_message = ?, completed_at = datetime('now'),
                           updated_at = datetime('now')
                           WHERE id = ?""",
                        (upload_error, upload_id),
                    )
                    # Continue with other destinations
                    continue

                # Update upload record
                await self.db.execute(
                    """UPDATE uploads SET
                       status = 'completed', remote_id = ?, remote_url = ?,
                       progress = 100, completed_at = datetime('now'),
                       updated_at = datetime('now')
                       WHERE id = ?""",
                    (result.get("remote_id"), result.get("remote_url"), upload_id),
                )

            except Exception as e:
                logger.error("upload_failed", job_id=job.id, destination=dest["name"], error=str(e))
                await self.db.execute(
                    """UPDATE uploads SET
                       status = 'failed', error_message = ?, completed_at = datetime('now'),
                       updated_at = datetime('now')
                       WHERE id = ?""",
                    (str(e), upload_id),
                )
                # Continue with other destinations

        await self.save_checkpoint(job.id, "upload_complete", {})
        await self.update_job_progress(job, 90, "Uploads complete")

        return True

    async def finalize_job(self, job: Job, status: JobStatus) -> None:
        """Finalize job with given status."""
        await self.db.execute(
            """UPDATE jobs SET
               status = ?, progress = 100, completed_at = datetime('now'),
               updated_at = datetime('now')
               WHERE id = ?""",
            (status.value, job.id),
        )
        await self.log_event(job.id, "status_change", job.status, status, f"Job {status.value.lower()}")

    async def handle_job_failure(self, job: Job, error: str) -> None:
        """Handle job failure with retry logic."""
        job.retry_count += 1

        if job.retry_count <= job.max_retries:
            # Schedule retry
            delay = self.settings.retry_base_delay * (2 ** (job.retry_count - 1))
            await self.db.execute(
                """UPDATE jobs SET
                   status = 'FAILED', retry_count = ?, error_message = ?,
                   updated_at = datetime('now')
                   WHERE id = ?""",
                (job.retry_count, error, job.id),
            )
            await self.log_event(job.id, "error", job.status, JobStatus.FAILED, f"Job failed (retry {job.retry_count}/{job.max_retries}): {error}")

            # Wait before allowing retry (handled by scheduler)
            logger.info("job_scheduled_for_retry", job_id=job.id, retry=job.retry_count, delay=delay)
        else:
            # Max retries exceeded
            await self.db.execute(
                """UPDATE jobs SET
                   status = 'FAILED', error_message = ?, error_code = 'MAX_RETRIES',
                   completed_at = datetime('now'), updated_at = datetime('now')
                   WHERE id = ?""",
                (f"Max retries exceeded: {error}", job.id),
            )
            await self.log_event(job.id, "error", job.status, JobStatus.FAILED, f"Job failed permanently: {error}")

    async def update_job_status(self, job: Job, status: JobStatus, step: str) -> None:
        """Update job status and progress."""
        job.status = status
        job.current_step = step

        await self.db.execute(
            """UPDATE jobs SET
               status = ?, current_step = ?, started_at = COALESCE(started_at, datetime('now')),
               updated_at = datetime('now')
               WHERE id = ?""",
            (status.value, step, job.id),
        )
        await self.log_event(job.id, "status_change", job.status, status, f"Step: {step}")

    async def update_job_progress(self, job: Job, progress: int, step: str) -> None:
        """Update job progress."""
        job.progress = progress
        job.current_step = step

        await self.db.execute(
            "UPDATE jobs SET progress = ?, current_step = ?, updated_at = datetime('now') WHERE id = ?",
            (progress, step, job.id),
        )
        await self.log_event(job.id, "progress", None, None, f"Progress: {progress}% - {step}", "info", {"progress": progress})

    async def save_media_file(self, media_file: MediaFile) -> None:
        """Save media file record."""
        await self.db.execute(
            """INSERT INTO media_files (
                id, job_id, release_id, local_path, filename, file_size,
                mime_type, format, duration, width, height, bitrate,
                codec_video, codec_audio, checksum_sha256, validation_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                media_file.id,
                media_file.job_id,
                media_file.release_id,
                media_file.local_path,
                media_file.filename,
                media_file.file_size,
                media_file.mime_type,
                media_file.format,
                media_file.duration,
                media_file.width,
                media_file.height,
                media_file.bitrate,
                media_file.codec_video,
                media_file.codec_audio,
                media_file.checksum_sha256,
                media_file.validation_status.value,
            ),
        )

    async def save_checkpoint(self, job_id: str, step: str, data: Dict[str, Any]) -> None:
        """Save job checkpoint."""
        await self.db.execute(
            "INSERT INTO job_checkpoints (id, job_id, step, data) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), job_id, step, json.dumps(data)),
        )

    async def log_event(
        self,
        job_id: str,
        event_type: str,
        from_status: Optional[JobStatus],
        to_status: Optional[JobStatus],
        message: str,
        level: str = "info",
        data: Dict[str, Any] = None,
    ) -> None:
        """Log job event."""
        await self.db.execute(
            """INSERT INTO job_events (id, job_id, type, from_status, to_status, message, level, data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()),
                job_id,
                event_type,
                from_status.value if from_status else None,
                to_status.value if to_status else None,
                message,
                level,
                json.dumps(data or {}),
            ),
        )


async def run_worker(runner_id: str = None, max_jobs: int = 0) -> None:
    """Run a worker process."""
    db = get_database()
    await db.connect()

    processor = JobProcessor(db, runner_id)

    try:
        if max_jobs > 0:
            for _ in range(max_jobs):
                if not await processor.process_next_job():
                    break
        else:
            await processor.start()
    finally:
        await db.close()