"""
yt-dlp based downloader for authorized sources.
"""

from __future__ import annotations

import asyncio
import os
import structlog
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yt_dlp

from ..shared.config import get_settings
from ..shared.types import DownloadRequest, MediaFile, ValidationResult
from ..media_validation.validator import MediaValidator

logger = structlog.get_logger(__name__)


@dataclass
class DownloadResult:
    """Result of a download operation."""
    success: bool
    media_file: Optional[MediaFile] = None
    error: Optional[str] = None
    validation: Optional[ValidationResult] = None


class YTDLPDownloader:
    """Download media using yt-dlp."""

    def __init__(self, ytdlp_path: str = None, ffmpeg_path: str = None):
        self.settings = get_settings()
        self.ytdlp_path = ytdlp_path or self.settings.ytdlp_path or "yt-dlp"
        self.ffmpeg_path = ffmpeg_path or self.settings.ffmpeg_path or "ffmpeg"
        self.validator = MediaValidator(ffmpeg_path=self.ffmpeg_path)

    def _get_ydl_options(self, request: DownloadRequest) -> Dict[str, Any]:
        """Build yt-dlp options from download request."""
        opts = {
            "format": request.quality_preference or "bestvideo+bestaudio/best",
            "outtmpl": str(Path(self.settings.temp_dir) / "%(id)s.%(ext)s"),
            "noplaylist": True,
            "quiet": False,
            "no_warnings": False,
            "ignoreerrors": False,
            "retries": 3,
            "fragment_retries": 3,
            "skip_unavailable_fragments": True,
            "keepvideo": False,
            "postprocessors": [],
        }

        # Add subtitle options
        if request.subtitles:
            opts["writesubtitles"] = True
            opts["writeautomaticsub"] = True
            opts["subtitleslangs"] = ["en", "en-US", "en-GB"]

        # Add format constraints
        if request.expected_format:
            opts["format"] = f"{opts['format']}[ext={request.expected_format}]"

        # Size limit
        if request.expected_size:
            opts["max_filesize"] = request.expected_size

        # FFmpeg location
        if self.ffmpeg_path != "ffmpeg":
            opts["ffmpeg_location"] = self.ffmpeg_path

        return opts

    async def download(self, request: DownloadRequest) -> DownloadResult:
        """Download media from URL."""
        logger.info("download_start", job_id=request.job_id, url=request.source_url)

        # Check disk space
        if not await self._check_disk_space():
            return DownloadResult(
                success=False,
                error=f"Insufficient disk space (need {self.settings.min_disk_space_GB}GB)",
            )

        # Create temp directory
        temp_dir = Path(self.settings.temp_dir) / request.job_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        opts = self._get_ydl_options(request)
        opts["outtmpl"] = str(temp_dir / "%(id)s.%(ext)s")

        try:
            # Run yt-dlp in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None, self._run_ytdlp, request.source_url, opts
            )

            # Find downloaded file
            downloaded_files = list(temp_dir.glob("*"))
            if not downloaded_files:
                return DownloadResult(success=False, error="No file downloaded")

            # Use the largest file (main video)
            media_path = max(downloaded_files, key=lambda f: f.stat().st_size)

            # Create media file record
            media_file = await self._create_media_file(request, media_path, info)

            # Validate
            validation = await self.validator.validate(media_file)
            media_file.validation_status = "valid" if validation.valid else "invalid"
            media_file.validation_details = {
                "checks": validation.checks,
                "details": validation.details,
                "errors": validation.errors,
                "warnings": validation.warnings,
            }

            if not validation.valid:
                logger.warning("download_validation_failed", job_id=request.job_id, errors=validation.errors)
                return DownloadResult(
                    success=False,
                    media_file=media_file,
                    error=f"Validation failed: {', '.join(validation.errors)}",
                    validation=validation,
                )

            logger.info("download_complete", job_id=request.job_id, file=str(media_path))
            return DownloadResult(
                success=True,
                media_file=media_file,
                validation=validation,
            )

        except yt_dlp.utils.DownloadError as e:
            logger.error("download_failed", job_id=request.job_id, error=str(e))
            return DownloadResult(success=False, error=f"Download error: {e}")
        except Exception as e:
            logger.error("download_error", job_id=request.job_id, error=str(e))
            return DownloadResult(success=False, error=str(e))

    def _run_ytdlp(self, url: str, opts: Dict[str, Any]) -> Dict[str, Any]:
        """Run yt-dlp synchronously."""
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return info

    async def _create_media_file(
        self, request: DownloadRequest, file_path: Path, info: Dict[str, Any]
    ) -> MediaFile:
        """Create MediaFile record from downloaded file."""
        import hashlib

        stat = file_path.stat()

        # Compute checksum
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

        # Extract metadata from yt-dlp info
        duration = info.get("duration")
        width = info.get("width")
        height = info.get("height")
        format_id = info.get("format_id", "")
        vcodec = info.get("vcodec", "")
        acodec = info.get("acodec", "")

        # Determine MIME type
        ext = file_path.suffix.lower()
        mime_types = {
            ".mp4": "video/mp4",
            ".mkv": "video/x-matroska",
            ".webm": "video/webm",
            ".avi": "video/x-msvideo",
            ".mov": "video/quicktime",
        }
        mime_type = mime_types.get(ext, "application/octet-stream")

        return MediaFile(
            job_id=request.job_id,
            release_id=request.release_id,
            local_path=str(file_path),
            filename=file_path.name,
            file_size=stat.st_size,
            mime_type=mime_type,
            format=ext.lstrip("."),
            duration=duration,
            width=width,
            height=height,
            bitrate=info.get("tbr"),
            codec_video=vcodec if vcodec != "none" else None,
            codec_audio=acodec if acodec != "none" else None,
            checksum_sha256=sha256.hexdigest(),
        )

    async def _check_disk_space(self) -> bool:
        """Check if sufficient disk space is available."""
        import shutil

        total, used, free = shutil.disk_usage(self.settings.temp_dir)
        free_gb = free / (1024**3)
        return free_gb >= self.settings.min_disk_space_gb


class DirectDownloader:
    """Direct HTTP downloader for authorized direct links."""

    def __init__(self):
        self.settings = get_settings()
        self.validator = MediaValidator()

    async def download(self, request: DownloadRequest) -> DownloadResult:
        """Download file via direct HTTP."""
        import httpx
        import hashlib

        logger.info("direct_download_start", job_id=request.job_id, url=request.source_url)

        if not await self._check_disk_space():
            return DownloadResult(
                success=False,
                error=f"Insufficient disk space",
            )

        temp_dir = Path(self.settings.temp_dir) / request.job_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename
        filename = request.metadata.get("filename") or "download"
        if not Path(filename).suffix and request.expected_format:
            filename += f".{request.expected_format}"

        file_path = temp_dir / filename

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(3600.0)) as client:
                async with client.stream("GET", request.source_url) as response:
                    response.raise_for_status()

                    # Check content length
                    content_length = response.headers.get("content-length")
                    if content_length and request.expected_size:
                        if int(content_length) != request.expected_size:
                            logger.warning(
                                "size_mismatch",
                                expected=request.expected_size,
                                actual=content_length,
                            )

                    # Stream download
                    sha256 = hashlib.sha256()
                    with open(file_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                            sha256.update(chunk)

            # Create media file record
            stat = file_path.stat()
            media_file = MediaFile(
                job_id=request.job_id,
                release_id=request.release_id,
                local_path=str(file_path),
                filename=file_path.name,
                file_size=stat.st_size,
                mime_type=response.headers.get("content-type", "application/octet-stream"),
                format=file_path.suffix.lstrip("."),
                checksum_sha256=sha256.hexdigest(),
            )

            # Validate
            validation = await self.validator.validate(media_file)
            media_file.validation_status = "valid" if validation.valid else "invalid"

            if not validation.valid:
                return DownloadResult(
                    success=False,
                    media_file=media_file,
                    error=f"Validation failed: {', '.join(validation.errors)}",
                    validation=validation,
                )

            return DownloadResult(success=True, media_file=media_file, validation=validation)

        except Exception as e:
            logger.error("direct_download_failed", job_id=request.job_id, error=str(e))
            return DownloadResult(success=False, error=str(e))

    async def _check_disk_space(self) -> bool:
        import shutil
        total, used, free = shutil.disk_usage(self.settings.temp_dir)
        free_gb = free / (1024**3)
        return free_gb >= self.settings.min_disk_space_gb


def create_downloader(source_type: str, **kwargs):
    """Factory to create appropriate downloader."""
    if source_type == "ytdlp":
        return YTDLPDownloader(**kwargs)
    elif source_type == "direct":
        return DirectDownloader()
    else:
        raise ValueError(f"Unknown downloader type: {source_type}")