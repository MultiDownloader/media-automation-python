"""
Media file validation using FFmpeg.
"""

from __future__ import annotations

import asyncio
import json
import structlog
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..shared.config import get_settings
from ..shared.types import MediaFile, ValidationResult

logger = structlog.get_logger(__name__)


class MediaValidator:
    """Validate media files using FFprobe/FFmpeg."""

    def __init__(self, ffmpeg_path: str = None, ffprobe_path: str = None):
        self.settings = get_settings()
        self.ffmpeg_path = ffmpeg_path or self.settings.ffmpeg_path or "ffmpeg"
        self.ffprobe_path = ffprobe_path or self.settings.ffmpeg_path.replace("ffmpeg", "ffprobe") or "ffprobe"

    async def validate(self, media_file: MediaFile) -> ValidationResult:
        """Run all validation checks on a media file."""
        checks = {}
        details = {}
        errors = []
        warnings = []

        file_path = Path(media_file.local_path)

        # Check 1: File exists
        checks["file_exists"] = file_path.exists()
        if not checks["file_exists"]:
            errors.append("File does not exist")
            return ValidationResult(valid=False, checks=checks, details=details, errors=errors, warnings=warnings)

        # Check 2: File size > 0
        stat = file_path.stat()
        checks["size_positive"] = stat.st_size > 0
        if not checks["size_positive"]:
            errors.append("File is empty")

        # Check 3: Size matches expected (if provided)
        if media_file.file_size > 0:
            checks["size_matches"] = stat.st_size == media_file.file_size
            if not checks["size_matches"]:
                errors.append(f"Size mismatch: expected {media_file.file_size}, got {stat.st_size}")

        # Check 4: FFprobe analysis
        probe_result = await self._probe_file(file_path)
        if probe_result:
            checks["format_valid"] = True
            details["probe"] = probe_result

            # Validate duration
            if media_file.duration and probe_result.get("duration"):
                probe_duration = float(probe_result["duration"])
                expected_duration = media_file.duration
                # Allow 5% tolerance
                tolerance = expected_duration * 0.05
                checks["duration_matches"] = abs(probe_duration - expected_duration) <= tolerance
                if not checks["duration_matches"]:
                    warnings.append(f"Duration mismatch: expected {expected_duration}s, got {probe_duration}s")

            # Validate codecs
            video_streams = [s for s in probe_result.get("streams", []) if s.get("codec_type") == "video"]
            audio_streams = [s for s in probe_result.get("streams", []) if s.get("codec_type") == "audio"]

            if video_streams:
                vstream = video_streams[0]
                details["video_codec"] = vstream.get("codec_name")
                details["video_width"] = vstream.get("width")
                details["video_height"] = vstream.get("height")
                details["video_bitrate"] = vstream.get("bit_rate")

                if media_file.width and vstream.get("width") != media_file.width:
                    warnings.append(f"Width mismatch: expected {media_file.width}, got {vstream.get('width')}")
                if media_file.height and vstream.get("height") != media_file.height:
                    warnings.append(f"Height mismatch: expected {media_file.height}, got {vstream.get('height')}")

            if audio_streams:
                astream = audio_streams[0]
                details["audio_codec"] = astream.get("codec_name")
                details["audio_channels"] = astream.get("channels")
                details["audio_sample_rate"] = astream.get("sample_rate")
        else:
            checks["format_valid"] = False
            errors.append("FFprobe failed - file may be corrupted or unsupported format")

        # Check 5: Checksum validation (if checksum provided)
        if media_file.checksum_sha256:
            computed = await self._compute_checksum(file_path)
            checks["checksum_valid"] = computed == media_file.checksum_sha256
            if not checks["checksum_valid"]:
                errors.append("Checksum mismatch - file may be corrupted")
        else:
            checks["checksum_valid"] = True

        # Check 6: Quick corruption test (decode first few seconds)
        if checks.get("format_valid"):
            checks["no_corruption"] = await self._quick_corruption_check(file_path)
            if not checks["no_corruption"]:
                errors.append("Corruption detected in file")

        # Overall validity
        valid = all(checks.values()) if checks else False

        return ValidationResult(
            valid=valid,
            checks=checks,
            details=details,
            errors=errors,
            warnings=warnings,
        )

    async def _probe_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Run ffprobe on file."""
        try:
            cmd = [
                self.ffprobe_path,
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                str(file_path),
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60.0)

            if proc.returncode != 0:
                logger.warning("ffprobe_failed", file=str(file_path), error=stderr.decode())
                return None

            return json.loads(stdout.decode())

        except asyncio.TimeoutError:
            logger.warning("ffprobe_timeout", file=str(file_path))
            return None
        except Exception as e:
            logger.warning("ffprobe_error", file=str(file_path), error=str(e))
            return None

    async def _compute_checksum(self, file_path: Path) -> str:
        """Compute SHA256 checksum."""
        import hashlib

        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def _quick_corruption_check(self, file_path: Path) -> bool:
        """Quick corruption check by decoding first 10 seconds."""
        try:
            cmd = [
                self.ffmpeg_path,
                "-v", "error",
                "-i", str(file_path),
                "-t", "10",
                "-f", "null",
                "-",
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)

            # FFmpeg returns 0 on success, non-zero on error
            # stderr may contain warnings even on success
            if proc.returncode != 0:
                stderr_text = stderr.decode()
                # Filter out non-fatal warnings
                if any(fatal in stderr_text.lower() for fatal in ["error", "corrupt", "invalid", "failed"]):
                    return False

            return True

        except asyncio.TimeoutError:
            logger.warning("corruption_check_timeout", file=str(file_path))
            return False
        except Exception as e:
            logger.warning("corruption_check_error", file=str(file_path), error=str(e))
            return False

    async def extract_thumbnail(self, media_file: MediaFile, timestamp: float = 5.0) -> Optional[Path]:
        """Extract thumbnail from video at given timestamp."""
        import tempfile

        file_path = Path(media_file.local_path)
        if not file_path.exists():
            return None

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            thumb_path = Path(tmp.name)

        try:
            cmd = [
                self.ffmpeg_path,
                "-y",
                "-ss", str(timestamp),
                "-i", str(file_path),
                "-vframes", "1",
                "-vf", "scale=320:-1",
                str(thumb_path),
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=30.0)

            if proc.returncode == 0 and thumb_path.exists():
                return thumb_path

            return None

        except Exception as e:
            logger.warning("thumbnail_extraction_failed", file=str(file_path), error=str(e))
            return None

    async def transcode(
        self,
        media_file: MediaFile,
        output_path: Path,
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        crf: int = 23,
        preset: str = "medium",
    ) -> bool:
        """Transcode media file to target format."""
        file_path = Path(media_file.local_path)
        if not file_path.exists():
            return False

        try:
            cmd = [
                self.ffmpeg_path,
                "-y",
                "-i", str(file_path),
                "-c:v", video_codec,
                "-crf", str(crf),
                "-preset", preset,
                "-c:a", audio_codec,
                "-b:a", "128k",
                str(output_path),
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=3600.0)

            return proc.returncode == 0 and output_path.exists()

        except asyncio.TimeoutError:
            logger.error("transcode_timeout", file=str(file_path))
            return False
        except Exception as e:
            logger.error("transcode_error", file=str(file_path), error=str(e))
            return False