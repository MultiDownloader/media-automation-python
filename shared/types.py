"""
Shared Python types for the media automation platform.
Mirrors the TypeScript types in shared/contracts/types.ts
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

# ============================================================================
# Enums
# ============================================================================

class MediaType(str, Enum):
    MOVIE = "movie"
    TV = "tv"
    ANIME = "anime"


class JobStatus(str, Enum):
    DISCOVERED = "DISCOVERED"
    METADATA_PENDING = "METADATA_PENDING"
    READY = "READY"
    APPROVED = "APPROVED"
    QUEUED = "QUEUED"
    DOWNLOADING = "DOWNLOADING"
    VALIDATING = "VALIDATING"
    UPLOADING = "UPLOADING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class SourceStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"


class DestinationStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class ValidationStatus(str, Enum):
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    CORRUPTED = "corrupted"


class UploadStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


# ============================================================================
# Base Classes
# ============================================================================

def generate_id() -> str:
    """Generate a UUID v7-like ID (timestamp + random)."""
    return uuid.uuid4().hex


def now_iso() -> str:
    """Current time in ISO 8601 format."""
    return datetime.utcnow().isoformat() + "Z"


# ============================================================================
# Source & Discovery Types
# ============================================================================

@dataclass
class SourceConfig:
    id: str
    name: str
    type: str  # 'api' | 'rss' | 'parser'
    enabled: bool
    config: Dict[str, Any]
    poll_interval_minutes: int
    max_items_per_poll: int
    rate_limit_rpm: int
    timeout_seconds: int
    health_check_url: Optional[str] = None
    last_polled_at: Optional[str] = None
    last_error: Optional[str] = None
    error_count: int = 0

    @classmethod
    def create(cls, name: str, type: str, config: Dict[str, Any], **kwargs) -> "SourceConfig":
        return cls(
            id=generate_id(),
            name=name,
            type=type,
            enabled=kwargs.get("enabled", True),
            config=config,
            poll_interval_minutes=kwargs.get("poll_interval_minutes", 60),
            max_items_per_poll=kwargs.get("max_items_per_poll", 50),
            rate_limit_rpm=kwargs.get("rate_limit_rpm", 60),
            timeout_seconds=kwargs.get("timeout_seconds", 30),
            health_check_url=kwargs.get("health_check_url"),
        )


@dataclass
class RawRelease:
    source_id: str
    source_url: str
    source_id_external: str
    title: str
    original_title: Optional[str] = None
    year: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    media_type: MediaType = MediaType.MOVIE
    release_date: Optional[str] = None
    source_data: Dict[str, Any] = field(default_factory=dict)
    discovered_at: str = field(default_factory=now_iso)


@dataclass
class NormalizedRelease:
    id: str
    source_id: str
    source_url: str
    source_id_external: str
    title: str
    original_title: Optional[str] = None
    year: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    media_type: MediaType = MediaType.MOVIE
    release_date: Optional[str] = None
    normalized_title: str = ""
    search_key: str = ""
    status: str = "new"  # new | matched | ambiguous | rejected | duplicate
    confidence_score: Optional[float] = None
    matched_metadata_id: Optional[str] = None
    discovered_at: str = field(default_factory=now_iso)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    @classmethod
    def from_raw(cls, raw: RawRelease) -> "NormalizedRelease":
        normalized = raw.title.lower().strip()
        search_key = "".join(c for c in normalized if c.isalnum() or c.isspace()).strip()
        return cls(
            id=generate_id(),
            source_id=raw.source_id,
            source_url=raw.source_url,
            source_id_external=raw.source_id_external,
            title=raw.title,
            original_title=raw.original_title,
            year=raw.year,
            season=raw.season,
            episode=raw.episode,
            media_type=raw.media_type,
            release_date=raw.release_date,
            normalized_title=normalized,
            search_key=search_key,
            discovered_at=raw.discovered_at,
        )


# ============================================================================
# Metadata Types
# ============================================================================

@dataclass
class ExternalIdentifier:
    id: str
    release_id: str
    provider: str  # tmdb | imdb | tvdb | anidb | mal | custom
    provider_id: str
    provider_type: str  # movie | tv | season | episode
    confidence: float
    matched_at: str
    match_method: str  # exact | fuzzy | manual

    @classmethod
    def create(cls, release_id: str, provider: str, provider_id: str, provider_type: str,
               confidence: float, match_method: str) -> "ExternalIdentifier":
        return cls(
            id=generate_id(),
            release_id=release_id,
            provider=provider,
            provider_id=provider_id,
            provider_type=provider_type,
            confidence=confidence,
            matched_at=now_iso(),
            match_method=match_method,
        )


@dataclass
class MetadataSnapshot:
    id: str
    release_id: str
    provider: str
    provider_id: str
    provider_type: str
    data: Dict[str, Any]
    retrieved_at: str
    etag: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    @classmethod
    def create(cls, release_id: str, provider: str, provider_id: str, provider_type: str,
               data: Dict[str, Any], etag: Optional[str] = None) -> "MetadataSnapshot":
        return cls(
            id=generate_id(),
            release_id=release_id,
            provider=provider,
            provider_id=provider_id,
            provider_type=provider_type,
            data=data,
            retrieved_at=now_iso(),
            etag=etag,
        )


@dataclass
class MetadataMatch:
    release_id: str
    external_identifiers: List[ExternalIdentifier]
    primary_provider: str
    primary_id: str
    confidence: float
    ambiguous: bool
    alternatives: List[Dict[str, Any]] = field(default_factory=list)


# ============================================================================
# Generated Content Types
# ============================================================================

@dataclass
class GeneratedContent:
    id: str
    release_id: str
    provider: str
    model: str
    display_title: str
    seo_title: str
    overview: str
    seo_description: str
    keywords: List[str]
    tags: List[str]
    translations: Dict[str, Dict[str, str]]
    image_prompt: Optional[str] = None
    generation_params: Dict[str, Any] = field(default_factory=dict)
    generated_at: str = field(default_factory=now_iso)
    version: int = 1
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass
class GeneratedArtwork:
    id: str
    release_id: str
    provider: str
    model: str
    prompt: str
    revised_prompt: Optional[str] = None
    image_url: str = ""
    thumbnail_url: Optional[str] = None
    width: int = 0
    height: int = 0
    format: str = ""
    file_size: int = 0
    generation_params: Dict[str, Any] = field(default_factory=dict)
    usage_rights: str = "ai_generated"  # ai_generated | licensed | public_domain
    generated_at: str = field(default_factory=now_iso)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


# ============================================================================
# Job Types
# ============================================================================

@dataclass
class Job:
    id: str
    release_id: str
    status: JobStatus = JobStatus.DISCOVERED
    priority: int = 0
    assigned_runner: Optional[str] = None
    lease_expires_at: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    progress: int = 0
    current_step: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)

    @classmethod
    def create(cls, release_id: str, priority: int = 0, max_retries: int = 3) -> "Job":
        return cls(
            id=generate_id(),
            release_id=release_id,
            priority=priority,
            max_retries=max_retries,
        )

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries and self.status in (JobStatus.FAILED,)


@dataclass
class JobEvent:
    id: str
    job_id: str
    type: str  # status_change | progress | error | retry | log | checkpoint
    from_status: Optional[JobStatus] = None
    to_status: Optional[JobStatus] = None
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    level: LogLevel = LogLevel.INFO
    created_at: str = field(default_factory=now_iso)

    @classmethod
    def create(cls, job_id: str, type: str, message: str, **kwargs) -> "JobEvent":
        return cls(
            id=generate_id(),
            job_id=job_id,
            type=type,
            message=message,
            **kwargs,
        )


@dataclass
class JobCheckpoint:
    job_id: str
    step: str
    data: Dict[str, Any]
    created_at: str = field(default_factory=now_iso)


# ============================================================================
# Download & Media Types
# ============================================================================

@dataclass
class DownloadRequest:
    job_id: str
    release_id: str
    source_url: str
    source_type: str  # ytdlp | direct | stream
    expected_format: Optional[str] = None
    expected_size: Optional[int] = None
    expected_duration: Optional[float] = None
    quality_preference: Optional[str] = None
    subtitles: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MediaFile:
    id: str
    job_id: str
    release_id: str
    local_path: str
    filename: str
    file_size: int
    mime_type: str
    format: str
    duration: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    bitrate: Optional[int] = None
    codec_video: Optional[str] = None
    codec_audio: Optional[str] = None
    checksum_sha256: str = ""
    validation_status: ValidationStatus = ValidationStatus.PENDING
    validation_details: Dict[str, Any] = field(default_factory=dict)
    validated_at: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass
class ValidationResult:
    valid: bool
    checks: Dict[str, bool]
    details: Dict[str, Any]
    errors: List[str]
    warnings: List[str]


# ============================================================================
# Upload & Publish Types
# ============================================================================

@dataclass
class DestinationConfig:
    id: str
    name: str
    type: str  # youtube | vimeo | local | s3 | r2 | custom
    enabled: bool
    config: Dict[str, Any]
    auth_status: str = "unknown"
    last_tested_at: Optional[str] = None
    max_file_size: Optional[int] = None
    supported_formats: Optional[List[str]] = None
    default_privacy: str = "private"

    @classmethod
    def create(cls, name: str, type: str, config: Dict[str, Any], **kwargs) -> "DestinationConfig":
        return cls(
            id=generate_id(),
            name=name,
            type=type,
            enabled=kwargs.get("enabled", True),
            config=config,
            max_file_size=kwargs.get("max_file_size"),
            supported_formats=kwargs.get("supported_formats"),
            default_privacy=kwargs.get("default_privacy", "private"),
        )


@dataclass
class UploadRecord:
    id: str
    job_id: str
    release_id: str
    destination_id: str
    destination_type: str
    local_file_id: str
    remote_id: Optional[str] = None
    remote_url: Optional[str] = None
    status: UploadStatus = UploadStatus.PENDING
    progress: int = 0
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass
class PublishRequest:
    job_id: str
    release_id: str
    destination_ids: List[str]
    title: str
    description: str
    tags: List[str]
    thumbnail_path: Optional[str] = None
    privacy: str = "private"
    schedule_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Settings Types
# ============================================================================

@dataclass
class AppSettings:
    # Discovery
    enabled_sources: List[str] = field(default_factory=list)
    source_poll_interval: int = 60
    source_max_items: int = 50
    source_timeout: int = 30
    source_rate_limit: int = 60

    # Metadata
    tmdb_api_key: str = ""
    tmdb_language: str = "en-US"
    metadata_confidence_threshold: float = 0.8
    auto_approve_threshold: float = 0.95

    # AI Generation
    ai_text_enabled: bool = True
    ai_text_provider: str = "openai"
    ai_text_model: str = "gpt-4o-mini"
    ai_text_max_tokens: int = 2000
    ai_text_temperature: float = 0.7
    ai_image_enabled: bool = False
    ai_image_provider: str = "disabled"
    ai_image_model: str = "dall-e-3"
    ai_image_size: str = "1024x1024"
    title_template: str = "{title} ({year})"
    site_name_suffix: Optional[str] = None

    # Download
    download_enabled: bool = True
    worker_concurrency: int = 2
    job_timeout: int = 7200
    max_retries: int = 3
    retry_base_delay: int = 60
    min_disk_space_gb: int = 10
    temp_dir: str = "/tmp/media-automation"
    ffmpeg_path: Optional[str] = None
    ytdlp_path: Optional[str] = None

    # Upload
    upload_enabled: bool = True
    enabled_destinations: List[str] = field(default_factory=list)
    local_storage_path: str = "./media/uploads"
    max_upload_size: int = 10737418240  # 10GB
    require_approval: bool = True

    # General
    log_level: str = "info"
    log_format: str = "json"
    sentry_dsn: Optional[str] = None
    health_port: int = 8080

    @classmethod
    def from_env(cls, env: Dict[str, str]) -> "AppSettings":
        """Create settings from environment variables."""
        def get(key: str, default: Any = None) -> Any:
            return env.get(key, default)

        def get_bool(key: str, default: bool = False) -> bool:
            val = env.get(key, "").lower()
            return val in ("true", "1", "yes", "on") if val else default

        def get_int(key: str, default: int = 0) -> int:
            try:
                return int(env.get(key, str(default)))
            except ValueError:
                return default

        def get_float(key: str, default: float = 0.0) -> float:
            try:
                return float(env.get(key, str(default)))
            except ValueError:
                return default

        def get_list(key: str, default: List[str] = None) -> List[str]:
            val = env.get(key, "")
            return [x.strip() for x in val.split(",") if x.strip()] if val else (default or [])

        return cls(
            enabled_sources=get_list("ENABLED_SOURCES", ["tmdb", "rss"]),
            source_poll_interval=get_int("SOURCE_POLL_INTERVAL", 60),
            source_max_items=get_int("SOURCE_MAX_ITEMS", 50),
            source_timeout=get_int("SOURCE_TIMEOUT", 30),
            source_rate_limit=get_int("SOURCE_RATE_LIMIT", 60),
            tmdb_api_key=get("TMDB_API_KEY", ""),
            tmdb_language=get("TMDB_LANGUAGE", "en-US"),
            metadata_confidence_threshold=get_float("METADATA_CONFIDENCE_THRESHOLD", 0.8),
            auto_approve_threshold=get_float("AUTO_APPROVE_THRESHOLD", 0.95),
            ai_text_enabled=get_bool("ENABLE_AI_TEXT", True),
            ai_text_provider=get("AI_TEXT_PROVIDER", "openai"),
            ai_text_model=get("AI_TEXT_MODEL", "gpt-4o-mini"),
            ai_text_max_tokens=get_int("AI_TEXT_MAX_TOKENS", 2000),
            ai_text_temperature=get_float("AI_TEXT_TEMPERATURE", 0.7),
            ai_image_enabled=get_bool("ENABLE_AI_IMAGE", False),
            ai_image_provider=get("AI_IMAGE_PROVIDER", "disabled"),
            ai_image_model=get("AI_IMAGE_MODEL", "dall-e-3"),
            ai_image_size=get("AI_IMAGE_SIZE", "1024x1024"),
            title_template=get("TITLE_TEMPLATE", "{title} ({year})"),
            site_name_suffix=get("SITE_NAME_SUFFIX") or None,
            download_enabled=get_bool("ENABLE_DOWNLOAD", True),
            worker_concurrency=get_int("WORKER_CONCURRENCY", 2),
            job_timeout=get_int("JOB_TIMEOUT", 7200),
            max_retries=get_int("MAX_RETRIES", 3),
            retry_base_delay=get_int("RETRY_BASE_DELAY", 60),
            min_disk_space_gb=get_int("MIN_DISK_SPACE_GB", 10),
            temp_dir=get("TEMP_DIR", "/tmp/media-automation"),
            ffmpeg_path=get("FFMPEG_PATH") or None,
            ytdlp_path=get("YTDLP_PATH") or None,
            upload_enabled=get_bool("ENABLE_UPLOAD", True),
            enabled_destinations=get_list("ENABLED_DESTINATIONS", ["local"]),
            local_storage_path=get("LOCAL_STORAGE_PATH", "./media/uploads"),
            max_upload_size=get_int("MAX_UPLOAD_SIZE", 10737418240),
            require_approval=get_bool("REQUIRE_APPROVAL", True),
            log_level=get("LOG_LEVEL", "info"),
            log_format=get("LOG_FORMAT", "json"),
            sentry_dsn=get("SENTRY_DSN") or None,
            health_port=get_int("HEALTH_PORT", 8080),
        )


# ============================================================================
# Dashboard Types
# ============================================================================

@dataclass
class DashboardStats:
    releases: Dict[str, int]
    jobs: Dict[str, int]
    success_rate: float
    avg_processing_time: float
    recent_activity: List[Dict[str, Any]]


@dataclass
class SourceHealth:
    source_id: str
    name: str
    status: SourceStatus
    last_polled: str
    last_success: str
    error_count: int
    items_found: int
    items_new: int
    avg_response_time: float


@dataclass
class DestinationHealth:
    destination_id: str
    name: str
    status: DestinationStatus
    last_tested: str
    auth_valid: bool
    uploads_today: int
    uploads_failed: int
    avg_upload_time: float


# ============================================================================
# Log & Audit Types
# ============================================================================

@dataclass
class LogEntry:
    id: str
    timestamp: str
    level: LogLevel
    source: str
    message: str
    context: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
    user_id: Optional[str] = None

    @classmethod
    def create(cls, level: LogLevel, source: str, message: str, **kwargs) -> "LogEntry":
        return cls(
            id=generate_id(),
            timestamp=now_iso(),
            level=level,
            source=source,
            message=message,
            **kwargs,
        )


@dataclass
class AuditLog:
    id: str
    timestamp: str
    user_id: str
    action: str
    resource_type: str
    resource_id: str
    changes: Optional[Dict[str, Dict[str, Any]]] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

    @classmethod
    def create(cls, user_id: str, action: str, resource_type: str, resource_id: str,
               **kwargs) -> "AuditLog":
        return cls(
            id=generate_id(),
            timestamp=now_iso(),
            user_id=user_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            **kwargs,
        )


# ============================================================================
# API Response Types
# ============================================================================

@dataclass
class ApiResponse:
    success: bool
    data: Any = None
    error: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None

    @classmethod
    def ok(cls, data: Any = None, meta: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        return cls(success=True, data=data, meta=meta)

    @classmethod
    def err(cls, code: str, message: str, details: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        return cls(success=False, error={"code": code, "message": message, "details": details})


@dataclass
class PaginatedResponse(ApiResponse):
    data: List[Any]
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, items: List[Any], page: int, limit: int, total: int) -> "PaginatedResponse":
        return cls(
            success=True,
            data=items,
            meta={
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": page * limit < total,
            },
        )