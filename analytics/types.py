"""
Shared analytics types for the media automation platform.
Used by both Python workers and Cloudflare Workers (via JSON schemas).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _uuid() -> str:
    return str(uuid.uuid4())


# ============================================================================
# Enums
# ============================================================================

class EntityType(str, Enum):
    MOVIE = "movie"
    MOVIE_EDITION = "movie_edition"
    TV_SERIES = "tv_series"
    TV_SEASON = "tv_season"
    TV_EPISODE = "tv_episode"
    ANIME_SERIES = "anime_series"
    ANIME_SEASON = "anime_season"
    ANIME_EPISODE = "anime_episode"
    SPECIAL = "special"
    OVA = "ova"
    DOCUMENTARY = "documentary"
    SHORT_FILM = "short_film"
    WEB_SERIES = "web_series"
    WEB_SEASON = "web_season"
    WEB_EPISODE = "web_episode"
    OTHER = "other"


class EventCategory(str, Enum):
    ENTITY = "entity"
    JOB = "job"
    SOURCE = "source"
    METADATA = "metadata"
    AI = "ai"
    DOWNLOAD = "download"
    UPLOAD = "upload"
    SYSTEM = "system"
    SECURITY = "security"


class ActorType(str, Enum):
    SYSTEM = "system"
    USER = "user"
    SCHEDULED = "scheduled"
    SERVICE = "service"
    WEBHOOK = "webhook"


class Outcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"
    PENDING = "pending"
    CANCELLED = "cancelled"


class TraceStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


class SpanStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ProcessingState(str, Enum):
    DISCOVERED = "discovered"
    METADATA_PENDING = "metadata_pending"
    METADATA_MATCHED = "metadata_matched"
    REVIEW_NEEDED = "review_needed"
    APPROVED = "approved"
    DOWNLOADING = "downloading"
    VALIDATING = "validating"
    PROCESSING = "processing"
    UPLOADING = "uploading"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    FAILED = "failed"


class RelationshipType(str, Enum):
    SEASON_OF = "season_of"
    EPISODE_OF = "episode_of"
    EDITION_OF = "edition_of"
    ALTERNATE_VERSION_OF = "alternate_version_of"
    SPECIAL_OF = "special_of"
    PART_OF = "part_of"
    RELATED_TO = "related_to"
    DERIVED_FROM = "derived_from"


# ============================================================================
# Canonical Media Entity
# ============================================================================

@dataclass
class MediaEntity:
    """Universal canonical media entity."""
    id: str
    entity_type: EntityType
    canonical_title: str
    normalized_title: str
    search_key: str
    original_title: Optional[str] = None
    release_year: Optional[int] = None
    release_date: Optional[str] = None
    parent_entity_id: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    episode_title: Optional[str] = None
    metadata_confidence_score: float = 0.0
    processing_state: ProcessingState = ProcessingState.DISCOVERED
    source_provenance: Dict[str, Any] = field(default_factory=dict)
    is_deleted: bool = False
    deleted_at: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    @classmethod
    def create(
        cls,
        entity_type: EntityType,
        canonical_title: str,
        original_title: Optional[str] = None,
        release_year: Optional[int] = None,
        release_date: Optional[str] = None,
        parent_entity_id: Optional[str] = None,
        season_number: Optional[int] = None,
        episode_number: Optional[int] = None,
        episode_title: Optional[str] = None,
        **kwargs,
    ) -> "MediaEntity":
        normalized = canonical_title.lower().strip()
        search_key = "".join(
            c for c in normalized if c.isalnum() or c.isspace()
        ).strip()
        if release_year:
            search_key += f" {release_year}"
        return cls(
            id=_uuid(),
            entity_type=entity_type,
            canonical_title=canonical_title,
            normalized_title=normalized,
            search_key=search_key,
            original_title=original_title,
            release_year=release_year,
            release_date=release_date,
            parent_entity_id=parent_entity_id,
            season_number=season_number,
            episode_number=episode_number,
            episode_title=episode_title,
            **kwargs,
        )


# ============================================================================
# Event Schema (shared across JS/Python)
# ============================================================================

EVENT_SCHEMA_VERSION = 1


@dataclass
class AnalyticsEvent:
    """Universal analytics event. Every component emits these."""
    id: str
    event_schema_version: int
    event_type: str                # e.g. 'entity.discovered', 'job.attempt.failed'
    event_category: EventCategory
    outcome: Outcome
    actor_type: ActorType
    timestamp: str                 # ISO 8601 UTC (when the event happened)
    received_at: str               # ISO 8601 UTC (when we stored it)

    # Correlation fields (all optional)
    entity_id: Optional[str] = None
    source_id: Optional[str] = None
    job_id: Optional[str] = None
    destination_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    correlation_id: Optional[str] = None

    actor_id: Optional[str] = None
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    context: Dict[str, Any] = field(default_factory=dict)
    processing_duration_ms: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    event_idempotency_key: Optional[str] = None

    @classmethod
    def create(
        cls,
        event_type: str,
        event_category: EventCategory,
        outcome: Outcome,
        actor_type: ActorType = ActorType.SYSTEM,
        actor_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        **kwargs,
    ) -> "AnalyticsEvent":
        ts = _now_iso()
        return cls(
            id=_uuid(),
            event_schema_version=EVENT_SCHEMA_VERSION,
            event_type=event_type,
            event_category=event_category,
            outcome=outcome,
            actor_type=actor_type,
            actor_id=actor_id,
            timestamp=ts,
            received_at=ts,
            correlation_id=correlation_id,
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        d = self.__dict__.copy()
        d["event_category"] = self.event_category.value
        d["outcome"] = self.outcome.value
        d["actor_type"] = self.actor_type.value
        return d


# ============================================================================
# Distributed Tracing (W3C Trace Context compatible)
# ============================================================================

def generate_trace_id() -> str:
    """32 hex chars (W3C trace-id), never all-zero."""
    tid = uuid.uuid4().hex
    return tid if tid != "0" * 32 else generate_trace_id()


def generate_span_id() -> str:
    """16 hex chars (W3C span-id), never all-zero."""
    sid = uuid.uuid4().hex[:16]
    return sid if sid != "0" * 16 else generate_span_id()


def parse_traceparent(traceparent: str) -> Optional[Dict[str, str]]:
    """
    Parse a W3C traceparent header.
    Format: {version}-{trace-id}-{parent-id}-{trace-flags}
    Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    Returns {'version','trace_id','span_id','flags'} or None.
    """
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, span_id, flags = parts
    if len(trace_id) != 32 or len(span_id) != 16:
        return None
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    return {
        "version": version,
        "trace_id": trace_id,
        "span_id": span_id,
        "flags": flags,
    }


def format_traceparent(trace_id: str, span_id: str, sampled: bool = True) -> str:
    flags = "01" if sampled else "00"
    return f"00-{trace_id}-{span_id}-{flags}"


@dataclass
class Span:
    """A single span within a distributed trace."""
    id: str
    trace_id: str
    span_id: str
    operation_name: str
    service_name: str
    start_time: str
    parent_span_id: Optional[str] = None
    end_time: Optional[str] = None
    duration_ms: Optional[int] = None
    status: SpanStatus = SpanStatus.OK
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    retry_number: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    links: List[Dict[str, Any]] = field(default_factory=list)
    created_at: str = field(default_factory=_now_iso)

    @classmethod
    def start(
        cls,
        trace_id: str,
        operation_name: str,
        service_name: str,
        parent_span_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> "Span":
        return cls(
            id=_uuid(),
            trace_id=trace_id,
            span_id=generate_span_id(),
            operation_name=operation_name,
            service_name=service_name,
            parent_span_id=parent_span_id,
            start_time=_now_iso(),
            attributes=attributes or {},
        )

    def finish(self, status: SpanStatus = SpanStatus.OK, error: Optional[str] = None,
               error_category: Optional[str] = None) -> None:
        self.end_time = _now_iso()
        self.status = status
        if error:
            self.error_message = error
            self.error_category = error_category or "unknown"
        # compute duration
        try:
            start = datetime.fromisoformat(self.start_time.replace("Z", "+00:00"))
            end = datetime.fromisoformat(self.end_time.replace("Z", "+00:00"))
            self.duration_ms = int((end - start).total_seconds() * 1000)
        except (ValueError, TypeError):
            self.duration_ms = None

    @property
    def traceparent(self) -> str:
        return format_traceparent(self.trace_id, self.span_id)


@dataclass
class TraceContext:
    """
    Ambient trace context, propagated across components.
    Create at workflow root, pass to child operations.
    """
    trace_id: str
    span_id: str
    root_operation: str
    root_service: str
    correlation_id: Optional[str] = None

    @classmethod
    def new(cls, root_operation: str, root_service: str,
            correlation_id: Optional[str] = None) -> "TraceContext":
        return cls(
            trace_id=generate_trace_id(),
            span_id=generate_span_id(),
            root_operation=root_operation,
            root_service=root_service,
            correlation_id=correlation_id,
        )

    @classmethod
    def from_traceparent(cls, traceparent: str, root_operation: str,
                         root_service: str,
                         correlation_id: Optional[str] = None) -> "TraceContext":
        parsed = parse_traceparent(traceparent)
        if parsed:
            return cls(
                trace_id=parsed["trace_id"],
                span_id=parsed["span_id"],
                root_operation=root_operation,
                root_service=root_service,
                correlation_id=correlation_id,
            )
        return cls.new(root_operation, root_service, correlation_id)

    @property
    def traceparent(self) -> str:
        return format_traceparent(self.trace_id, self.span_id)

    def child(self, operation_name: str, service_name: str) -> Span:
        return Span.start(
            trace_id=self.trace_id,
            operation_name=operation_name,
            service_name=service_name,
            parent_span_id=self.span_id,
        )


# ============================================================================
# Per-category analytics payload types
# ============================================================================

@dataclass
class SourceCheckResult:
    """Result payload of a single source poll/check run."""
    source_id: str
    run_type: str            # scheduled | manual | health_check | retry
    status: str              # started | completed | failed | timeout | rate_limited
    started_at: str
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    response_time_ms: Optional[int] = None
    items_discovered: int = 0
    items_new: int = 0
    items_duplicate: int = 0
    items_valid: int = 0
    items_invalid: int = 0
    metadata_matches: int = 0
    metadata_ambiguous: int = 0
    downloads_eligible: int = 0
    downloads_started: int = 0
    downloads_completed: int = 0
    downloads_failed: int = 0
    rate_limit_hits: int = 0
    timeout_count: int = 0
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    error_details: Dict[str, Any] = field(default_factory=dict)
    trace_id: Optional[str] = None


@dataclass
class JobAttemptRecord:
    """Single execution attempt of a job."""
    id: str
    job_id: str
    attempt_number: int
    status: str              # claimed | running | completed | failed | cancelled | timed_out
    runner_id: Optional[str] = None
    stage: Optional[str] = None
    progress_start: int = 0
    progress_end: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    bytes_processed: Optional[int] = None
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    error_details: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    trace_id: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)

    @classmethod
    def create(cls, job_id: str, attempt_number: int, **kwargs) -> "JobAttemptRecord":
        return cls(id=_uuid(), job_id=job_id, attempt_number=attempt_number, **kwargs)


@dataclass
class AIUsageRecord:
    """Record of a single AI provider request."""
    id: str
    provider: str
    model: str
    feature: str             # text_generation | image_generation | embedding
    task_type: str           # overview | seo_title | keywords | artwork | translation
    status: str              # success | failure | timeout | rate_limited | cancelled
    request_timestamp: str
    entity_id: Optional[str] = None
    job_id: Optional[str] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    input_characters: Optional[int] = None
    output_characters: Optional[int] = None
    image_count: int = 0
    image_dimensions: Optional[str] = None
    estimated_cost_usd: Optional[float] = None   # Calculated locally from pricing table
    reported_cost_usd: Optional[float] = None    # Actual cost reported by provider
    currency: str = "USD"
    latency_ms: Optional[int] = None
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    fallback_provider: Optional[str] = None
    fallback_model: Optional[str] = None
    cache_hit: bool = False
    response_timestamp: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)

    @classmethod
    def create(cls, provider: str, model: str, feature: str, task_type: str,
               status: str, **kwargs) -> "AIUsageRecord":
        return cls(
            id=_uuid(),
            provider=provider,
            model=model,
            feature=feature,
            task_type=task_type,
            status=status,
            request_timestamp=_now_iso(),
            **kwargs,
        )


@dataclass
class UploadAttemptRecord:
    """Record of a single upload attempt to a destination."""
    id: str
    upload_id: str
    entity_id: str
    destination_id: str
    job_id: str
    attempt_number: int
    status: str              # pending | uploading | completed | failed | cancelled | verifying
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    bytes_transferred: int = 0
    total_bytes: Optional[int] = None
    transfer_rate_mbps: Optional[float] = None
    remote_object_id: Optional[str] = None
    remote_url: Optional[str] = None
    verification_status: Optional[str] = None    # pending | verified | failed | skipped
    verification_details: Dict[str, Any] = field(default_factory=dict)
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    error_details: Dict[str, Any] = field(default_factory=dict)
    trace_id: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)

    @classmethod
    def create(cls, upload_id: str, entity_id: str, destination_id: str,
               job_id: str, attempt_number: int, **kwargs) -> "UploadAttemptRecord":
        return cls(
            id=_uuid(),
            upload_id=upload_id,
            entity_id=entity_id,
            destination_id=destination_id,
            job_id=job_id,
            attempt_number=attempt_number,
            **kwargs,
        )


@dataclass
class MetadataProvenanceRecord:
    """Field-level provenance for a metadata field."""
    id: str
    entity_id: str
    provider: str
    provider_record_id: str
    field_name: str
    retrieval_timestamp: str
    original_value: Optional[str] = None
    normalized_value: Optional[str] = None
    confidence: float = 1.0
    provider_api_version: Optional[str] = None
    validation_status: str = "unverified"  # unverified | validated | conflicted | overridden | missing
    conflict_with: Optional[str] = None
    overridden_by: Optional[str] = None    # ai | manual | other_provider
    overridden_at: Optional[str] = None
    overridden_by_user: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    @classmethod
    def create(cls, entity_id: str, provider: str, provider_record_id: str,
               field_name: str, **kwargs) -> "MetadataProvenanceRecord":
        return cls(
            id=_uuid(),
            entity_id=entity_id,
            provider=provider,
            provider_record_id=provider_record_id,
            field_name=field_name,
            retrieval_timestamp=_now_iso(),
            **kwargs,
        )


# ============================================================================
# Retention policies
# ============================================================================

DEFAULT_RETENTION_DAYS = {
    # category -> days to keep raw events
    "entity": 365,
    "job": 180,
    "source": 180,
    "metadata": 365,
    "ai": 730,         # cost records should be kept longer
    "download": 180,
    "upload": 365,
    "system": 90,
    "security": 730,   # audit/security kept longest
}


@dataclass
class RetentionPolicy:
    """Retention policy for analytics data."""
    category_days: Dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_RETENTION_DAYS)
    )
    trace_days: int = 90
    span_days: int = 30
    aggregate_days: int = 730
    health_snapshot_days: int = 30
    default_days: int = 180

    def days_for(self, category: str) -> int:
        return self.category_days.get(category, self.default_days)
