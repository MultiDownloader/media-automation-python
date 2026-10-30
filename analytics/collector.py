"""
Analytics event collector with buffered delivery, idempotency, and
graceful degradation (operational state must never depend on analytics).
"""

from __future__ import annotations

import asyncio
import json
import structlog
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..shared.config import get_settings
from ..shared.database import Database, get_database
from .types import (
    AIUsageRecord,
    AnalyticsEvent,
    EventCategory,
    JobAttemptRecord,
    MediaEntity,
    MetadataProvenanceRecord,
    Outcome,
    ActorType,
    Span,
    SourceCheckResult,
    TraceContext,
    TraceStatus,
    UploadAttemptRecord,
    _now_iso,
    _uuid,
)

logger = structlog.get_logger(__name__)

MAX_BUFFER = 500          # bounded buffering
FLUSH_INTERVAL_MS = 5000  # 5 seconds
MAX_BATCH = 50            # D1-friendly batch size

# Analytics must NEVER break operational flows.
# Every public method swallows-and-logs its own failures unless told otherwise.


@dataclass
class _PendingEvent:
    kind: str  # event | span | trace | job_attempt | source_check | ai_usage | upload_attempt | provenance
    payload: Dict[str, Any]


class EventCollector:
    """
    Collects analytics records from Python workers, buffers them in memory,
    and flushes them to storage in bounded batches.

    Delivery guarantees:
    - At-least-once with idempotency keys for events
    - Failures are recorded in a local outbox table (dead-letter) for replay
    - Flush failures never raise into the calling workflow
    """

    def __init__(self, db: Optional[Database] = None, service_name: str = "python-worker"):
        self._db = db
        self._service_name = service_name
        self._buffer: List[_PendingEvent] = []
        self._failed: List[_PendingEvent] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._closed = False
        self.stats = {"collected": 0, "flushed": 0, "failed": 0, "dropped": 0}

    async def _get_db(self) -> Database:
        if self._db is None:
            self._db = get_database()
            if not self._db._initialized:
                await self._db.connect()
        return self._db

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def close(self) -> None:
        self._closed = True
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
        await self.flush()

    async def _flush_loop(self) -> None:
        interval = FLUSH_INTERVAL_MS / 1000
        while not self._closed:
            try:
                await asyncio.sleep(interval)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # never die
                logger.warning("analytics_flush_loop_error", error=str(e))

    # ------------------------------------------------------------------
    # Buffering
    # ------------------------------------------------------------------

    async def _add(self, kind: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            if len(self._buffer) >= MAX_BUFFER:
                # bounded: drop oldest non-critical entry
                self._buffer.pop(0)
                self.stats["dropped"] += 1
                logger.warning("analytics_buffer_overflow", dropped=self.stats["dropped"])
            self._buffer.append(_PendingEvent(kind=kind, payload=payload))
            self.stats["collected"] += 1
        # opportunistic flush when buffer crosses a batch size
        if len(self._buffer) >= MAX_BATCH:
            await self.flush()

    # ------------------------------------------------------------------
    # Public: Events
    # ------------------------------------------------------------------

    async def emit(
        self,
        event_type: str,
        category: EventCategory,
        outcome: Outcome,
        actor_type: ActorType = ActorType.SYSTEM,
        actor_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        entity_id: Optional[str] = None,
        source_id: Optional[str] = None,
        job_id: Optional[str] = None,
        destination_id: Optional[str] = None,
        error_category: Optional[str] = None,
        error_message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[int] = None,
        tags: Optional[List[str]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[str]:
        try:
            event = AnalyticsEvent.create(
                event_type=event_type,
                event_category=category,
                outcome=outcome,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                trace_id=trace_id,
                span_id=span_id,
                entity_id=entity_id,
                source_id=source_id,
                job_id=job_id,
                destination_id=destination_id,
                error_category=error_category,
                error_message=error_message,
                context=context or {},
                processing_duration_ms=duration_ms,
                tags=tags or [],
                event_idempotency_key=idempotency_key,
            )
            await self._add("event", event.to_dict())
            return event.id
        except Exception as e:
            logger.warning("analytics_emit_failed", event_type=event_type, error=str(e))
            return None

    # ------------------------------------------------------------------
    # Public: Tracing
    # ------------------------------------------------------------------

    async def start_trace(self, root_operation: str, root_service: Optional[str] = None,
                          correlation_id: Optional[str] = None) -> TraceContext:
        ctx = TraceContext.new(
            root_operation=root_operation,
            root_service=root_service or self._service_name,
            correlation_id=correlation_id,
        )
        await self._add("trace", {
            "trace_id": ctx.trace_id,
            "root_operation": ctx.root_operation,
            "root_service": ctx.root_service,
            "status": TraceStatus.ACTIVE.value,
            "start_time": _now_iso(),
            "metadata": {"correlation_id": ctx.correlation_id or ""},
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        })
        return ctx

    async def start_span(
        self,
        trace_id: str,
        operation_name: str,
        service_name: str,
        parent_span_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Span:
        span = Span.start(
            trace_id=trace_id,
            operation_name=operation_name,
            service_name=service_name,
            parent_span_id=parent_span_id,
            attributes=self._sanitize(attributes or {}),
        )
        await self._add("span", self._span_payload(span, "start"))
        return span

    async def finish_span(
        self,
        span: Span,
        status=None,
        error: Optional[str] = None,
        error_category: Optional[str] = None,
        retry_number: int = 0,
    ) -> None:
        from .types import SpanStatus
        span.finish(status=status or SpanStatus.OK, error=error, error_category=error_category)
        span.retry_number = retry_number
        await self._add("span", self._span_payload(span, "finish"))
        if error:
            await self.emit(
                event_type="span.error",
                category=EventCategory.SYSTEM,
                outcome=Outcome.FAILURE,
                trace_id=span.trace_id,
                span_id=span.span_id,
                error_category=error_category,
                error_message=error,
                context={"operation": span.operation_name, "service": span.service_name},
            )

    async def finish_trace(self, trace_id: str, status: TraceStatus,
                           duration_ms: Optional[int] = None) -> None:
        await self._add("trace_update", {
            "trace_id": trace_id,
            "status": status.value,
            "end_time": _now_iso(),
            "duration_ms": duration_ms,
        })

    def _span_payload(self, span: Span, phase: str) -> Dict[str, Any]:
        return {
            "phase": phase,
            "id": span.id,
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "parent_span_id": span.parent_span_id,
            "operation_name": span.operation_name,
            "service_name": span.service_name,
            "start_time": span.start_time,
            "end_time": span.end_time,
            "duration_ms": span.duration_ms,
            "status": span.status.value if hasattr(span.status, "value") else span.status,
            "error_category": span.error_category,
            "error_message": span.error_message,
            "retry_number": span.retry_number,
            "attributes": span.attributes,
            "created_at": span.created_at,
        }

    # ------------------------------------------------------------------
    # Public: Domain record helpers
    # ------------------------------------------------------------------

    async def record_job_attempt(self, attempt: JobAttemptRecord) -> None:
        await self._add("job_attempt", attempt.__dict__)

    async def record_source_check(self, check: SourceCheckResult) -> None:
        payload = dict(check.__dict__)
        payload.setdefault("id", _uuid())
        payload.setdefault("created_at", _now_iso())
        await self._add("source_check", payload)

    async def record_ai_usage(self, usage: AIUsageRecord) -> None:
        await self._add("ai_usage", usage.__dict__)

    async def record_upload_attempt(self, attempt: UploadAttemptRecord) -> None:
        await self._add("upload_attempt", attempt.__dict__)

    async def record_provenance(self, record: MetadataProvenanceRecord) -> None:
        await self._add("provenance", record.__dict__)

    async def record_health_snapshot(self, snapshot_type: str, component: str,
                                     status: str, metric_name: Optional[str] = None,
                                     metric_value: Optional[float] = None,
                                     metric_unit: Optional[str] = None,
                                     details: Optional[Dict[str, Any]] = None) -> None:
        await self._add("health", {
            "id": _uuid(),
            "snapshot_type": snapshot_type,
            "component_name": component,
            "status": status,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "metric_unit": metric_unit,
            "details": self._sanitize(details or {}),
            "recorded_at": _now_iso(),
        })

    # ------------------------------------------------------------------
    # Flush / persistence
    # ------------------------------------------------------------------

    async def flush(self) -> int:
        async with self._lock:
            if not self._buffer:
                return 0
            batch = self._buffer[:MAX_BATCH]
            self._buffer = self._buffer[len(batch):]

        try:
            await self._persist(batch)
            self.stats["flushed"] += len(batch)
            return len(batch)
        except Exception as e:
            # dead-letter: keep for retry, but never raise
            logger.warning("analytics_flush_failed", count=len(batch), error=str(e))
            async with self._lock:
                self._failed.extend(batch)
                self.stats["failed"] += len(batch)
            await self._persist_outbox(batch)
            return 0

    async def _persist(self, batch: List[_PendingEvent]) -> None:
        db = await self._get_db()

        events = [p.payload for p in batch if p.kind == "event"]
        spans = [p.payload for p in batch if p.kind == "span"]
        traces = [p.payload for p in batch if p.kind == "trace"]
        trace_updates = [p.payload for p in batch if p.kind == "trace_update"]
        job_attempts = [p.payload for p in batch if p.kind == "job_attempt"]
        source_checks = [p.payload for p in batch if p.kind == "source_check"]
        ai_usages = [p.payload for p in batch if p.kind == "ai_usage"]
        upload_attempts = [p.payload for p in batch if p.kind == "upload_attempt"]
        provenances = [p.payload for p in batch if p.kind == "provenance"]
        healths = [p.payload for p in batch if p.kind == "health"]

        if events:
            await db.executemany(
                """INSERT OR IGNORE INTO analytics_events
                (id, event_idempotency_key, event_schema_version, event_type, event_category,
                 entity_id, source_id, job_id, destination_id, trace_id, span_id, parent_span_id,
                 correlation_id, actor_type, actor_id, outcome, error_category, error_message,
                 error_details, context, timestamp, received_at, processing_duration_ms, tags)
                VALUES (:id, :event_idempotency_key, :event_schema_version, :event_type, :event_category,
                 :entity_id, :source_id, :job_id, :destination_id, :trace_id, :span_id, :parent_span_id,
                 :correlation_id, :actor_type, :actor_id, :outcome, :error_category, :error_message,
                 :error_details, :context, :timestamp, :received_at, :processing_duration_ms, :tags)""",
                [self._json_fields(e) for e in events],
            )

        if spans:
            await db.executemany(
                """INSERT INTO trace_spans
                (id, trace_id, span_id, parent_span_id, operation_name, service_name,
                 start_time, end_time, duration_ms, status, error_category, error_message,
                 retry_number, attributes, events, links, created_at)
                VALUES (:id, :trace_id, :span_id, :parent_span_id, :operation_name, :service_name,
                 :start_time, :end_time, :duration_ms, :status, :error_category, :error_message,
                 :retry_number, :attributes, :events, :links, :created_at)""",
                [self._json_fields(s, fields=("attributes", "events", "links")) for s in spans],
            )

        if traces:
            await db.executemany(
                """INSERT OR IGNORE INTO traces
                (trace_id, root_operation, root_service, status, start_time, metadata, created_at, updated_at)
                VALUES (:trace_id, :root_operation, :root_service, :status, :start_time, :metadata, :created_at, :updated_at)""",
                [self._json_fields(t, fields=("metadata",)) for t in traces],
            )

        for tu in trace_updates:
            await db.execute(
                """UPDATE traces SET status = ?, end_time = COALESCE(?, end_time),
                   duration_ms = COALESCE(?, duration_ms), updated_at = datetime('now')
                   WHERE trace_id = ?""",
                (tu.get("status"), tu.get("end_time"), tu.get("duration_ms"), tu.get("trace_id")),
            )

        if job_attempts:
            await db.executemany(
                """INSERT INTO job_attempts
                (id, job_id, attempt_number, runner_id, status, stage, progress_start, progress_end,
                 started_at, completed_at, duration_ms, bytes_processed, resource_usage,
                 error_category, error_message, error_details, metadata, trace_id, created_at)
                VALUES (:id, :job_id, :attempt_number, :runner_id, :status, :stage, :progress_start,
                 :progress_end, :started_at, :completed_at, :duration_ms, :bytes_processed,
                 :resource_usage, :error_category, :error_message, :error_details, :metadata,
                 :trace_id, :created_at)""",
                [self._json_fields(a, fields=("resource_usage", "error_details", "metadata"))
                 for a in job_attempts],
            )

        if source_checks:
            await db.executemany(
                """INSERT INTO source_check_runs
                (id, source_id, run_type, status, items_discovered, items_new, items_duplicate,
                 items_valid, items_invalid, metadata_matches, metadata_ambiguous,
                 downloads_eligible, downloads_started, downloads_completed, downloads_failed,
                 duration_ms, response_time_ms, rate_limit_hits, timeout_count,
                 error_category, error_message, error_details, trace_id, started_at, completed_at, created_at)
                VALUES (:id, :source_id, :run_type, :status, :items_discovered, :items_new,
                 :items_duplicate, :items_valid, :items_invalid, :metadata_matches,
                 :metadata_ambiguous, :downloads_eligible, :downloads_started,
                 :downloads_completed, :downloads_failed, :duration_ms, :response_time_ms,
                 :rate_limit_hits, :timeout_count, :error_category, :error_message,
                 :error_details, :trace_id, :started_at, :completed_at, :created_at)""",
                [self._json_fields(s, fields=("error_details",)) for s in source_checks],
            )

        if ai_usages:
            await db.executemany(
                """INSERT INTO ai_usage_records
                (id, provider, model, feature, task_type, entity_id, job_id, trace_id, request_id,
                 prompt_tokens, completion_tokens, total_tokens, input_characters, output_characters,
                 image_count, image_dimensions, estimated_cost_usd, reported_cost_usd, currency,
                 latency_ms, status, error_category, error_message, fallback_provider, fallback_model,
                 cache_hit, request_timestamp, response_timestamp, created_at)
                VALUES (:id, :provider, :model, :feature, :task_type, :entity_id, :job_id, :trace_id,
                 :request_id, :prompt_tokens, :completion_tokens, :total_tokens, :input_characters,
                 :output_characters, :image_count, :image_dimensions, :estimated_cost_usd,
                 :reported_cost_usd, :currency, :latency_ms, :status, :error_category,
                 :error_message, :fallback_provider, :fallback_model, :cache_hit,
                 :request_timestamp, :response_timestamp, :created_at)""",
                [self._json_fields(u) for u in ai_usages],
            )

        if upload_attempts:
            await db.executemany(
                """INSERT INTO upload_attempts
                (id, upload_id, entity_id, destination_id, job_id, attempt_number, status,
                 bytes_transferred, total_bytes, transfer_rate_mbps, started_at, completed_at,
                 duration_ms, remote_object_id, remote_url, verification_status, verification_details,
                 error_category, error_message, error_details, trace_id, created_at)
                VALUES (:id, :upload_id, :entity_id, :destination_id, :job_id, :attempt_number,
                 :status, :bytes_transferred, :total_bytes, :transfer_rate_mbps, :started_at,
                 :completed_at, :duration_ms, :remote_object_id, :remote_url, :verification_status,
                 :verification_details, :error_category, :error_message, :error_details,
                 :trace_id, :created_at)""",
                [self._json_fields(u, fields=("verification_details", "error_details"))
                 for u in upload_attempts],
            )

        if provenances:
            await db.executemany(
                """INSERT INTO metadata_provenance
                (id, entity_id, provider, provider_record_id, field_name, original_value,
                 normalized_value, confidence, retrieval_timestamp, provider_api_version,
                 validation_status, conflict_with, overridden_by, overridden_at, overridden_by_user,
                 created_at, updated_at)
                VALUES (:id, :entity_id, :provider, :provider_record_id, :field_name,
                 :original_value, :normalized_value, :confidence, :retrieval_timestamp,
                 :provider_api_version, :validation_status, :conflict_with, :overridden_by,
                 :overridden_at, :overridden_by_user, :created_at, :updated_at)""",
                [self._json_fields(p) for p in provenances],
            )

        if healths:
            await db.executemany(
                """INSERT INTO system_health_snapshots
                (id, snapshot_type, component_name, status, metric_name, metric_value, metric_unit,
                 details, recorded_at, created_at)
                VALUES (:id, :snapshot_type, :component_name, :status, :metric_name, :metric_value,
                 :metric_unit, :details, :recorded_at, :created_at)""",
                [self._json_fields(h, fields=("details",)) for h in healths],
            )

    async def _persist_outbox(self, batch: List[_PendingEvent]) -> None:
        """Best-effort dead-letter write; ignore failures here too."""
        try:
            db = await self._get_db()
            await db.executemany(
                """INSERT INTO analytics_outbox (id, kind, payload, attempts, created_at)
                 VALUES (:id, :kind, :payload, 0, :created_at)""",
                [{
                    "id": _uuid(),
                    "kind": p.kind,
                    "payload": json.dumps(p.payload, default=str),
                    "created_at": _now_iso(),
                } for p in batch[:100]],  # bounded outbox writes
            )
        except Exception as e:
            logger.warning("analytics_outbox_write_failed", error=str(e))

    # ------------------------------------------------------------------
    # Sanitization & helpers
    # ------------------------------------------------------------------

    _SECRET_HINTS = (
        "token", "secret", "password", "passwd", "apikey", "api_key",
        "authorization", "cookie", "credential", "private_key", "signed_url",
    )

    def _sanitize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Strip secrets from context before persistence."""
        if not isinstance(data, dict):
            return {}
        clean: Dict[str, Any] = {}
        for k, v in data.items():
            lk = str(k).lower()
            if any(h in lk for h in self._SECRET_HINTS):
                clean[k] = "[REDACTED]"
            elif isinstance(v, dict):
                clean[k] = self._sanitize(v)
            elif isinstance(v, str) and len(v) > 4000:
                clean[k] = v[:4000] + "...[truncated]"
            else:
                clean[k] = v
        return clean

    def _json_fields(self, payload: Dict[str, Any],
                     fields: tuple = ("context", "error_details", "tags")) -> Dict[str, Any]:
        out = dict(payload)
        for f in fields:
            if f in out and not isinstance(out[f], str):
                out[f] = json.dumps(out[f] if out[f] is not None else {}, default=str)
        if out.get("context") is None:
            out["context"] = "{}"
        if out.get("tags") is None:
            out["tags"] = "[]"
        if out.get("error_details") is None:
            out["error_details"] = "{}"
        return out


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_collector: Optional[EventCollector] = None


def get_collector(service_name: str = "python-worker") -> EventCollector:
    global _collector
    if _collector is None:
        _collector = EventCollector(service_name=service_name)
    return _collector


async def emit(event_type: str, category: EventCategory, outcome: Outcome, **kwargs) -> Optional[str]:
    """Convenience: emit an event through the singleton collector."""
    return await get_collector().emit(event_type, category, outcome, **kwargs)
