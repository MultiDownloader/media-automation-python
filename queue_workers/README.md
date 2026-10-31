# Queue Workers (`python/queue_workers/`)

Queue/worker infrastructure so discovery, job processing, uploads and
analytics flushes run as retryable, observable tasks — in-process by default
and on Celery+Redis when you scale out.

## Architecture

```
enqueue(task_type, payload, idempotency_key=...)
        │
        ▼
  TaskEnvelope ──► executor ──► handler ──► TaskResult
        │          (inline | celery)            │
        │                    │                  │ success + idempotency_key
        │                    │ retries exhausted
        ▼                    ▼                  ▼
   trace_id / attempts   RetryPolicy       cache (dedupe next run)
                          (exponential
                           backoff + jitter)
                               │ terminal categories
                               ▼
                        FAILED (no retry: compliance_blocked / auth / permanent_input)
                               │ retryable, out of attempts
                               ▼
                        DEAD_LETTERED ──► DeadLetterStore (memory | Redis)
                                                │
                                                └── replay(id, executor) → same envelope id,
                                                    attempts carried forward
```

| Module | Purpose |
|---|---|
| `envelope.py` | `TaskEnvelope` / `TaskResult` / `TaskStatus`, W3C `trace_id` helpers |
| `retry.py` | `RetryPolicy` (exp backoff + full jitter), `classify_error()` taxonomy |
| `cache.py` | TTL cache (Redis or thread-safe memory) used for idempotency |
| `dlq.py` | `MemoryDeadLetterStore` / `RedisDeadLetterStore` + `replay()` |
| `executor.py` | `InlineExecutor` (default) and `CeleryExecutor` (scale-out) |
| `tasks.py` | `enqueue()`, `register_default_handlers()`, default task types |

## Default task types

| Task type | Handler | Wraps |
|---|---|---|
| `discovery.poll` | `discovery.engine.run_discovery_once()` | one discovery sweep |
| `job.process` | `processor.Processor.process_next_job()` | one queued job |
| `upload.perform` | `uploaders.base.create_uploader(...).upload(...)` | one file upload |
| `analytics.flush` | `analytics.collector.get_collector().flush()` | buffered events |

## Retry & dead-letter semantics

- `RetryPolicy`: `max_attempts` (default 3), exponential backoff with full
  jitter (`base_delay_s` → `max_delay_s`), retrying only
  `transient_network`, `rate_limited`, `storage`, `unknown`.
- **Never retried** (terminal, `FAILED`, not dead-lettered):
  `compliance_blocked` (the user's policy said no), `auth` (needs a human),
  `permanent_input` (can never succeed).
- Retryable + out of attempts → `DEAD_LETTERED`, stored with its envelope so
  `replay()` can re-submit it. Replay preserves the envelope **id** and
  carries the **attempt count** forward — history and tracing stay continuous.
  Terminal entries refuse replay.
- Analytics and job state are never corrupted by queue failures: task results
  are recorded before any cache/idempotency bookkeeping, and handlers wrap the
  existing modules defensively (unknown signatures degrade to classified
  errors, not crashes).

## Idempotency

```python
from queue_workers import enqueue

result = enqueue(
    "job.process",
    {"runner_id": "worker-1"},
    idempotency_key="job-12345",   # duplicates return the first result
)
```

Completed results are cached for 24 h under `queue:idem:<task_type>:<key>`;
duplicate scheduler ticks or double-clicks do not re-execute the work.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `QUEUE_BACKEND` | `inline` | `celery` to use CeleryExecutor (falls back to inline if unavailable) |
| `CELERY_BROKER_URL` / `REDIS_URL` | unset | broker for Celery |
| `CELERY_RESULT_BACKEND` | broker URL | result backend |
| `DLQ_BACKEND` | `auto` | `memory` \| `redis` \| `auto` |
| `CACHE_BACKEND` | `auto` | idempotency cache backend |
| `CACHE_TTL_S` | `86400` | idempotency TTL |

No secrets are read from anywhere but the environment, and broker URLs (which
may carry credentials) are never logged.

## Running workers

Local in-process (default — nothing to start):

```python
from queue_workers import get_executor, register_default_handlers, enqueue
register_default_handlers(get_executor())
```

Celery over Redis (see `docker-compose.queue.yml` at the repo root):

```bash
docker compose -f docker-compose.queue.yml up -d redis
docker compose -f docker-compose.queue.yml up worker
# or directly:
QUEUE_BACKEND=celery CELERY_BROKER_URL=redis://localhost:6379/0 \
  celery -A queue_workers.worker worker -Q default,dead_letter -c 4
```

## Tests

```bash
cd python
python -m pytest tests/test_queue_*.py -v
```

Envelope round-trips, backoff bounds, retry matrix, executor success/retry/
dead-letter paths, DLQ add/list/get/replay/purge, terminal-category replay
refusal. Celery/Redis tests are guarded by `importorskip` and never need a
running broker.
