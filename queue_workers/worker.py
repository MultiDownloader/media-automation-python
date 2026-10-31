"""
Celery worker entrypoint.

Run with::

    QUEUE_BACKEND=celery CELERY_BROKER_URL=redis://localhost:6379/0 \
      celery -A queue_workers.worker worker -Q default,dead_letter -c 4

Importing this module builds the process-wide :class:`CeleryExecutor`
(respecting `QUEUE_BACKEND` / `CELERY_BROKER_URL` / `CELERY_RESULT_BACKEND`)
and registers the default task handlers inside the worker process, which is
what the Celery task body needs to resolve handlers at runtime.
"""

from __future__ import annotations

import os

from .envelope import get_logger
from .executor import CeleryExecutor, get_executor, set_executor
from .tasks import register_default_handlers

logger = get_logger(__name__)


def build_worker_executor() -> CeleryExecutor:
    """Create the CeleryExecutor for this worker process and wire handlers."""
    executor = CeleryExecutor(
        queue_name=os.environ.get("CELERY_QUEUE", "default"),
        dlq_name=os.environ.get("CELERY_DLQ_QUEUE", "dead_letter"),
    )
    set_executor(executor)
    register_default_handlers(executor)
    logger.info("worker_executor_ready", queue=executor.stats().get("queue"))
    return executor


# ``celery -A queue_workers.worker`` expects an app attribute at import time.
# When Celery/Redis are not configured (e.g. CI imports this module), we skip
# app creation and expose None so importing stays safe.
app = None
try:
    app = build_worker_executor()._app
except Exception as exc:  # noqa: BLE001 - optional infra
    logger.warning("worker_app_not_created", error_type=type(exc).__name__)

__all__ = ["app", "build_worker_executor", "get_executor"]
