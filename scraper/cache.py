"""
Response cache for the scraping stack.

Caches fetched pages (URL -> FetchResult payload) with a TTL so repeated
polls of the same page do not hammer the origin. Redis is the production
backend; an in-memory LRU keeps everything working without it (and is what
tests use).

No secrets are logged or cached: only the URL hash is used as the key, and
the value is the page payload the operator's own pipeline produced.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

from queue_workers.envelope import get_logger

logger = get_logger(__name__)

#: Default TTL for cached pages (30 minutes).
DEFAULT_TTL_S = 30 * 60


def _key_for(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"scraper:page:{digest}"


# ============================================================================
# In-memory LRU
# ============================================================================

class MemoryResponseCache:
    """Thread-safe in-memory LRU with TTL. Values are JSON-serializable."""

    def __init__(self, max_entries: int = 512, default_ttl_s: int = DEFAULT_TTL_S, time_fn=time.time) -> None:
        self._max = max(1, int(max_entries))
        self._ttl = default_ttl_s
        self._time_fn = time_fn
        self._data: "OrderedDict[str, Any]" = OrderedDict()
        self._expiry: Dict[str, float] = {}
        self._lock = threading.RLock()

    def get(self, url: str) -> Optional[Any]:
        key = _key_for(url)
        with self._lock:
            if key not in self._data:
                return None
            exp = self._expiry.get(key)
            if exp is not None and exp <= self._time_fn():
                del self._data[key]
                self._expiry.pop(key, None)
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def set(self, url: str, value: Any, ttl_s: Optional[int] = None) -> None:
        key = _key_for(url)
        ttl = self._ttl if ttl_s is None else ttl_s
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            if ttl and ttl > 0:
                self._expiry[key] = self._time_fn() + ttl
            while len(self._data) > self._max:
                old_key, _ = self._data.popitem(last=False)
                self._expiry.pop(old_key, None)

    def invalidate(self, url: str) -> bool:
        key = _key_for(url)
        with self._lock:
            existed = self._data.pop(key, None) is not None
            self._expiry.pop(key, None)
            return existed

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._expiry.clear()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"backend": "memory", "entries": len(self._data), "max_entries": self._max}


# ============================================================================
# Redis-backed
# ============================================================================

class RedisResponseCache:
    """Redis-backed cache. The client is injected (never built from a URL here)."""

    def __init__(self, client: Any, default_ttl_s: int = DEFAULT_TTL_S) -> None:
        self._client = client
        self._ttl = default_ttl_s

    def get(self, url: str) -> Optional[Any]:
        try:
            raw = self._client.get(_key_for(url))
        except Exception as exc:  # noqa: BLE001 - cache is an optimization
            logger.warning("response_cache_get_failed", error_type=type(exc).__name__)
            return None
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception:  # noqa: BLE001 - corrupt entry: treat as miss
            return None

    def set(self, url: str, value: Any, ttl_s: Optional[int] = None) -> None:
        ttl = self._ttl if ttl_s is None else ttl_s
        try:
            payload = json.dumps(value, default=str)
            if ttl and ttl > 0:
                self._client.set(_key_for(url), payload, ex=int(ttl))
            else:
                self._client.set(_key_for(url), payload)
        except Exception as exc:  # noqa: BLE001 - cache is an optimization
            logger.warning("response_cache_set_failed", error_type=type(exc).__name__)

    def invalidate(self, url: str) -> bool:
        try:
            return bool(self._client.delete(_key_for(url)))
        except Exception as exc:  # noqa: BLE001 - cache is an optimization
            logger.warning("response_cache_invalidate_failed", error_type=type(exc).__name__)
            return False

    def clear(self) -> None:
        # Deliberately a no-op: scanning/deleting by prefix on a shared Redis
        # is an operator action, not something a scraper should do silently.
        logger.info("response_cache_clear_skipped", backend="redis")

    def stats(self) -> Dict[str, Any]:
        return {"backend": "redis"}


# ============================================================================
# Factory
# ============================================================================

def get_response_cache(backend: str = "auto", refresh: bool = False) -> Any:
    """Return a process-wide response cache.

    ``backend``: ``memory`` | ``redis`` | ``auto`` (Redis when ``REDIS_URL``
    is set and reachable, otherwise memory).
    """
    global _response_cache
    with _cache_lock:
        if _response_cache is not None and not refresh:
            return _response_cache
        import os

        if backend in ("redis", "auto"):
            from queue_workers.cache import get_redis_client

            want_redis = backend == "redis" or bool(os.environ.get("REDIS_URL"))
            client = get_redis_client(ping=True) if want_redis else None
            if client is not None:
                _response_cache = RedisResponseCache(client)
                logger.info("response_cache_backend", backend="redis")
                return _response_cache
            if backend == "redis":
                logger.warning("response_cache_fallback", requested="redis", using="memory")
        _response_cache = MemoryResponseCache()
        logger.info("response_cache_backend", backend="memory")
        return _response_cache


def set_response_cache(cache: Optional[Any]) -> None:
    """Override the process-wide response cache (mainly for tests)."""
    global _response_cache
    with _cache_lock:
        _response_cache = cache


_cache_lock = threading.Lock()
_response_cache: Optional[Any] = None
