"""
Thin TTL key-value cache used by the queue layer (idempotency keys, etc.).

Backed by Redis when it is configured and reachable, otherwise by a
thread-safe in-memory store. The ``redis`` package is imported lazily so this
module stays import-safe with only the core CI dependencies installed.

No secrets are ever logged: connection URLs may carry credentials, so we only
ever log backend names and exception class names -- never the URL itself.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Optional

from .envelope import get_logger

logger = get_logger(__name__)

#: Default TTL for cached entries (24 hours).
DEFAULT_TTL_S = 24 * 3600

#: How long a Redis connect/ping may block the caller before we fall back to
#: the in-memory cache (seconds).
_REDIS_PROBE_TIMEOUT_S = 1


# ============================================================================
# Redis client factory (lazy import)
# ============================================================================

def get_redis_client(url: Optional[str] = None, ping: bool = False) -> Optional[Any]:
    """Return a ``redis.Redis`` client, or ``None`` when unavailable.

    - ``redis`` package missing -> ``None``
    - no URL configured (explicit or via ``REDIS_URL``) -> ``None``
    - ``ping=True`` and the server does not answer within the probe timeout
      -> ``None``

    The URL may contain credentials; it is never logged.
    """
    url = url or os.environ.get("REDIS_URL")
    if not url:
        return None
    try:
        import redis  # noqa: PLC0415 - lazy on purpose
    except ImportError:
        return None
    try:
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=_REDIS_PROBE_TIMEOUT_S,
            socket_timeout=_REDIS_PROBE_TIMEOUT_S,
        )
        if ping:
            client.ping()
        return client
    except Exception as exc:  # pragma: no cover - depends on live infra
        # Log only the exception class; the message may embed the URL.
        logger.warning("redis_unavailable", error_type=type(exc).__name__)
        return None


# ============================================================================
# In-memory implementation
# ============================================================================

class MemoryCache:
    """Thread-safe in-memory TTL cache.

    Values are stored as-is (the queue layer stores JSON strings). Expired
    entries are dropped lazily on access and opportunistically on write.
    """

    def __init__(self, default_ttl_s: Optional[int] = DEFAULT_TTL_S) -> None:
        self._default_ttl_s = default_ttl_s
        self._data: dict = {}
        self._expiry: dict = {}
        self._lock = threading.Lock()

    def _is_expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        return exp is not None and exp <= time.time()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if key not in self._data or self._is_expired(key):
                self._data.pop(key, None)
                self._expiry.pop(key, None)
                return None
            return self._data[key]

    def set(self, key: str, value: str, ttl_s: Optional[int] = None) -> None:
        ttl = self._default_ttl_s if ttl_s is None else ttl_s
        with self._lock:
            self._data[key] = value
            if ttl and ttl > 0:
                self._expiry[key] = time.time() + ttl
            else:
                self._expiry.pop(key, None)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)
            self._expiry.pop(key, None)

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        with self._lock:
            self._data.clear()
            self._expiry.clear()


# ============================================================================
# Redis implementation
# ============================================================================

class RedisCache:
    """TTL cache backed by a ``redis.Redis`` client (passed in, not created)."""

    def __init__(
        self,
        client: Any,
        default_ttl_s: Optional[int] = DEFAULT_TTL_S,
    ) -> None:
        self._client = client
        self._default_ttl_s = default_ttl_s

    def get(self, key: str) -> Optional[str]:
        try:
            value = self._client.get(key)
        except Exception as exc:  # pragma: no cover - depends on live infra
            logger.warning("cache_get_failed", error_type=type(exc).__name__)
            return None
        if value is None:
            return None
        # decode_responses=True gives str, but be tolerant of bytes clients.
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def set(self, key: str, value: str, ttl_s: Optional[int] = None) -> None:
        ttl = self._default_ttl_s if ttl_s is None else ttl_s
        try:
            if ttl and ttl > 0:
                self._client.set(key, value, ex=int(ttl))
            else:
                self._client.set(key, value)
        except Exception as exc:  # pragma: no cover - depends on live infra
            logger.warning("cache_set_failed", error_type=type(exc).__name__)

    def delete(self, key: str) -> None:
        try:
            self._client.delete(key)
        except Exception as exc:  # pragma: no cover - depends on live infra
            logger.warning("cache_delete_failed", error_type=type(exc).__name__)

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # pragma: no cover - depends on live infra
            pass


# ============================================================================
# Factory
# ============================================================================

_cache_lock = threading.Lock()
_cache: Optional[Any] = None


def get_cache(refresh: bool = False) -> Any:
    """Return the process-wide cache instance.

    ``CACHE_BACKEND`` selects the implementation:

    - ``memory`` -- always in-memory
    - ``redis``  -- Redis via ``REDIS_URL`` (falls back to memory with a
      warning if unavailable, so operational flows never hard-fail)
    - ``auto``   (default) -- Redis when ``REDIS_URL`` is set and reachable,
      otherwise in-memory
    """
    global _cache
    with _cache_lock:
        if _cache is None or refresh:
            _cache = _create_cache()
        return _cache


def set_cache(cache: Optional[Any]) -> None:
    """Override the process-wide cache (mainly for tests). ``None`` resets."""
    global _cache
    with _cache_lock:
        _cache = cache


def _create_cache() -> Any:
    backend = os.environ.get("CACHE_BACKEND", "auto").strip().lower()
    default_ttl: Optional[int]
    raw_ttl = os.environ.get("CACHE_TTL_S")
    try:
        default_ttl = int(raw_ttl) if raw_ttl else DEFAULT_TTL_S
    except ValueError:
        default_ttl = DEFAULT_TTL_S

    if backend == "memory":
        logger.info("cache_backend_selected", backend="memory")
        return MemoryCache(default_ttl_s=default_ttl)

    if backend == "redis":
        client = get_redis_client(ping=True)
        if client is not None:
            logger.info("cache_backend_selected", backend="redis")
            return RedisCache(client, default_ttl_s=default_ttl)
        logger.warning("cache_backend_fallback", requested="redis", using="memory")
        return MemoryCache(default_ttl_s=default_ttl)

    # auto: Redis only when configured AND reachable; otherwise memory.
    if os.environ.get("REDIS_URL"):
        client = get_redis_client(ping=True)
        if client is not None:
            logger.info("cache_backend_selected", backend="redis")
            return RedisCache(client, default_ttl_s=default_ttl)
        logger.info("cache_backend_selected", backend="memory", reason="redis_unreachable")
        return MemoryCache(default_ttl_s=default_ttl)

    logger.info("cache_backend_selected", backend="memory")
    return MemoryCache(default_ttl_s=default_ttl)
