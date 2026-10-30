"""
Shared configuration module for Python workers.
Loads settings from environment variables with validation.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

from .types import AppSettings


class Config:
    """Singleton configuration holder."""

    _instance: Optional["Config"] = None
    _settings: Optional[AppSettings] = None

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def settings(self) -> AppSettings:
        if self._settings is None:
            self._settings = AppSettings.from_env(dict(os.environ))
        return self._settings

    def reload(self) -> AppSettings:
        """Reload settings from environment."""
        self._settings = AppSettings.from_env(dict(os.environ))
        return self._settings


def get_config() -> Config:
    """Get the global config instance."""
    return Config()


def get_settings() -> AppSettings:
    """Get the current settings."""
    return get_config().settings


# ============================================================================
# Validation Helpers
# ============================================================================

def validate_required_env(keys: list[str]) -> Dict[str, str]:
    """Validate that required environment variables are set."""
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return {k: os.environ[k] for k in keys}


def get_env(key: str, default: Any = None, required: bool = False) -> Any:
    """Get environment variable with type conversion."""
    value = os.environ.get(key)
    if value is None:
        if required:
            raise ValueError(f"Required environment variable not set: {key}")
        return default

    # Try to convert to appropriate type
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    if value.isdigit():
        return int(value)
    try:
        return float(value)
    except ValueError:
        return value


def ensure_dir(path: str) -> Path:
    """Ensure directory exists, create if needed."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# ============================================================================
# Logging Configuration
# ============================================================================

import logging
import json
import sys
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """JSON log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "context"):
            log_obj["context"] = record.context
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def setup_logging(level: str = "info", format: str = "json") -> None:
    """Configure application logging."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    if format == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers = [handler]

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)


# ============================================================================
# HTTP Client Configuration
# ============================================================================

import httpx


def create_http_client(
    timeout: float = 30.0,
    max_retries: int = 3,
    base_url: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> httpx.AsyncClient:
    """Create a configured HTTP client with retries."""
    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    timeout_config = httpx.Timeout(timeout, connect=10.0)

    client = httpx.AsyncClient(
        base_url=base_url,
        headers=headers or {},
        timeout=timeout_config,
        limits=limits,
        follow_redirects=True,
    )

    return client


# ============================================================================
# File Utilities
# ============================================================================

import hashlib
import aiofiles


async def compute_sha256(filepath: Path, chunk_size: int = 8192) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    async with aiofiles.open(filepath, "rb") as f:
        while chunk := await f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"


# ============================================================================
# Retry Utilities
# ============================================================================

import asyncio
import random
from functools import wraps
from typing import Callable, TypeVar, Tuple, Type

T = TypeVar("T")


async def retry_with_backoff(
    func: Callable[..., T],
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs,
) -> T:
    """Execute function with exponential backoff retry."""
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retry_exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (exponential_base ** attempt), max_delay)
                if jitter:
                    delay *= (0.5 + random.random())
                await asyncio.sleep(delay)
            else:
                raise

    raise last_exception


def retryable(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retry_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """Decorator for retryable async functions."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            return await retry_with_backoff(
                func,
                *args,
                max_retries=max_retries,
                base_delay=base_delay,
                max_delay=max_delay,
                exponential_base=exponential_base,
                retry_exceptions=retry_exceptions,
                **kwargs,
            )

        return wrapper

    return decorator


# ============================================================================
# Health Check Utilities
# ============================================================================

import psutil


def check_disk_space(path: str, min_gb: float) -> bool:
    """Check if disk has minimum free space."""
    try:
        usage = psutil.disk_usage(path)
        free_gb = usage.free / (1024 ** 3)
        return free_gb >= min_gb
    except Exception:
        return False


def get_system_info() -> Dict[str, Any]:
    """Get system information for health checks."""
    return {
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_usage": {
            "total_gb": psutil.disk_usage("/").total / (1024 ** 3),
            "free_gb": psutil.disk_usage("/").free / (1024 ** 3),
        },
        "load_average": psutil.getloadavg() if hasattr(psutil, "getloadavg") else None,
    }