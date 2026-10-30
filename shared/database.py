"""
Database layer for Python workers.
Uses aiosqlite for async SQLite access (local development)
Can be adapted for Cloudflare D1 via HTTP API in production.
"""

from __future__ import annotations

import asyncio
import json
import structlog
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import aiosqlite

from .config import get_settings

logger = structlog.get_logger(__name__)


class Database:
    """Async database wrapper for SQLite/D1."""

    def __init__(self, db_path: str = None):
        self.settings = get_settings()
        self.db_path = db_path or self.settings.temp_dir + "/media_automation.db"
        self._pool: Optional[aiosqlite.Connection] = None
        self._initialized = False

    async def connect(self) -> None:
        """Initialize database connection."""
        if self._initialized:
            return

        # Ensure directory exists
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        self._pool = await aiosqlite.connect(self.db_path)
        self._pool.row_factory = aiosqlite.Row
        await self._pool.execute("PRAGMA foreign_keys = ON")
        await self._pool.execute("PRAGMA journal_mode = WAL")
        await self._pool.commit()

        await self._init_schema()
        self._initialized = True
        logger.info("database_connected", path=self.db_path)

    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._initialized = False
            logger.info("database_closed")

    async def _init_schema(self) -> None:
        """Initialize database schema (subset of D1 schema)."""
        schema = """
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            config TEXT NOT NULL DEFAULT '{}',
            poll_interval_minutes INTEGER NOT NULL DEFAULT 60,
            max_items_per_poll INTEGER NOT NULL DEFAULT 50,
            rate_limit_rpm INTEGER NOT NULL DEFAULT 60,
            timeout_seconds INTEGER NOT NULL DEFAULT 30,
            health_check_url TEXT,
            last_polled_at TEXT,
            last_error TEXT,
            error_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS releases (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_id_external TEXT NOT NULL,
            title TEXT NOT NULL,
            original_title TEXT,
            year INTEGER,
            season INTEGER,
            episode INTEGER,
            media_type TEXT NOT NULL,
            release_date TEXT,
            normalized_title TEXT NOT NULL,
            search_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            confidence_score REAL,
            matched_metadata_id TEXT,
            discovered_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_releases_source_external 
        ON releases(source_id, source_id_external);
        CREATE INDEX IF NOT EXISTS idx_releases_search_key ON releases(search_key);
        CREATE INDEX IF NOT EXISTS idx_releases_status ON releases(status);

        CREATE TABLE IF NOT EXISTS external_identifiers (
            id TEXT PRIMARY KEY,
            release_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            provider_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            matched_at TEXT NOT NULL,
            match_method TEXT NOT NULL,
            FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_ext_id_release_provider 
        ON external_identifiers(release_id, provider);

        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            release_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'DISCOVERED',
            priority INTEGER NOT NULL DEFAULT 0,
            assigned_runner TEXT,
            lease_expires_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            error_message TEXT,
            error_code TEXT,
            progress INTEGER NOT NULL DEFAULT 0,
            current_step TEXT,
            started_at TEXT,
            completed_at TEXT,
            metadata TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_runner ON jobs(assigned_runner);
        """

        await self._pool.executescript(schema)
        await self._pool.commit()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """Context manager for database transactions."""
        if not self._pool:
            raise RuntimeError("Database not connected")

        async with self._pool.execute("BEGIN") as cursor:
            try:
                yield self._pool
                await self._pool.commit()
            except Exception:
                await self._pool.rollback()
                raise

    async def execute(self, query: str, params: Tuple = ()) -> aiosqlite.Cursor:
        """Execute a query."""
        if not self._pool:
            raise RuntimeError("Database not connected")
        return await self._pool.execute(query, params)

    async def executemany(self, query: str, params_list: List[Tuple]) -> None:
        """Execute many queries."""
        if not self._pool:
            raise RuntimeError("Database not connected")
        await self._pool.executemany(query, params_list)
        await self._pool.commit()

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
        """Fetch single row."""
        cursor = await self.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        """Fetch all rows."""
        cursor = await self.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def fetchval(self, query: str, params: Tuple = ()) -> Any:
        """Fetch single value."""
        cursor = await self.execute(query, params)
        row = await cursor.fetchone()
        return row[0] if row else None


# Global database instance
_db_instance: Optional[Database] = None


def get_database() -> Database:
    """Get or create global database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance


async def init_database(db_path: str = None) -> Database:
    """Initialize and return database instance."""
    db = Database(db_path)
    await db.connect()
    return db


class D1Database:
    """Cloudflare D1 HTTP API client for production use."""

    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}"
        self._client = None

    async def _get_client(self):
        import httpx
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {self.api_token}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def query(self, sql: str, params: List[Any] = None) -> Dict[str, Any]:
        """Execute a query via D1 HTTP API."""
        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/query",
            json={"sql": sql, "params": params or []},
        )
        response.raise_for_status()
        return response.json()

    async def execute(self, sql: str, params: List[Any] = None) -> Dict[str, Any]:
        """Execute a statement via D1 HTTP API."""
        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/query",
            json={"sql": sql, "params": params or []},
        )
        response.raise_for_status()
        return response.json()

    async def batch(self, statements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute batch statements via D1 HTTP API."""
        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/batch",
            json={"statements": statements},
        )
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None