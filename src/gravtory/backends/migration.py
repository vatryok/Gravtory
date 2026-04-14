# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Schema versioning and migration infrastructure.

For Section 03, only v1 schema exists. This module provides the foundation
for future schema migrations when tables change in later sections.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from gravtory.backends.schema import CURRENT_SCHEMA_VERSION
from gravtory.core.errors import ConfigurationError

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

_MigrationFn = Callable[["Backend"], Awaitable[None]]

logger = logging.getLogger("gravtory.migration")


class SchemaMigrator:
    """Check schema version and apply pending migrations."""

    def __init__(self, backend: Backend) -> None:
        self._backend = backend

    async def check_and_migrate(self) -> int:
        """Check schema version and apply pending migrations.

        Returns the current schema version after any migrations.

        1. Read current version from schema_version table
        2. If no version → fresh install, already created by initialize()
        3. If version < CURRENT_SCHEMA_VERSION → run migrations
        4. If version == CURRENT_SCHEMA_VERSION → no-op
        5. If version > CURRENT_SCHEMA_VERSION → error (downgrade not supported)
        """
        current = await self._get_current_version()

        if current is None:
            # Fresh install — initialize() already created tables at current version
            logger.info("Fresh install detected, schema at v%d", CURRENT_SCHEMA_VERSION)
            return CURRENT_SCHEMA_VERSION

        if current == CURRENT_SCHEMA_VERSION:
            logger.debug("Schema already at v%d, no migration needed", current)
            return current

        if current > CURRENT_SCHEMA_VERSION:
            raise ConfigurationError(
                f"Database schema version ({current}) is newer than "
                f"application schema version ({CURRENT_SCHEMA_VERSION}). "
                f"Downgrade is not supported."
            )

        # current < CURRENT_SCHEMA_VERSION — run migrations
        logger.info("Migrating schema from v%d to v%d", current, CURRENT_SCHEMA_VERSION)
        await self._run_migrations(current, CURRENT_SCHEMA_VERSION)
        return CURRENT_SCHEMA_VERSION

    async def _get_current_version(self) -> int | None:
        """Read current schema version from the database.

        Uses the backend's ``query_schema_version`` method to read
        the most recent version from the schema_version table.
        Returns None for a fresh install (no rows in table).
        """
        version = await query_schema_version(self._backend)
        return version

    async def _run_migrations(self, from_version: int, to_version: int) -> None:
        """Run migration scripts from from_version to to_version.

        Each migration is a function: _migrate_vN_to_vN+1
        Migrations run sequentially. After each successful migration,
        the new version is recorded in the schema_version table.
        """
        for v in range(from_version, to_version):
            migration_fn = _MIGRATIONS.get(v)
            if migration_fn is None:
                raise ConfigurationError(f"No migration path from v{v} to v{v + 1}")
            logger.info("Applying migration v%d → v%d", v, v + 1)
            await migration_fn(self._backend)
            await record_schema_version(self._backend, v + 1)
            logger.info("Migration v%d → v%d complete", v, v + 1)


# Migration registry: version → async migration function


async def _migrate_v1_to_v2(backend: Backend) -> None:
    """v1 → v2: Add workflow_definitions and circuit_breakers tables."""
    # SQLite
    try:
        from gravtory.backends.sqlite import SQLiteBackend

        if isinstance(backend, SQLiteBackend):
            p = backend._p
            await backend._conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS {p}workflow_definitions (
                    name            TEXT NOT NULL,
                    version         INTEGER NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (name, version)
                );
                CREATE TABLE IF NOT EXISTS {p}circuit_breakers (
                    name            TEXT PRIMARY KEY,
                    state_json      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
                );
            """)
            await backend._conn.commit()
            return
    except Exception:
        pass

    # PostgreSQL
    try:
        from gravtory.backends.postgresql import PostgreSQLBackend

        if isinstance(backend, PostgreSQLBackend):
            p = backend._prefix
            pool = backend._require_pool()
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {p}workflow_definitions (
                        name            TEXT NOT NULL,
                        version         INTEGER NOT NULL,
                        definition_json TEXT NOT NULL,
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (name, version)
                    )
                """)
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {p}circuit_breakers (
                        name            TEXT PRIMARY KEY,
                        state_json      TEXT NOT NULL,
                        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            return
    except Exception:
        pass

    # MySQL
    try:
        from gravtory.backends.mysql import MySQLBackend

        if isinstance(backend, MySQLBackend):
            p = backend._p
            await backend._execute(f"""
                CREATE TABLE IF NOT EXISTS {p}workflow_definitions (
                    name            VARCHAR(255) NOT NULL,
                    version         INT NOT NULL,
                    definition_json LONGTEXT NOT NULL,
                    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    PRIMARY KEY (name, version)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            await backend._execute(f"""
                CREATE TABLE IF NOT EXISTS {p}circuit_breakers (
                    name            VARCHAR(255) PRIMARY KEY,
                    state_json      LONGTEXT NOT NULL,
                    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            return
    except Exception:
        pass

    # InMemory / Redis / MongoDB — tables managed differently, no-op
    logger.debug("_migrate_v1_to_v2: no-op for %s", type(backend).__name__)


_MIGRATIONS: dict[int, _MigrationFn] = {
    1: _migrate_v1_to_v2,
}


# ── Schema version helpers ───────────────────────────────────────


async def query_schema_version(backend: Backend) -> int | None:
    """Read the latest schema version from the backend.

    Returns ``None`` when the table is empty (fresh install).
    Falls back to ``CURRENT_SCHEMA_VERSION`` for backends that don't
    support raw queries (InMemory, Redis, MongoDB).
    """
    # SQLite
    try:
        from gravtory.backends.sqlite import SQLiteBackend

        if isinstance(backend, SQLiteBackend):
            rows = list(
                await backend._conn.execute_fetchall(
                    f"SELECT version FROM {backend._p}schema_version ORDER BY version DESC LIMIT 1"
                )
            )
            return int(rows[0]["version"]) if rows else None
    except Exception:
        pass

    # PostgreSQL
    try:
        from gravtory.backends.postgresql import PostgreSQLBackend

        if isinstance(backend, PostgreSQLBackend):
            pool = backend._require_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT version FROM {backend._p}schema_version ORDER BY version DESC LIMIT 1"
                )
            return int(row["version"]) if row else None
    except Exception:
        pass

    # MySQL
    try:
        from gravtory.backends.mysql import MySQLBackend

        if isinstance(backend, MySQLBackend):
            row = await backend._fetchone(
                f"SELECT version FROM {backend._p}schema_version ORDER BY version DESC LIMIT 1"
            )
            return int(row["version"]) if row else None
    except Exception:
        pass

    # InMemory / Redis / MongoDB — no schema table; treat as current
    return CURRENT_SCHEMA_VERSION


async def record_schema_version(backend: Backend, version: int) -> None:
    """Write *version* into the schema_version table."""
    try:
        from gravtory.backends.sqlite import SQLiteBackend

        if isinstance(backend, SQLiteBackend):
            await backend._conn.execute(
                f"INSERT INTO {backend._p}schema_version (version) VALUES (?)",
                (version,),
            )
            await backend._conn.commit()
            return
    except Exception:
        pass

    try:
        from gravtory.backends.postgresql import PostgreSQLBackend

        if isinstance(backend, PostgreSQLBackend):
            pool = backend._require_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO {backend._p}schema_version (version) VALUES ($1)",
                    version,
                )
            return
    except Exception:
        pass

    try:
        from gravtory.backends.mysql import MySQLBackend

        if isinstance(backend, MySQLBackend):
            await backend._execute(
                f"INSERT INTO {backend._p}schema_version (version) VALUES (%s)",
                (version,),
            )
            return
    except Exception:
        pass

    # InMemory / Redis / MongoDB — no-op
    logger.debug("record_schema_version: no-op for %s", type(backend).__name__)
