# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Gravtory backend abstraction layer — PostgreSQL, SQLite, MySQL, MongoDB, Redis."""

from __future__ import annotations

from typing import Any

from gravtory.backends.base import Backend
from gravtory.core.errors import ConfigurationError


def create_backend(connection_string: str, **kwargs: Any) -> Backend:
    """Auto-detect and create the appropriate backend from a connection string.

    Supported prefixes:
      - postgresql:// or postgres:// → PostgreSQLBackend
      - sqlite:/// → SQLiteBackend
      - mysql:// → MySQLBackend
      - mongodb:// → MongoDBBackend
      - redis:// → RedisBackend
    """
    if connection_string.startswith(("postgresql://", "postgres://")):
        from gravtory.backends.postgresql import PostgreSQLBackend

        return PostgreSQLBackend(connection_string, **kwargs)
    if connection_string.startswith("sqlite://"):
        from gravtory.backends.sqlite import SQLiteBackend

        return SQLiteBackend(connection_string, **kwargs)
    if connection_string.startswith("mysql://"):
        from gravtory.backends.mysql import MySQLBackend

        return MySQLBackend(connection_string, **kwargs)
    if connection_string.startswith(("mongodb://", "mongodb+srv://")):
        from gravtory.backends.mongodb import MongoDBBackend

        return MongoDBBackend(connection_string, **kwargs)
    if connection_string.startswith(("redis://", "rediss://")):
        from gravtory.backends.redis import RedisBackend

        return RedisBackend(connection_string, **kwargs)
    raise ConfigurationError(
        f"Unknown backend connection string: {connection_string!r}. "
        f"Supported: postgresql://, sqlite://, mysql://, mongodb://, redis://"
    )


__all__ = ["Backend", "create_backend"]
