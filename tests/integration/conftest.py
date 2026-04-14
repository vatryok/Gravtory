"""Integration test fixtures — parameterized across backend types.

All backends that are available in the environment are exercised via the
``backend`` fixture.  InMemoryBackend and SQLite are always available.
PostgreSQL, MySQL, MongoDB, and Redis require the corresponding
``*_TEST_DSN`` environment variables to be set.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.backends.sqlite import SQLiteBackend

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from gravtory.backends.base import Backend


def _available_backends() -> list[str]:
    """Build list of backend IDs available for contract testing."""
    backends = ["memory", "sqlite"]
    if os.environ.get("POSTGRES_TEST_DSN"):
        backends.append("postgresql")
    if os.environ.get("MYSQL_TEST_DSN"):
        backends.append("mysql")
    if os.environ.get("MONGO_TEST_DSN"):
        backends.append("mongodb")
    if os.environ.get("REDIS_TEST_DSN"):
        backends.append("redis")
    return backends


@pytest.fixture(params=_available_backends())
async def backend(request: pytest.FixtureRequest, tmp_path: object) -> AsyncIterator[Backend]:
    """Create a fresh backend for each test.

    InMemoryBackend is always available.  SQLite uses a temp file.
    Other backends require environment variables (CI or docker-compose).
    """
    b: Backend

    if request.param == "memory":
        b = InMemoryBackend()

    elif request.param == "sqlite":
        from pathlib import Path

        db_path = str(Path(str(tmp_path)) / "test.db")
        b = SQLiteBackend(f"sqlite:///{db_path}")

    elif request.param == "postgresql":
        from gravtory.backends.postgresql import PostgreSQLBackend

        b = PostgreSQLBackend(os.environ["POSTGRES_TEST_DSN"])

    elif request.param == "mysql":
        from gravtory.backends.mysql import MySQLBackend

        b = MySQLBackend(os.environ["MYSQL_TEST_DSN"])

    elif request.param == "mongodb":
        from gravtory.backends.mongodb import MongoDBBackend

        b = MongoDBBackend(os.environ["MONGO_TEST_DSN"])

    elif request.param == "redis":
        from gravtory.backends.redis import RedisBackend

        b = RedisBackend(os.environ["REDIS_TEST_DSN"])

    else:
        pytest.skip(f"Backend {request.param} not available")

    await b.initialize()
    yield b
    await b.close()


@pytest.fixture(autouse=True)
def _clear_module_level_mutable_state() -> None:
    """Automatically clear shared mutable state before each test.

    Several integration test modules use module-level lists/dicts to track
    side effects (e.g. compensation calls).  Forgetting to call .clear()
    at the top of a test causes flaky failures.  This fixture does it
    automatically for all known registries.
    """
    import sys

    _KNOWN_MUTABLE = {
        "tests.integration.test_crash_recovery": ["_call_log"],
        "tests.integration.test_saga_compensation": [
            "_compensation_log",
            "_compensation_outputs",
        ],
        "tests.integration.test_retry_dlq_saga": [
            "_retry_counter",
            "_compensation_log",
        ],
    }

    for module_path, attr_names in _KNOWN_MUTABLE.items():
        mod = sys.modules.get(module_path)
        if mod is None:
            continue
        for attr in attr_names:
            obj = getattr(mod, attr, None)
            if hasattr(obj, "clear"):
                obj.clear()
