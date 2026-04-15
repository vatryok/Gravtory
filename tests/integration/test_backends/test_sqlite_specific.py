"""SQLite-specific integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from gravtory.backends.sqlite import SQLiteBackend
from gravtory.core.types import WorkflowRun, WorkflowStatus

pytestmark = pytest.mark.integration


class TestFilePersistence:
    @pytest.mark.asyncio
    async def test_survives_reconnect(self, tmp_path: Path) -> None:
        """Data persists across close/reopen (plan 3.7 requirement)."""
        db_path = str(tmp_path / "persist.db")
        dsn = f"sqlite:///{db_path}"

        b1 = SQLiteBackend(dsn)
        await b1.initialize()
        await b1.create_workflow_run(WorkflowRun(id="persist-1", workflow_name="WF"))
        await b1.update_workflow_status("persist-1", WorkflowStatus.COMPLETED)
        await b1.close()

        b2 = SQLiteBackend(dsn)
        await b2.initialize()
        run = await b2.get_workflow_run("persist-1")
        assert run is not None
        assert run.id == "persist-1"
        assert run.status == WorkflowStatus.COMPLETED
        await b2.close()

    @pytest.mark.asyncio
    async def test_in_memory_mode(self) -> None:
        b = SQLiteBackend("sqlite://:memory:")
        await b.initialize()
        assert await b.health_check()
        await b.close()


class TestCreateBackendAutodetect:
    def test_sqlite_autodetect(self, tmp_path: Path) -> None:
        from gravtory.backends import create_backend

        b = create_backend(f"sqlite:///{tmp_path / 'auto.db'}")
        assert isinstance(b, SQLiteBackend)

    def test_pg_autodetect(self) -> None:
        pytest.importorskip("asyncpg")
        from gravtory.backends import create_backend
        from gravtory.backends.postgresql import PostgreSQLBackend

        b = create_backend("postgresql://localhost/test")
        assert isinstance(b, PostgreSQLBackend)

    def test_unknown_raises(self) -> None:
        from gravtory.backends import create_backend
        from gravtory.core.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            create_backend("unknown://foo")
