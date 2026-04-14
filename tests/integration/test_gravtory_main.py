"""Integration tests — Gravtory main class end-to-end.

Tests the user-facing Gravtory class: start, run workflows, shutdown,
using both in-memory and SQLite backends.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gravtory.core.engine import Gravtory
from gravtory.core.types import WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

pytestmark = pytest.mark.integration


# ── Fixture workflows ────────────────────────────────────────────


@workflow(id="grav-single-{x}")
class GravSingleStep:
    @step(1)
    async def compute(self, x: int) -> dict[str, int]:
        return {"result": x * 3}


@workflow(id="grav-chain-{val}")
class GravChainWorkflow:
    @step(1)
    async def first(self, val: str) -> dict[str, str]:
        return {"upper": val.upper()}

    @step(2, depends_on=1)
    async def second(self, upper: str, **kw: object) -> dict[str, str]:
        return {"final": upper + "!!!"}


# ── Tests ────────────────────────────────────────────────────────


class TestGravtoryWithMemory:
    @pytest.mark.asyncio
    async def test_start_run_shutdown(self) -> None:
        """Full lifecycle: start → run → shutdown with in-memory backend."""
        grav = Gravtory(backend="memory://")
        await grav.start()

        result = await grav.run(GravSingleStep, x=10)
        assert result.status == WorkflowStatus.COMPLETED

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_run_chain_workflow(self) -> None:
        """Multi-step workflow via Gravtory.run()."""
        grav = Gravtory(backend="memory://")
        await grav.start()

        result = await grav.run(GravChainWorkflow, val="hello")
        assert result.status == WorkflowStatus.COMPLETED

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_run_same_workflow_twice_is_idempotent(self) -> None:
        """Running the same workflow ID twice returns the cached result."""
        grav = Gravtory(backend="memory://")
        await grav.start()

        r1 = await grav.run(GravSingleStep, x=5)
        r2 = await grav.run(GravSingleStep, x=5)
        assert r1.status == WorkflowStatus.COMPLETED
        assert r2.status == WorkflowStatus.COMPLETED
        assert r1.id == r2.id

        await grav.shutdown()


class TestGravtoryWithSQLite:
    @pytest.mark.asyncio
    async def test_sqlite_lifecycle(self, tmp_path: Path) -> None:
        """Full lifecycle with SQLite file backend."""
        db_path = str(tmp_path / "grav_test.db")
        grav = Gravtory(backend=f"sqlite:///{db_path}")
        await grav.start()

        result = await grav.run(GravSingleStep, x=7)
        assert result.status == WorkflowStatus.COMPLETED

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_sqlite_data_persists(self, tmp_path: Path) -> None:
        """Data persists across Gravtory restart with SQLite."""
        db_path = str(tmp_path / "persist_test.db")

        # First session
        grav1 = Gravtory(backend=f"sqlite:///{db_path}")
        await grav1.start()
        r1 = await grav1.run(GravSingleStep, x=42)
        assert r1.status == WorkflowStatus.COMPLETED
        await grav1.shutdown()

        # Second session — same DB
        grav2 = Gravtory(backend=f"sqlite:///{db_path}")
        await grav2.start()

        # Running same workflow should return existing result
        r2 = await grav2.run(GravSingleStep, x=42)
        assert r2.status == WorkflowStatus.COMPLETED
        assert r2.id == r1.id

        await grav2.shutdown()


class TestGravtoryBackgroundRun:
    @pytest.mark.asyncio
    async def test_background_returns_run_id(self) -> None:
        """background=True returns run_id string, not WorkflowRun."""
        grav = Gravtory(backend="memory://")
        await grav.start()

        result = await grav.run(GravSingleStep, background=True, x=99)
        assert isinstance(result, str)
        assert result == "grav-single-99"

        await grav.shutdown()
