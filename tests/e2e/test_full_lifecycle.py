"""E2E tests — full workflow lifecycle through the Gravtory user-facing API.

Exercises the complete user journey: create Gravtory instance, define
workflows with decorators, run them, inspect results, and verify
persistence across sessions — all with a real SQLite backend.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gravtory.core.engine import Gravtory
from gravtory.core.types import WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

pytestmark = pytest.mark.e2e


# ── Fixture workflows ────────────────────────────────────────────


@workflow(id="order-{order_id}")
class OrderWorkflow:
    @step(1)
    async def validate(self, order_id: str) -> dict[str, str]:
        return {"validated": order_id}

    @step(2, depends_on=1)
    async def charge(self, validated: str, **kw: object) -> dict[str, str]:
        return {"charged": validated}

    @step(3, depends_on=2)
    async def ship(self, charged: str, **kw: object) -> dict[str, str]:
        return {"shipped": charged}


@workflow(id="failing-order-{order_id}")
class FailingOrderWorkflow:
    @step(1)
    async def validate(self, order_id: str) -> dict[str, str]:
        return {"validated": order_id}

    @step(2, depends_on=1)
    async def charge(self, validated: str, **kw: object) -> None:
        raise RuntimeError("payment gateway down")


# ── Tests ────────────────────────────────────────────────────────


class TestEndToEndOrderWorkflow:
    @pytest.mark.asyncio
    async def test_happy_path(self, e2e_db: str) -> None:
        """Order workflow completes all three steps."""
        grav = Gravtory(backend=e2e_db)
        await grav.start()

        result = await grav.run(OrderWorkflow, order_id="abc123")
        assert result.status == WorkflowStatus.COMPLETED
        assert result.id == "order-abc123"

        # Inspect via the API
        inspected = await grav.inspect("order-abc123")
        assert inspected.status == WorkflowStatus.COMPLETED

        # List should contain the run
        runs = await grav.list()
        assert any(r.id == "order-abc123" for r in runs)

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_idempotent_rerun(self, e2e_db: str) -> None:
        """Running the same order_id twice returns the cached result."""
        grav = Gravtory(backend=e2e_db)
        await grav.start()

        r1 = await grav.run(OrderWorkflow, order_id="dup-1")
        r2 = await grav.run(OrderWorkflow, order_id="dup-1")
        assert r1.id == r2.id
        assert r1.status == WorkflowStatus.COMPLETED
        assert r2.status == WorkflowStatus.COMPLETED

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_workflows(self, e2e_db: str) -> None:
        """Multiple different workflow runs complete independently."""
        grav = Gravtory(backend=e2e_db)
        await grav.start()

        r1 = await grav.run(OrderWorkflow, order_id="multi-1")
        r2 = await grav.run(OrderWorkflow, order_id="multi-2")
        r3 = await grav.run(OrderWorkflow, order_id="multi-3")

        assert r1.status == WorkflowStatus.COMPLETED
        assert r2.status == WorkflowStatus.COMPLETED
        assert r3.status == WorkflowStatus.COMPLETED

        count = await grav.count()
        assert count >= 3

        await grav.shutdown()

    @pytest.mark.asyncio
    async def test_failure_path(self, e2e_db: str) -> None:
        """Workflow failure is recorded with error message."""
        grav = Gravtory(backend=e2e_db)
        await grav.start()

        with pytest.raises(RuntimeError, match="payment gateway down"):
            await grav.run(FailingOrderWorkflow, order_id="fail-1")

        run = await grav.inspect("failing-order-fail-1")
        assert run.status == WorkflowStatus.FAILED
        assert run.error_message is not None
        assert "payment gateway down" in run.error_message

        await grav.shutdown()


class TestEndToEndPersistence:
    @pytest.mark.asyncio
    async def test_data_survives_restart(self, tmp_path: Path) -> None:
        """Data persists across Gravtory sessions."""
        db = f"sqlite:///{tmp_path / 'persist.db'}"

        # Session 1: run a workflow
        grav1 = Gravtory(backend=db)
        await grav1.start()
        r1 = await grav1.run(OrderWorkflow, order_id="persist-1")
        assert r1.status == WorkflowStatus.COMPLETED
        await grav1.shutdown()

        # Session 2: verify it's still there
        grav2 = Gravtory(backend=db)
        await grav2.start()
        inspected = await grav2.inspect("order-persist-1")
        assert inspected.status == WorkflowStatus.COMPLETED

        runs = await grav2.list()
        assert any(r.id == "order-persist-1" for r in runs)

        await grav2.shutdown()

    @pytest.mark.asyncio
    async def test_count_persists(self, tmp_path: Path) -> None:
        """Workflow counts survive restart."""
        db = f"sqlite:///{tmp_path / 'count.db'}"

        grav1 = Gravtory(backend=db)
        await grav1.start()
        for i in range(5):
            await grav1.run(OrderWorkflow, order_id=f"count-{i}")
        c1 = await grav1.count()
        await grav1.shutdown()

        grav2 = Gravtory(backend=db)
        await grav2.start()
        c2 = await grav2.count()
        assert c2 == c1
        await grav2.shutdown()


class TestEndToEndBackgroundMode:
    @pytest.mark.asyncio
    async def test_background_enqueue(self, e2e_db: str) -> None:
        """background=True enqueues and returns run_id."""
        grav = Gravtory(backend=e2e_db)
        await grav.start()

        run_id = await grav.run(OrderWorkflow, background=True, order_id="bg-1")
        assert isinstance(run_id, str)
        assert run_id == "order-bg-1"

        # The run should exist in the backend
        run = await grav.inspect("order-bg-1")
        assert run.status in (
            WorkflowStatus.PENDING,
            WorkflowStatus.RUNNING,
            WorkflowStatus.COMPLETED,
        )

        await grav.shutdown()
