"""Integration tests — workflow execution lifecycle with SQLite backend.

Tests the full flow: create workflow → register → execute → checkpoint → complete,
verifying that data persists correctly in the database at each stage.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepStatus, WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Fixture workflows ────────────────────────────────────────────


@workflow(id="single-{x}")
class SingleStepWorkflow:
    @step(1)
    async def compute(self, x: int) -> dict[str, int]:
        return {"result": x * 2}


@workflow(id="two-step-{val}")
class TwoStepWorkflow:
    @step(1)
    async def upper(self, val: str) -> dict[str, str]:
        return {"intermediate": val.upper()}

    @step(2, depends_on=1)
    async def exclaim(self, intermediate: str, **kw: object) -> dict[str, str]:
        return {"final": intermediate + "!"}


@workflow(id="three-step-{n}")
class ThreeStepWorkflow:
    @step(1)
    async def first(self, n: int) -> dict[str, int]:
        return {"a": n + 1}

    @step(2, depends_on=1)
    async def second(self, a: int, **kw: object) -> dict[str, int]:
        return {"b": a * 10}

    @step(3, depends_on=2)
    async def third(self, b: int, **kw: object) -> dict[str, int]:
        return {"c": b + 5}


# ── Tests ────────────────────────────────────────────────────────


class TestSingleStepExecution:
    @pytest.mark.asyncio
    async def test_execute_and_complete(self, backend: Backend) -> None:
        """Single-step workflow executes and reaches COMPLETED."""
        registry = WorkflowRegistry()
        registry.register(SingleStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=SingleStepWorkflow.definition,
            run_id="single-42",
            input_data={"x": 42},
        )
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_step_output_persisted(self, backend: Backend) -> None:
        """Step output is checkpointed in the backend."""
        registry = WorkflowRegistry()
        registry.register(SingleStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=SingleStepWorkflow.definition,
            run_id="single-7",
            input_data={"x": 7},
        )

        out = await backend.get_step_output("single-7", 1)
        assert out is not None
        assert out.status == StepStatus.COMPLETED
        assert out.step_name == "compute"

    @pytest.mark.asyncio
    async def test_workflow_run_persisted(self, backend: Backend) -> None:
        """Workflow run record is saved with correct status."""
        registry = WorkflowRegistry()
        registry.register(SingleStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=SingleStepWorkflow.definition,
            run_id="single-99",
            input_data={"x": 99},
        )

        run = await backend.get_workflow_run("single-99")
        assert run is not None
        assert run.status == WorkflowStatus.COMPLETED
        assert run.workflow_name == "SingleStepWorkflow"


class TestMultiStepExecution:
    @pytest.mark.asyncio
    async def test_two_step_chain(self, backend: Backend) -> None:
        """Two-step workflow with dependency executes in order."""
        registry = WorkflowRegistry()
        registry.register(TwoStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=TwoStepWorkflow.definition,
            run_id="two-step-hello",
            input_data={"val": "hello"},
        )
        assert result.status == WorkflowStatus.COMPLETED

        # Both steps should be persisted
        s1 = await backend.get_step_output("two-step-hello", 1)
        s2 = await backend.get_step_output("two-step-hello", 2)
        assert s1 is not None
        assert s2 is not None
        assert s1.status == StepStatus.COMPLETED
        assert s2.status == StepStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_three_step_chain(self, backend: Backend) -> None:
        """Three-step linear chain executes all steps."""
        registry = WorkflowRegistry()
        registry.register(ThreeStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ThreeStepWorkflow.definition,
            run_id="three-step-5",
            input_data={"n": 5},
        )
        assert result.status == WorkflowStatus.COMPLETED

        outputs = await backend.get_step_outputs("three-step-5")
        assert len(outputs) == 3

    @pytest.mark.asyncio
    async def test_step_order_tracked(self, backend: Backend) -> None:
        """Backend tracks current_step as steps complete."""
        registry = WorkflowRegistry()
        registry.register(ThreeStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=ThreeStepWorkflow.definition,
            run_id="three-step-10",
            input_data={"n": 10},
        )

        run = await backend.get_workflow_run("three-step-10")
        assert run is not None
        assert run.current_step == 3


class TestWorkflowListing:
    @pytest.mark.asyncio
    async def test_list_completed_runs(self, backend: Backend) -> None:
        """Completed runs appear in the listing."""
        registry = WorkflowRegistry()
        registry.register(SingleStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        for i in range(3):
            await engine.execute_workflow(
                definition=SingleStepWorkflow.definition,
                run_id=f"list-{i}",
                input_data={"x": i},
            )

        runs = await backend.list_workflow_runs(status=WorkflowStatus.COMPLETED)
        assert len(runs) == 3

    @pytest.mark.asyncio
    async def test_list_filters_by_status(self, backend: Backend) -> None:
        """Status filter excludes non-matching runs."""
        registry = WorkflowRegistry()
        registry.register(SingleStepWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=SingleStepWorkflow.definition,
            run_id="completed-run",
            input_data={"x": 1},
        )

        pending = await backend.list_workflow_runs(status=WorkflowStatus.PENDING)
        assert all(r.id != "completed-run" for r in pending)
