"""Integration tests — parallel step execution and conditional steps.

Tests that independent steps run in parallel, and conditional steps
are properly skipped or executed based on their condition function.

Parallel tests use InMemoryBackend because SQLite cannot handle
concurrent transactions from asyncio.gather.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.context import StepContext
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepStatus, WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Fixture workflows ────────────────────────────────────────────

_parallel_log: list[str] = []


@workflow(id="parallel-{tag}")
class ParallelWorkflow:
    """Steps 2 and 3 depend only on step 1, so they can run in parallel."""

    @step(1)
    async def root(self, tag: str) -> dict[str, str]:
        _parallel_log.append("root")
        return {"base": tag}

    @step(2, depends_on=1)
    async def branch_a(self, base: str, **kw: object) -> dict[str, str]:
        _parallel_log.append("branch_a")
        return {"a": f"a-{base}"}

    @step(3, depends_on=1)
    async def branch_b(self, base: str, **kw: object) -> dict[str, str]:
        _parallel_log.append("branch_b")
        return {"b": f"b-{base}"}

    @step(4, depends_on=[2, 3])
    async def merge(self, **kw: object) -> dict[str, str]:
        _parallel_log.append("merge")
        return {"merged": "done"}


def _high_score(ctx: StepContext) -> bool:
    return ctx.output(1)["score"] > 50


def _low_score(ctx: StepContext) -> bool:
    return ctx.output(1)["score"] <= 50


@workflow(id="conditional-{tag}")
class ConditionalWorkflow:
    @step(1)
    async def evaluate(self, tag: str, score: int = 75) -> dict[str, int]:
        return {"score": score}

    @step(2, depends_on=1, condition=_high_score)
    async def premium_path(self, score: int, **kw: object) -> dict[str, str]:
        return {"path": "premium"}

    @step(3, depends_on=1, condition=_low_score)
    async def basic_path(self, score: int, **kw: object) -> dict[str, str]:
        return {"path": "basic"}


# ── Tests ────────────────────────────────────────────────────────


class TestParallelExecution:
    @pytest.mark.asyncio
    async def test_diamond_dag_completes(self) -> None:
        """Diamond DAG (1 → 2,3 → 4) completes successfully."""
        _parallel_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ParallelWorkflow.definition,
            run_id="parallel-d1",
            input_data={"tag": "d1"},
        )
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_all_steps_checkpointed(self) -> None:
        """All 4 steps in the diamond have checkpoint entries."""
        _parallel_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=ParallelWorkflow.definition,
            run_id="parallel-d2",
            input_data={"tag": "d2"},
        )

        outputs = await backend.get_step_outputs("parallel-d2")
        assert len(outputs) == 4
        assert all(o.status == StepStatus.COMPLETED for o in outputs)

    @pytest.mark.asyncio
    async def test_execution_order(self) -> None:
        """Root runs first, merge runs last."""
        _parallel_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=ParallelWorkflow.definition,
            run_id="parallel-d3",
            input_data={"tag": "d3"},
        )

        assert _parallel_log[0] == "root"
        assert _parallel_log[-1] == "merge"
        # branch_a and branch_b should be somewhere in the middle
        assert "branch_a" in _parallel_log
        assert "branch_b" in _parallel_log


class TestConditionalExecution:
    @pytest.mark.asyncio
    async def test_high_score_takes_premium_path(self, backend: Backend) -> None:
        """Score > 50 → premium_path runs, basic_path skipped."""
        registry = WorkflowRegistry()
        registry.register(ConditionalWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ConditionalWorkflow.definition,
            run_id="conditional-hi",
            input_data={"tag": "hi", "score": 75},
        )
        assert result.status == WorkflowStatus.COMPLETED

        s2 = await backend.get_step_output("conditional-hi", 2)
        s3 = await backend.get_step_output("conditional-hi", 3)
        assert s2 is not None
        assert s2.status == StepStatus.COMPLETED
        assert s3 is not None
        assert s3.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_low_score_takes_basic_path(self, backend: Backend) -> None:
        """Score <= 50 → basic_path runs, premium_path skipped."""
        registry = WorkflowRegistry()
        registry.register(ConditionalWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ConditionalWorkflow.definition,
            run_id="conditional-lo",
            input_data={"tag": "lo", "score": 30},
        )
        assert result.status == WorkflowStatus.COMPLETED

        s2 = await backend.get_step_output("conditional-lo", 2)
        s3 = await backend.get_step_output("conditional-lo", 3)
        assert s2 is not None
        assert s2.status == StepStatus.SKIPPED
        assert s3 is not None
        assert s3.status == StepStatus.COMPLETED
