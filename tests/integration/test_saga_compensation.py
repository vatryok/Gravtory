"""Integration tests — full saga compensation end-to-end.

Verifies:
  - Multi-step workflows trigger compensation in REVERSE order on failure.
  - Best-effort: if one compensation handler fails, the rest still run.
  - Compensation handlers receive the correct step output.
  - Workflow status transitions through COMPENSATING → COMPENSATED.
  - COMPENSATION_FAILED status when a handler raises.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Shared state for tracking compensation calls ────────────────

_compensation_log: list[str] = []
_compensation_outputs: list[object] = []


# ── Fixture workflows ───────────────────────────────────────────


@workflow(id="saga-full-{tag}", saga=True)
class FullSagaWorkflow:
    """3 steps with compensation on steps 1 and 2; step 3 fails."""

    @step(1, compensate="undo_step1")
    async def step1(self, tag: str) -> dict[str, str]:
        return {"step1": f"data-{tag}"}

    @step(2, depends_on=1, compensate="undo_step2")
    async def step2(self, step1: str, **kw: object) -> dict[str, str]:
        return {"step2": f"processed-{step1}"}

    @step(3, depends_on=2)
    async def step3(self, **kw: object) -> None:
        raise RuntimeError("step3 bombed")

    async def undo_step2(self, output: object) -> None:
        _compensation_log.append("undo_step2")
        _compensation_outputs.append(output)

    async def undo_step1(self, output: object) -> None:
        _compensation_log.append("undo_step1")
        _compensation_outputs.append(output)


@workflow(id="saga-partial-{tag}", saga=True)
class PartialFailSagaWorkflow:
    """Compensation for step 2 raises — best-effort checks step 1 still runs."""

    @step(1, compensate="undo_ok")
    async def first(self, tag: str) -> dict[str, str]:
        return {"first": tag}

    @step(2, depends_on=1, compensate="undo_broken")
    async def second(self, first: str, **kw: object) -> dict[str, str]:
        return {"second": first}

    @step(3, depends_on=2)
    async def third(self, **kw: object) -> None:
        raise RuntimeError("third crashed")

    async def undo_broken(self, output: object) -> None:
        _compensation_log.append("undo_broken_attempted")
        raise RuntimeError("compensation handler exploded")

    async def undo_ok(self, output: object) -> None:
        _compensation_log.append("undo_ok")


@workflow(id="saga-4step-{tag}", saga=True)
class FourStepSagaWorkflow:
    """4 steps, all with compensation; step 4 fails.

    Verifies reverse-order: undo3 → undo2 → undo1.
    """

    @step(1, compensate="undo1")
    async def s1(self, tag: str) -> dict[str, str]:
        return {"s1": tag}

    @step(2, depends_on=1, compensate="undo2")
    async def s2(self, s1: str, **kw: object) -> dict[str, str]:
        return {"s2": s1}

    @step(3, depends_on=2, compensate="undo3")
    async def s3(self, s2: str, **kw: object) -> dict[str, str]:
        return {"s3": s2}

    @step(4, depends_on=3)
    async def s4(self, **kw: object) -> None:
        raise RuntimeError("s4 failed")

    async def undo3(self, output: object) -> None:
        _compensation_log.append("undo3")

    async def undo2(self, output: object) -> None:
        _compensation_log.append("undo2")

    async def undo1(self, output: object) -> None:
        _compensation_log.append("undo1")


# ── Tests ────────────────────────────────────────────────────────


class TestSagaCompensationEndToEnd:
    """Full saga compensation integration tests."""

    @pytest.mark.asyncio
    async def test_compensation_runs_in_reverse_order(self) -> None:
        """Compensation handlers execute in descending step order."""
        _compensation_log.clear()
        _compensation_outputs.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(FullSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError, match="step3 bombed"):
            await engine.execute_workflow(
                definition=FullSagaWorkflow.definition,
                run_id="saga-full-rev",
                input_data={"tag": "rev"},
            )

        # Compensation must run step2 before step1 (reverse order)
        assert _compensation_log == ["undo_step2", "undo_step1"]

    @pytest.mark.asyncio
    async def test_compensation_receives_step_output(self) -> None:
        """Compensation handlers receive the original step output."""
        _compensation_log.clear()
        _compensation_outputs.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(FullSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=FullSagaWorkflow.definition,
                run_id="saga-full-out",
                input_data={"tag": "out"},
            )

        # undo_step2 gets step2's output, undo_step1 gets step1's output
        assert len(_compensation_outputs) == 2
        assert isinstance(_compensation_outputs[0], dict)
        assert "step2" in _compensation_outputs[0]
        assert isinstance(_compensation_outputs[1], dict)
        assert "step1" in _compensation_outputs[1]

    @pytest.mark.asyncio
    async def test_compensated_status_on_success(self) -> None:
        """Workflow ends with COMPENSATED status when all compensations succeed."""
        _compensation_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(FullSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=FullSagaWorkflow.definition,
                run_id="saga-full-status",
                input_data={"tag": "status"},
            )

        run = await backend.get_workflow_run("saga-full-status")
        assert run is not None
        assert run.status == WorkflowStatus.COMPENSATED

    @pytest.mark.asyncio
    async def test_compensation_failed_status(self) -> None:
        """Workflow ends with COMPENSATION_FAILED when a handler raises."""
        _compensation_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(PartialFailSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=PartialFailSagaWorkflow.definition,
                run_id="saga-partial-cf",
                input_data={"tag": "cf"},
            )

        run = await backend.get_workflow_run("saga-partial-cf")
        assert run is not None
        assert run.status == WorkflowStatus.COMPENSATION_FAILED

    @pytest.mark.asyncio
    async def test_best_effort_remaining_handlers_still_run(self) -> None:
        """Even if undo_broken fails, undo_ok still executes (best-effort)."""
        _compensation_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(PartialFailSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=PartialFailSagaWorkflow.definition,
                run_id="saga-partial-be",
                input_data={"tag": "be"},
            )

        # undo_broken was attempted, then undo_ok should also have run
        assert "undo_broken_attempted" in _compensation_log
        assert "undo_ok" in _compensation_log

    @pytest.mark.asyncio
    async def test_four_step_reverse_order(self) -> None:
        """4-step workflow compensates in exact reverse: undo3 → undo2 → undo1."""
        _compensation_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(FourStepSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError, match="s4 failed"):
            await engine.execute_workflow(
                definition=FourStepSagaWorkflow.definition,
                run_id="saga-4step-rev",
                input_data={"tag": "rev"},
            )

        assert _compensation_log == ["undo3", "undo2", "undo1"]

    @pytest.mark.asyncio
    async def test_saga_failure_recorded_in_dlq(self) -> None:
        """Saga-compensated workflow failure is recorded in DLQ."""
        _compensation_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(FullSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=FullSagaWorkflow.definition,
                run_id="saga-full-dlq",
                input_data={"tag": "dlq"},
            )

        dlq = await backend.list_dlq()
        matching = [e for e in dlq if e.workflow_run_id == "saga-full-dlq"]
        assert len(matching) >= 1
        assert "step3 bombed" in matching[0].error_message

    @pytest.mark.asyncio
    async def test_saga_with_sqlite_backend(self, backend: Backend) -> None:
        """Saga compensation works with SQLite backend (real DB)."""
        _compensation_log.clear()
        registry = WorkflowRegistry()
        registry.register(FullSagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=FullSagaWorkflow.definition,
                run_id="saga-full-sqlite",
                input_data={"tag": "sqlite"},
            )

        run = await backend.get_workflow_run("saga-full-sqlite")
        assert run is not None
        assert run.status == WorkflowStatus.COMPENSATED
        assert _compensation_log == ["undo_step2", "undo_step1"]
