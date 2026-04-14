"""Integration tests — crash recovery and workflow resumption.

Tests that workflows can be interrupted mid-execution and resumed from
the last checkpoint, with step outputs replayed from the backend.

Uses InMemoryBackend because SQLite drops non-bytes output_data, and
these tests focus on the ExecutionEngine resume logic, not serialization.
"""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

pytestmark = pytest.mark.integration


# ── Fixture workflows ────────────────────────────────────────────

_call_log: list[str] = []


@workflow(id="resumable-{tag}")
class ResumableWorkflow:
    @step(1)
    async def step_a(self, tag: str) -> dict[str, str]:
        _call_log.append("step_a")
        return {"a_out": f"a-{tag}"}

    @step(2, depends_on=1)
    async def step_b(self, a_out: str, **kw: object) -> dict[str, str]:
        _call_log.append("step_b")
        return {"b_out": f"b-{a_out}"}

    @step(3, depends_on=2)
    async def step_c(self, b_out: str, **kw: object) -> dict[str, str]:
        _call_log.append("step_c")
        return {"c_out": f"c-{b_out}"}


# ── Helper ───────────────────────────────────────────────────────


async def _mem_backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


# ── Tests ────────────────────────────────────────────────────────


class TestCrashRecovery:
    @pytest.mark.asyncio
    async def test_resume_skips_completed_steps(self) -> None:
        """Resuming replays step 1 from checkpoint and runs steps 2+3."""
        _call_log.clear()
        backend = await _mem_backend()
        registry = WorkflowRegistry()
        registry.register(ResumableWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        # Simulate: step 1 was already completed before crash
        run = WorkflowRun(
            id="resumable-x",
            workflow_name="ResumableWorkflow",
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="resumable-x",
                step_order=1,
                step_name="step_a",
                output_data={"a_out": "a-x"},
                status=StepStatus.COMPLETED,
                duration_ms=10,
            )
        )

        # Resume from checkpoint
        result = await engine.execute_workflow(
            definition=ResumableWorkflow.definition,
            run_id="resumable-x",
            input_data={"tag": "x"},
            resume=True,
        )
        assert result.status == WorkflowStatus.COMPLETED

        # step_a should NOT have been called again (replayed from checkpoint)
        assert "step_a" not in _call_log
        # step_b and step_c should have been called
        assert "step_b" in _call_log
        assert "step_c" in _call_log

    @pytest.mark.asyncio
    async def test_resume_with_all_completed(self) -> None:
        """Resuming a workflow where all steps are already done finishes immediately."""
        _call_log.clear()
        backend = await _mem_backend()
        registry = WorkflowRegistry()
        registry.register(ResumableWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(
            id="resumable-done",
            workflow_name="ResumableWorkflow",
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)

        # All three steps already completed
        for i, name in enumerate(["step_a", "step_b", "step_c"], start=1):
            await backend.save_step_output(
                StepOutput(
                    workflow_run_id="resumable-done",
                    step_order=i,
                    step_name=name,
                    output_data={"out": f"v{i}"},
                    status=StepStatus.COMPLETED,
                )
            )

        result = await engine.execute_workflow(
            definition=ResumableWorkflow.definition,
            run_id="resumable-done",
            input_data={"tag": "done"},
            resume=True,
        )
        assert result.status == WorkflowStatus.COMPLETED
        # No steps should have been called
        assert _call_log == []

    @pytest.mark.asyncio
    async def test_idempotent_step_execution(self) -> None:
        """Running the same workflow twice returns existing result without re-executing."""
        _call_log.clear()
        backend = await _mem_backend()
        registry = WorkflowRegistry()
        registry.register(ResumableWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        # First execution
        result1 = await engine.execute_workflow(
            definition=ResumableWorkflow.definition,
            run_id="resumable-idem",
            input_data={"tag": "idem"},
        )
        assert result1.status == WorkflowStatus.COMPLETED
        first_calls = list(_call_log)
        assert len(first_calls) == 3

        _call_log.clear()

        # Second execution (resume mode) — should replay all from checkpoint
        result2 = await engine.execute_workflow(
            definition=ResumableWorkflow.definition,
            run_id="resumable-idem",
            input_data={"tag": "idem"},
            resume=True,
        )
        assert result2.status == WorkflowStatus.COMPLETED
        # Steps should not have been called again (all replayed from checkpoint)
        assert _call_log == []


class TestRecoverIncomplete:
    @pytest.mark.asyncio
    async def test_recover_running_workflow(self) -> None:
        """recover_incomplete finds and resumes RUNNING workflows."""
        _call_log.clear()
        backend = await _mem_backend()
        registry = WorkflowRegistry()
        registry.register(ResumableWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        # Create a RUNNING workflow with step 1 done
        run = WorkflowRun(
            id="resumable-recover",
            workflow_name="ResumableWorkflow",
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("resumable-recover", WorkflowStatus.RUNNING)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="resumable-recover",
                step_order=1,
                step_name="step_a",
                output_data={"a_out": "a-recover"},
                status=StepStatus.COMPLETED,
            )
        )

        recovered = await engine.recover_incomplete()
        assert "resumable-recover" in recovered

        final = await backend.get_workflow_run("resumable-recover")
        assert final is not None
        assert final.status == WorkflowStatus.COMPLETED
