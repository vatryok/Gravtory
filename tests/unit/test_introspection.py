"""Tests for WorkflowInspection, StepInspection, ErrorInfo (Section 11.5)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    StepOutput,
    StepStatus,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.testing.introspection import (
    ErrorInfo,
    StepInspection,
    WorkflowInspection,
    inspect_workflow,
)


async def _seeded_backend() -> InMemoryBackend:
    backend = InMemoryBackend()
    await backend.initialize()

    run = WorkflowRun(
        id="run-1",
        workflow_name="OrderWorkflow",
        workflow_version=1,
        namespace="default",
        status=WorkflowStatus.COMPLETED,
        created_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        updated_at=datetime(2025, 1, 1, 10, 0, 5, tzinfo=timezone.utc),
        completed_at=datetime(2025, 1, 1, 10, 0, 5, tzinfo=timezone.utc),
    )
    await backend.create_workflow_run(run)

    await backend.save_step_output(
        StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge",
            status=StepStatus.COMPLETED,
            duration_ms=120,
            retry_count=0,
        )
    )
    await backend.save_step_output(
        StepOutput(
            workflow_run_id="run-1",
            step_order=2,
            step_name="ship",
            status=StepStatus.COMPLETED,
            duration_ms=300,
            retry_count=1,
        )
    )
    return backend


class TestWorkflowInspection:
    @pytest.mark.asyncio
    async def test_inspect_completed_workflow(self) -> None:
        backend = await _seeded_backend()
        inspection = await inspect_workflow(backend, "run-1")
        assert inspection is not None
        assert inspection.run_id == "run-1"
        assert inspection.workflow_name == "OrderWorkflow"
        assert inspection.status == WorkflowStatus.COMPLETED
        assert len(inspection.steps) == 2
        assert inspection.steps[1].name == "charge"
        assert inspection.steps[2].name == "ship"

    @pytest.mark.asyncio
    async def test_inspect_nonexistent_returns_none(self) -> None:
        backend = await _seeded_backend()
        result = await inspect_workflow(backend, "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_inspect_failed_workflow(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        run = WorkflowRun(
            id="fail-1",
            workflow_name="BrokenWorkflow",
            status=WorkflowStatus.FAILED,
            error_message="step exploded",
            error_traceback="Traceback ...",
        )
        await backend.create_workflow_run(run)

        inspection = await inspect_workflow(backend, "fail-1")
        assert inspection is not None
        assert inspection.status == WorkflowStatus.FAILED
        assert inspection.error is not None
        assert inspection.error.message == "step exploded"
        assert inspection.error.traceback == "Traceback ..."

    @pytest.mark.asyncio
    async def test_progress_property(self) -> None:
        backend = await _seeded_backend()
        inspection = await inspect_workflow(backend, "run-1")
        assert inspection is not None
        # Both steps completed → progress = 1.0
        assert inspection.progress == 1.0

    @pytest.mark.asyncio
    async def test_is_done_property(self) -> None:
        backend = await _seeded_backend()
        inspection = await inspect_workflow(backend, "run-1")
        assert inspection is not None
        assert inspection.is_done is True

    @pytest.mark.asyncio
    async def test_duration_ms_computed(self) -> None:
        backend = await _seeded_backend()
        inspection = await inspect_workflow(backend, "run-1")
        assert inspection is not None
        assert inspection.duration_ms == 5000  # 5 seconds


class TestStepInspection:
    def test_step_inspection_fields(self) -> None:
        si = StepInspection(
            order=1,
            name="charge",
            status=StepStatus.COMPLETED,
            output={"txn": "abc"},
            duration_ms=100,
            retry_count=0,
        )
        assert si.order == 1
        assert si.name == "charge"
        assert si.status == StepStatus.COMPLETED
        assert si.output == {"txn": "abc"}


class TestErrorInfo:
    def test_error_info_fields(self) -> None:
        err = ErrorInfo(
            message="boom",
            traceback="Traceback (most recent call last):\n...",
            step_name="charge",
            step_order=1,
        )
        assert err.message == "boom"
        assert err.step_name == "charge"
        assert err.step_order == 1


class TestWorkflowInspectionProperties:
    def test_is_done_for_all_terminal_statuses(self) -> None:
        for status in [
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.COMPENSATION_FAILED,
            WorkflowStatus.CANCELLED,
        ]:
            wi = WorkflowInspection(
                run_id="x",
                workflow_name="W",
                workflow_version=1,
                status=status,
                namespace="default",
                current_step=None,
                steps={},
            )
            assert wi.is_done is True, f"Expected is_done for {status}"

    def test_is_done_false_for_running(self) -> None:
        wi = WorkflowInspection(
            run_id="x",
            workflow_name="W",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
            namespace="default",
            current_step=1,
            steps={},
        )
        assert wi.is_done is False

    def test_progress_empty_steps(self) -> None:
        wi = WorkflowInspection(
            run_id="x",
            workflow_name="W",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
            namespace="default",
            current_step=None,
            steps={},
        )
        assert wi.progress == 0.0

    def test_progress_partial(self) -> None:
        wi = WorkflowInspection(
            run_id="x",
            workflow_name="W",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
            namespace="default",
            current_step=2,
            steps={
                1: StepInspection(order=1, name="a", status=StepStatus.COMPLETED),
                2: StepInspection(order=2, name="b", status=StepStatus.RUNNING),
                3: StepInspection(order=3, name="c", status=StepStatus.PENDING),
            },
        )
        assert wi.progress == pytest.approx(1.0 / 3.0)


class TestIntrospectionGapFill:
    """Gap-fill tests for introspection edge cases."""

    def test_progress_all_completed(self) -> None:
        wi = WorkflowInspection(
            run_id="x",
            workflow_name="W",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
            namespace="default",
            current_step=None,
            steps={
                1: StepInspection(order=1, name="a", status=StepStatus.COMPLETED),
                2: StepInspection(order=2, name="b", status=StepStatus.COMPLETED),
            },
        )
        assert wi.progress == pytest.approx(1.0)

    def test_progress_single_step_running(self) -> None:
        wi = WorkflowInspection(
            run_id="x",
            workflow_name="W",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
            namespace="default",
            current_step=1,
            steps={
                1: StepInspection(order=1, name="a", status=StepStatus.RUNNING),
            },
        )
        assert wi.progress == 0.0

    def test_step_inspection_fields(self) -> None:
        si = StepInspection(order=5, name="charge", status=StepStatus.COMPLETED)
        assert si.order == 5
        assert si.name == "charge"
        assert si.status == StepStatus.COMPLETED
