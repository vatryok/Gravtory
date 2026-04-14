"""Tests for WorkflowTestRunner (Section 11.1)."""

from __future__ import annotations

import pytest

from gravtory.core.types import StepStatus, WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow
from gravtory.testing.runner import WorkflowTestRunner

# ---------------------------------------------------------------------------
# Fixture workflows
# ---------------------------------------------------------------------------


@workflow(id="simple-{x}")
class SimpleWorkflow:
    @step(1)
    async def add_one(self, x: int) -> dict[str, int]:
        return {"result": x + 1}


@workflow(id="two-step-{val}")
class TwoStepWorkflow:
    @step(1)
    async def first(self, val: str) -> dict[str, str]:
        return {"intermediate": val.upper()}

    @step(2, depends_on=[1])
    async def second(self, intermediate: str, **kwargs: object) -> dict[str, str]:
        return {"final": intermediate + "!"}


@workflow(id="failing-{x}")
class FailingWorkflow:
    @step(1)
    async def explode(self, x: int) -> None:
        raise ValueError("boom")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWorkflowTestRunner:
    @pytest.mark.asyncio
    async def test_simple_workflow(self) -> None:
        runner = WorkflowTestRunner()
        result = await runner.run(SimpleWorkflow, x=5)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.run_id == "simple-5"
        assert 1 in result.steps
        assert result.steps[1].status == StepStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_mock_step_return_value(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 999})
        result = await runner.run(SimpleWorkflow, x=1)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.steps[1].output == {"result": 999}
        assert result.steps[1].was_mocked is True

    @pytest.mark.asyncio
    async def test_mock_step_raises(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", raises=RuntimeError)
        result = await runner.run(SimpleWorkflow, x=1)
        assert result.status == WorkflowStatus.FAILED
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_simulate_crash_and_resume(self) -> None:
        runner = WorkflowTestRunner()
        # Mock step 1 so it doesn't need 'self' binding, then crash after it
        runner.mock_step("first", return_value={"intermediate": "HELLO"})
        runner.simulate_crash("TwoStepWorkflow", after_step=1)

        # First run: crashes after step 1
        result1 = await runner.run(TwoStepWorkflow, val="hello")
        assert result1.status == WorkflowStatus.FAILED
        assert "Simulated crash" in (result1.error or "")

        # Clear crash and resume — step 1 should be replayed from checkpoint
        runner._crash_points.clear()
        runner._mock_steps.clear()
        runner.mock_step("second", return_value={"final": "HELLO!"})
        result2 = await runner.run(TwoStepWorkflow, val="hello")
        assert result2.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_assert_step_called(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 42})
        await runner.run(SimpleWorkflow, x=10)
        runner.assert_step_called("add_one")
        runner.assert_step_called("add_one", times=1)
        runner.assert_step_called("add_one", with_input={"x": 10})

    @pytest.mark.asyncio
    async def test_assert_step_called_fails(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 42})
        await runner.run(SimpleWorkflow, x=10)
        with pytest.raises(AssertionError, match="never called"):
            runner.assert_step_called("nonexistent_step")

    @pytest.mark.asyncio
    async def test_assert_step_not_called(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 42})
        await runner.run(SimpleWorkflow, x=10)
        runner.assert_step_not_called("some_other_step")
        with pytest.raises(AssertionError, match="was called"):
            runner.assert_step_not_called("add_one")

    @pytest.mark.asyncio
    async def test_execution_order_tracked(self) -> None:
        runner = WorkflowTestRunner()
        result = await runner.run(TwoStepWorkflow, val="hi")
        assert result.status == WorkflowStatus.COMPLETED
        assert result.execution_order == [1, 2]

    @pytest.mark.asyncio
    async def test_test_result_contents(self) -> None:
        runner = WorkflowTestRunner()
        result = await runner.run(SimpleWorkflow, x=7)
        assert result.run_id == "simple-7"
        assert result.total_duration_ms >= 0
        assert result.error is None
        assert 1 in result.steps
        step_result = result.steps[1]
        assert step_result.order == 1
        assert step_result.name == "add_one"
        assert step_result.duration_ms >= 0

    @pytest.mark.asyncio
    async def test_reset_clears_state(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 0})
        runner.simulate_crash("SimpleWorkflow", after_step=1)
        runner.reset()
        assert len(runner._mock_steps) == 0
        assert len(runner._crash_points) == 0
        assert len(runner._call_log) == 0
        # Should still work after reset
        result = await runner.run(SimpleWorkflow, x=3)
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_failing_workflow_status(self) -> None:
        runner = WorkflowTestRunner()
        result = await runner.run(FailingWorkflow, x=1)
        assert result.status == WorkflowStatus.FAILED
        assert result.error is not None


class TestTestRunnerGapFill:
    """Gap-fill tests for WorkflowTestRunner edge cases."""

    @pytest.mark.asyncio
    async def test_mock_step_side_effect(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", side_effect=lambda **kw: {"result": kw["x"] * 10})
        result = await runner.run(SimpleWorkflow, x=3)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.steps[1].output == {"result": 30}

    @pytest.mark.asyncio
    async def test_assert_step_called_wrong_times(self) -> None:
        runner = WorkflowTestRunner()
        runner.mock_step("add_one", return_value={"result": 1})
        await runner.run(SimpleWorkflow, x=1)
        with pytest.raises(AssertionError):
            runner.assert_step_called("add_one", times=5)

    @pytest.mark.asyncio
    async def test_run_returns_step_durations(self) -> None:
        runner = WorkflowTestRunner()
        result = await runner.run(SimpleWorkflow, x=1)
        assert result.steps[1].duration_ms >= 0
