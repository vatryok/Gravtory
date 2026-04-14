"""Integration tests — retry logic, DLQ entries, and saga compensation.

Tests that step failures trigger retries with the configured policy,
failed workflows land in the DLQ, and saga-enabled workflows run
compensation handlers correctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Fixture workflows ────────────────────────────────────────────

_retry_counter: dict[str, int] = {}
_compensation_log: list[str] = []


@workflow(id="retry-wf-{tag}")
class RetryWorkflow:
    @step(1, retries=3, backoff="constant", backoff_base=0.01)
    async def flaky(self, tag: str) -> dict[str, str]:
        key = f"flaky-{tag}"
        _retry_counter.setdefault(key, 0)
        _retry_counter[key] += 1
        if _retry_counter[key] < 3:
            raise RuntimeError(f"Transient error (attempt {_retry_counter[key]})")
        return {"result": "success"}


@workflow(id="fail-wf-{tag}")
class AlwaysFailWorkflow:
    @step(1)
    async def explode(self, tag: str) -> None:
        raise ValueError("permanent failure")


@workflow(id="saga-wf-{tag}", saga=True)
class SagaWorkflow:
    @step(1, compensate="undo_charge")
    async def charge(self, tag: str) -> dict[str, str]:
        return {"charged": tag}

    @step(2, depends_on=1)
    async def ship(self, charged: str, **kw: object) -> None:
        raise RuntimeError("shipping failed")

    async def undo_charge(self, output: object) -> None:
        _compensation_log.append(f"undo:{output}")


@workflow(id="retry-selective-{tag}")
class SelectiveRetryWorkflow:
    @step(1, retries=2, retry_on=[ConnectionError], backoff="constant", backoff_base=0.01)
    async def selective(self, tag: str) -> dict[str, str]:
        key = f"sel-{tag}"
        _retry_counter.setdefault(key, 0)
        _retry_counter[key] += 1
        if _retry_counter[key] == 1:
            raise ConnectionError("transient")
        return {"ok": "done"}


# ── Tests ────────────────────────────────────────────────────────


class TestRetryIntegration:
    @pytest.mark.asyncio
    async def test_retry_succeeds_after_transient_failures(self, backend: Backend) -> None:
        """Workflow with retries recovers from transient errors."""
        _retry_counter.clear()
        registry = WorkflowRegistry()
        registry.register(RetryWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=RetryWorkflow.definition,
            run_id="retry-wf-t1",
            input_data={"tag": "t1"},
        )
        assert result.status == WorkflowStatus.COMPLETED
        assert _retry_counter["flaky-t1"] == 3  # Failed 2x, succeeded on 3rd

    @pytest.mark.asyncio
    async def test_selective_retry_on_matching_exception(self, backend: Backend) -> None:
        """retry_on filters which exceptions trigger retries."""
        _retry_counter.clear()
        registry = WorkflowRegistry()
        registry.register(SelectiveRetryWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=SelectiveRetryWorkflow.definition,
            run_id="retry-selective-s1",
            input_data={"tag": "s1"},
        )
        assert result.status == WorkflowStatus.COMPLETED


class TestDLQIntegration:
    @pytest.mark.asyncio
    async def test_failed_workflow_lands_in_dlq(self, backend: Backend) -> None:
        """Permanently failed workflow adds an entry to the DLQ."""
        registry = WorkflowRegistry()
        registry.register(AlwaysFailWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(ValueError, match="permanent failure"):
            await engine.execute_workflow(
                definition=AlwaysFailWorkflow.definition,
                run_id="fail-wf-d1",
                input_data={"tag": "d1"},
            )

        run = await backend.get_workflow_run("fail-wf-d1")
        assert run is not None
        assert run.status == WorkflowStatus.FAILED

        dlq = await backend.list_dlq()
        matching = [e for e in dlq if e.workflow_run_id == "fail-wf-d1"]
        assert len(matching) >= 1
        assert "permanent failure" in matching[0].error_message

    @pytest.mark.asyncio
    async def test_failed_workflow_stores_error_message(self, backend: Backend) -> None:
        """Failed workflow run stores the error message."""
        registry = WorkflowRegistry()
        registry.register(AlwaysFailWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(ValueError):
            await engine.execute_workflow(
                definition=AlwaysFailWorkflow.definition,
                run_id="fail-wf-d2",
                input_data={"tag": "d2"},
            )

        run = await backend.get_workflow_run("fail-wf-d2")
        assert run is not None
        assert run.error_message is not None
        assert "permanent failure" in run.error_message


class TestSagaIntegration:
    @pytest.mark.asyncio
    async def test_saga_triggers_compensation(self, backend: Backend) -> None:
        """Saga-enabled workflow runs compensation on failure."""
        _compensation_log.clear()
        registry = WorkflowRegistry()
        registry.register(SagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError, match="shipping failed"):
            await engine.execute_workflow(
                definition=SagaWorkflow.definition,
                run_id="saga-wf-s1",
                input_data={"tag": "s1"},
            )

        run = await backend.get_workflow_run("saga-wf-s1")
        assert run is not None
        assert run.status in (WorkflowStatus.COMPENSATED, WorkflowStatus.COMPENSATION_FAILED)

    @pytest.mark.asyncio
    async def test_saga_failure_still_goes_to_dlq(self, backend: Backend) -> None:
        """Even with saga compensation, the failure is recorded in DLQ."""
        _compensation_log.clear()
        registry = WorkflowRegistry()
        registry.register(SagaWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError):
            await engine.execute_workflow(
                definition=SagaWorkflow.definition,
                run_id="saga-wf-s2",
                input_data={"tag": "s2"},
            )

        dlq = await backend.list_dlq()
        matching = [e for e in dlq if e.workflow_run_id == "saga-wf-s2"]
        assert len(matching) >= 1
