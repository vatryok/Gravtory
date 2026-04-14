"""Tests for workers.local — LocalWorker lifecycle, task execution, heartbeat."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    PendingStep,
    StepDefinition,
    StepResult,
    StepStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.workers.local import LocalWorker


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


@pytest.fixture
def registry() -> MagicMock:
    step1 = StepDefinition(name="step_a", order=1, retries=2, depends_on=[])
    definition = WorkflowDefinition(
        name="test-wf",
        version=1,
        steps={1: step1},
        config=WorkflowConfig(),
    )
    reg = MagicMock()
    reg.get.return_value = definition
    return reg


@pytest.fixture
def engine() -> MagicMock:
    eng = MagicMock()
    eng.execute_single_step = AsyncMock(
        return_value=StepResult(output=b"done", status=StepStatus.COMPLETED)
    )
    return eng


class TestLocalWorkerProperties:
    @pytest.mark.asyncio
    async def test_initial_state(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker("w1", backend, registry, engine)
        assert w.worker_id == "w1"
        assert not w.is_running
        assert w.active_task_count == 0


class TestLocalWorkerLifecycle:
    @pytest.mark.asyncio
    async def test_start_registers_worker(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=0.05
        )
        await w.start()
        assert w.is_running
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "w1"
        await w.stop(drain=False)

    @pytest.mark.asyncio
    async def test_stop_deregisters_worker(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=0.05
        )
        await w.start()
        await w.stop(drain=False)
        assert not w.is_running
        workers = await backend.list_workers()
        assert len(workers) == 0

    @pytest.mark.asyncio
    async def test_stop_with_drain(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=0.05
        )
        await w.start()
        await asyncio.sleep(0.05)
        await w.stop(drain=True)
        assert not w.is_running


class TestLocalWorkerTaskExecution:
    @pytest.mark.asyncio
    async def test_executes_claimed_step(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        # Create a workflow run
        run = WorkflowRun(
            id="run-1", workflow_name="test-wf", workflow_version=1, status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-1", WorkflowStatus.RUNNING)

        # Enqueue a step
        await backend.enqueue_step(
            PendingStep(
                workflow_run_id="run-1",
                step_order=1,
                priority=5,
                max_retries=2,
            )
        )

        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.02, heartbeat_interval=60.0
        )
        await w.start()
        await asyncio.sleep(0.3)  # let it process
        await w.stop(drain=True)

        # Step should have been executed
        engine.execute_single_step.assert_called()

    @pytest.mark.asyncio
    async def test_task_with_no_id_skipped(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=60.0
        )
        task = PendingStep(
            id=None,
            workflow_run_id="run-1",
            step_order=1,
            priority=5,
            max_retries=0,
        )
        await w._execute_task(task)
        engine.execute_single_step.assert_not_called()

    @pytest.mark.asyncio
    async def test_task_with_missing_workflow_run(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=60.0
        )
        task = PendingStep(
            id=1,
            workflow_run_id="nonexistent",
            step_order=1,
            priority=5,
            max_retries=0,
        )
        await w._execute_task(task)
        engine.execute_single_step.assert_not_called()

    @pytest.mark.asyncio
    async def test_task_with_missing_step_def(
        self, backend: InMemoryBackend, engine: MagicMock
    ) -> None:
        # Registry returns definition without step 99
        step1 = StepDefinition(name="step_a", order=1, retries=0, depends_on=[])
        definition = WorkflowDefinition(
            name="test-wf",
            version=1,
            steps={1: step1},
            config=WorkflowConfig(),
        )
        reg = MagicMock()
        reg.get.return_value = definition

        run = WorkflowRun(
            id="run-2", workflow_name="test-wf", workflow_version=1, status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run)

        w = LocalWorker("w1", backend, reg, engine, poll_interval=0.05, heartbeat_interval=60.0)
        task = PendingStep(
            id=1,
            workflow_run_id="run-2",
            step_order=99,
            priority=5,
            max_retries=0,
        )
        await w._execute_task(task)
        engine.execute_single_step.assert_not_called()

    @pytest.mark.asyncio
    async def test_task_execution_failure_with_retries(
        self, backend: InMemoryBackend, registry: MagicMock
    ) -> None:
        failing_engine = MagicMock()
        failing_engine.execute_single_step = AsyncMock(side_effect=RuntimeError("step exploded"))

        run = WorkflowRun(
            id="run-fail",
            workflow_name="test-wf",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)

        w = LocalWorker(
            "w1", backend, registry, failing_engine, poll_interval=0.05, heartbeat_interval=60.0
        )
        task = PendingStep(
            id=1,
            workflow_run_id="run-fail",
            step_order=1,
            priority=5,
            max_retries=3,
            retry_count=0,
        )
        await w._execute_task(task)
        # Should have called fail_step (with retry_at since retries remain)

    @pytest.mark.asyncio
    async def test_task_execution_failure_exhausted(
        self, backend: InMemoryBackend, registry: MagicMock
    ) -> None:
        failing_engine = MagicMock()
        failing_engine.execute_single_step = AsyncMock(side_effect=RuntimeError("final failure"))

        run = WorkflowRun(
            id="run-exhaust",
            workflow_name="test-wf",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)

        w = LocalWorker(
            "w1", backend, registry, failing_engine, poll_interval=0.05, heartbeat_interval=60.0
        )
        task = PendingStep(
            id=2,
            workflow_run_id="run-exhaust",
            step_order=1,
            priority=5,
            max_retries=1,
            retry_count=1,
        )
        await w._execute_task(task)
        # Should have sent to DLQ and failed the workflow

    @pytest.mark.asyncio
    async def test_task_failure_with_no_id(
        self, backend: InMemoryBackend, registry: MagicMock
    ) -> None:
        failing_engine = MagicMock()
        failing_engine.execute_single_step = AsyncMock(side_effect=RuntimeError("oops"))

        run = WorkflowRun(
            id="run-noid",
            workflow_name="test-wf",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
        )
        await backend.create_workflow_run(run)

        w = LocalWorker(
            "w1", backend, registry, failing_engine, poll_interval=0.05, heartbeat_interval=60.0
        )
        task = PendingStep(
            id=None,
            workflow_run_id="run-noid",
            step_order=1,
            priority=5,
            max_retries=0,
            retry_count=0,
        )
        await w._execute_task(task)  # should not raise


class TestLocalWorkerHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_loop(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=0.05
        )
        await w.start()
        await asyncio.sleep(0.15)  # let some heartbeats fire
        await w.stop(drain=False)

    @pytest.mark.asyncio
    async def test_heartbeat_error_recovery(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        w = LocalWorker(
            "w1", backend, registry, engine, poll_interval=0.05, heartbeat_interval=0.05
        )
        call_count = 0
        original_hb = backend.worker_heartbeat

        async def flaky_hb(wid: str, current_task: str | None = None) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("heartbeat fail")
            return await original_hb(wid, current_task)

        backend.worker_heartbeat = flaky_hb  # type: ignore[method-assign]
        await w.start()
        await asyncio.sleep(0.2)
        await w.stop(drain=False)
        assert call_count >= 2
