"""Tests for LocalWorker."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    PendingStep,
    StepDefinition,
    StepStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.workers.local import LocalWorker


def _make_engine(backend: InMemoryBackend) -> ExecutionEngine:
    registry = WorkflowRegistry()
    return ExecutionEngine(registry, backend)


def _make_worker(
    backend: InMemoryBackend,
    registry: WorkflowRegistry | None = None,
    engine: ExecutionEngine | None = None,
    **kw: Any,
) -> LocalWorker:
    reg = registry or WorkflowRegistry()
    eng = engine or ExecutionEngine(reg, backend)
    return LocalWorker(
        worker_id="test-worker",
        backend=backend,
        registry=reg,
        execution_engine=eng,
        poll_interval=0.01,
        max_idle_backoff=0.05,
        heartbeat_interval=60.0,
        **kw,
    )


class TestLocalWorkerLifecycle:
    @pytest.mark.asyncio
    async def test_start_registers_worker(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend)
        await worker.start()
        assert worker.is_running
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "test-worker"
        await worker.stop(drain=False)

    @pytest.mark.asyncio
    async def test_stop_deregisters_worker(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend)
        await worker.start()
        await worker.stop(drain=False)
        assert not worker.is_running
        workers = await backend.list_workers()
        assert len(workers) == 0

    @pytest.mark.asyncio
    async def test_stop_drain_waits_for_active(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend)
        await worker.start()
        await worker.stop(drain=True)
        assert worker.active_task_count == 0


class TestLocalWorkerExecution:
    @pytest.mark.asyncio
    async def test_worker_claims_and_executes(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()

        async def my_step() -> str:
            return "done"

        defn = WorkflowDefinition(
            name="test-wf",
            version=1,
            steps={1: StepDefinition(order=1, name="s1", function=my_step)},
            config=WorkflowConfig(),
        )
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(id="run-1", workflow_name="test-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-1", WorkflowStatus.RUNNING)
        await backend.enqueue_step(
            PendingStep(workflow_run_id="run-1", step_order=1, max_retries=0)
        )

        worker = _make_worker(backend, registry=registry, engine=engine)
        await worker.start()
        await asyncio.sleep(0.15)
        await worker.stop(drain=True)

        final = await backend.get_workflow_run("run-1")
        assert final is not None
        assert final.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_worker_adaptive_backoff(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend)
        await worker.start()
        await asyncio.sleep(0.1)
        await worker.stop(drain=False)
        assert not worker.is_running

    @pytest.mark.asyncio
    async def test_worker_bounded_concurrency(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend, max_concurrent=2)
        assert worker._semaphore._value == 2
        await worker.start()
        await worker.stop(drain=False)

    @pytest.mark.asyncio
    async def test_worker_heartbeat_updates(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        worker = _make_worker(backend)
        worker._heartbeat_interval = 0.05
        await worker.start()
        await asyncio.sleep(0.15)
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].last_heartbeat is not None
        await worker.stop(drain=False)

    @pytest.mark.asyncio
    async def test_worker_failed_task_dlq(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()

        async def bad_step() -> str:
            raise RuntimeError("boom")

        defn = WorkflowDefinition(
            name="fail-wf",
            version=1,
            steps={1: StepDefinition(order=1, name="bad", function=bad_step)},
            config=WorkflowConfig(),
        )
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(id="run-fail", workflow_name="fail-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-fail", WorkflowStatus.RUNNING)
        await backend.enqueue_step(
            PendingStep(workflow_run_id="run-fail", step_order=1, max_retries=0)
        )

        worker = _make_worker(backend, registry=registry, engine=engine)
        await worker.start()
        await asyncio.sleep(0.15)
        await worker.stop(drain=True)

        dlq = await backend.list_dlq()
        assert len(dlq) >= 1
        assert dlq[0].workflow_run_id == "run-fail"

    @pytest.mark.asyncio
    async def test_worker_enqueues_next_steps(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()

        async def step_a() -> str:
            return "a"

        async def step_b(_prev_output: Any = None) -> str:
            return "b"

        defn = WorkflowDefinition(
            name="chain-wf",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=step_a),
                2: StepDefinition(order=2, name="s2", function=step_b, depends_on=[1]),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(id="run-chain", workflow_name="chain-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-chain", WorkflowStatus.RUNNING)
        await backend.enqueue_step(
            PendingStep(workflow_run_id="run-chain", step_order=1, max_retries=0)
        )

        worker = _make_worker(backend, registry=registry, engine=engine)
        await worker.start()
        await asyncio.sleep(0.3)
        await worker.stop(drain=True)

        final = await backend.get_workflow_run("run-chain")
        assert final is not None
        assert final.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_worker_failed_task_retry(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()

        call_count = 0

        async def flaky_step() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("flaky")
            return "ok"

        defn = WorkflowDefinition(
            name="retry-wf",
            version=1,
            steps={1: StepDefinition(order=1, name="flaky", function=flaky_step, retries=2)},
            config=WorkflowConfig(),
        )
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(id="run-retry", workflow_name="retry-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-retry", WorkflowStatus.RUNNING)
        await backend.enqueue_step(
            PendingStep(workflow_run_id="run-retry", step_order=1, max_retries=2)
        )

        worker = _make_worker(backend, registry=registry, engine=engine)
        await worker.start()
        await asyncio.sleep(0.3)
        await worker.stop(drain=True)

        pending = [s for s in backend._pending_steps.values() if s.workflow_run_id == "run-retry"]
        assert any(s.retry_count > 0 or s.status == StepStatus.COMPLETED for s in pending)


class TestLocalWorkerGapFill:
    """Gap-fill tests for LocalWorker edge cases."""

    @pytest.mark.asyncio
    async def test_stop_without_start(self) -> None:
        """Stopping a worker that was never started should not raise."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        engine = ExecutionEngine(registry, backend)
        worker = _make_worker(backend, registry=registry, engine=engine)
        await worker.stop()

    @pytest.mark.asyncio
    async def test_worker_id_set(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        engine = ExecutionEngine(registry, backend)
        worker = _make_worker(backend, registry=registry, engine=engine)
        assert worker._worker_id is not None
