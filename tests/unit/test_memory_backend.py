"""Unit tests for the InMemoryBackend."""

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    DLQEntry,
    PendingStep,
    Signal,
    StepOutput,
    StepStatus,
    WorkerInfo,
    WorkflowRun,
    WorkflowStatus,
)


@pytest.fixture
def backend() -> InMemoryBackend:
    return InMemoryBackend()


class TestWorkflowRuns:
    @pytest.mark.asyncio
    async def test_create_and_get(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        result = await backend.get_workflow_run("run-1")
        assert result is not None
        assert result.id == "run-1"
        assert result.workflow_name == "test"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        result = await backend.get_workflow_run("nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_status(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-1", WorkflowStatus.COMPLETED)
        result = await backend.get_workflow_run("run-1")
        assert result is not None
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_list_by_status(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        await backend.create_workflow_run(WorkflowRun(id="r1", workflow_name="a"))
        await backend.create_workflow_run(WorkflowRun(id="r2", workflow_name="b"))
        await backend.update_workflow_status("r1", WorkflowStatus.COMPLETED)
        runs = await backend.list_workflow_runs(status=WorkflowStatus.COMPLETED)
        assert len(runs) == 1
        assert runs[0].id == "r1"

    @pytest.mark.asyncio
    async def test_idempotent_create(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        await backend.create_workflow_run(run)  # should not raise
        result = await backend.get_workflow_run("run-1")
        assert result is not None


class TestStepOutputs:
    @pytest.mark.asyncio
    async def test_checkpoint_step(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        output = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(output)
        result = await backend.get_step_output("run-1", 1)
        assert result is not None
        assert result.step_name == "charge"

    @pytest.mark.asyncio
    async def test_checkpoint_idempotent(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        output = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(output)
        await backend.save_step_output(output)  # idempotent — no overwrite
        outputs = await backend.get_step_outputs("run-1")
        assert len(outputs) == 1

    @pytest.mark.asyncio
    async def test_get_all_step_outputs(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        run = WorkflowRun(id="run-1", workflow_name="test")
        await backend.create_workflow_run(run)
        for i in range(1, 4):
            await backend.save_step_output(
                StepOutput(workflow_run_id="run-1", step_order=i, step_name=f"s{i}")
            )
        outputs = await backend.get_step_outputs("run-1")
        assert len(outputs) == 3
        assert [o.step_order for o in outputs] == [1, 2, 3]


class TestPendingSteps:
    @pytest.mark.asyncio
    async def test_claim_pending_step(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        step = PendingStep(workflow_run_id="run-1", step_order=1)
        await backend.enqueue_step(step)
        claimed = await backend.claim_step("worker-1")
        assert claimed is not None
        assert claimed.worker_id == "worker-1"
        assert claimed.status == StepStatus.RUNNING

    @pytest.mark.asyncio
    async def test_claim_returns_none_when_empty(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        claimed = await backend.claim_step("worker-1")
        assert claimed is None

    @pytest.mark.asyncio
    async def test_claim_exhausts_queue(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        await backend.enqueue_step(PendingStep(workflow_run_id="run-1", step_order=1))
        await backend.claim_step("w1")
        claimed = await backend.claim_step("w2")
        assert claimed is None


class TestSignals:
    @pytest.mark.asyncio
    async def test_signal_store_and_retrieve(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        sig = Signal(workflow_run_id="run-1", signal_name="approval")
        await backend.send_signal(sig)
        consumed = await backend.consume_signal("run-1", "approval")
        assert consumed is not None
        assert consumed.signal_name == "approval"
        assert consumed.consumed is True

    @pytest.mark.asyncio
    async def test_consume_nonexistent(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        result = await backend.consume_signal("run-1", "nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_signal_consumed_once(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        sig = Signal(workflow_run_id="run-1", signal_name="approval")
        await backend.send_signal(sig)
        await backend.consume_signal("run-1", "approval")
        second = await backend.consume_signal("run-1", "approval")
        assert second is None


class TestLocks:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        acquired = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert acquired is True
        released = await backend.release_lock("my-lock", "holder-1")
        assert released is True

    @pytest.mark.asyncio
    async def test_lock_contention(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        await backend.acquire_lock("my-lock", "holder-1", 60)
        contested = await backend.acquire_lock("my-lock", "holder-2", 60)
        assert contested is False

    @pytest.mark.asyncio
    async def test_release_wrong_holder(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        await backend.acquire_lock("my-lock", "holder-1", 60)
        released = await backend.release_lock("my-lock", "wrong-holder")
        assert released is False


class TestDLQ:
    @pytest.mark.asyncio
    async def test_add_and_list(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        entry = DLQEntry(workflow_run_id="run-1", error_message="boom")
        await backend.add_to_dlq(entry)
        results = await backend.list_dlq()
        assert len(results) == 1
        assert results[0].error_message == "boom"

    @pytest.mark.asyncio
    async def test_remove_from_dlq(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        entry = DLQEntry(workflow_run_id="run-1", error_message="boom")
        await backend.add_to_dlq(entry)
        entries = await backend.list_dlq()
        assert len(entries) == 1
        await backend.remove_from_dlq(entries[0].id)  # type: ignore[arg-type]
        assert len(await backend.list_dlq()) == 0


class TestWorkers:
    @pytest.mark.asyncio
    async def test_register_and_list(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        worker = WorkerInfo(worker_id="w1", node_id="node-1")
        await backend.register_worker(worker)
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "w1"

    @pytest.mark.asyncio
    async def test_deregister(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        worker = WorkerInfo(worker_id="w1", node_id="node-1")
        await backend.register_worker(worker)
        await backend.deregister_worker("w1")
        workers = await backend.list_workers()
        assert len(workers) == 0


class TestMemoryBackendGapFill:
    """Gap-fill tests for InMemoryBackend edge cases."""

    @pytest.mark.asyncio
    async def test_all_operations_comprehensive(self, backend: InMemoryBackend) -> None:
        """Full lifecycle: create run, checkpoint steps, signal, lock, DLQ."""
        await backend.initialize()

        # Create run
        run = WorkflowRun(id="comp-1", workflow_name="test")
        await backend.create_workflow_run(run)

        # Checkpoint 3 steps
        for i in range(1, 4):
            await backend.save_step_output(
                StepOutput(
                    workflow_run_id="comp-1",
                    step_order=i,
                    step_name=f"s{i}",
                    status=StepStatus.COMPLETED,
                    output_data=f"result_{i}",
                )
            )
        outputs = await backend.get_step_outputs("comp-1")
        assert len(outputs) == 3

        # Signal
        sig = Signal(workflow_run_id="comp-1", signal_name="approve")
        await backend.send_signal(sig)
        consumed = await backend.consume_signal("comp-1", "approve")
        assert consumed is not None

        # Lock
        assert await backend.acquire_lock("test-lock", "h1", 60)
        assert await backend.release_lock("test-lock", "h1")

        # DLQ
        await backend.add_to_dlq(DLQEntry(workflow_run_id="comp-1", error_message="err"))
        dlq = await backend.list_dlq()
        assert len(dlq) == 1

        # Complete
        await backend.update_workflow_status("comp-1", WorkflowStatus.COMPLETED)
        final = await backend.get_workflow_run("comp-1")
        assert final is not None
        assert final.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_concurrent_claims(self, backend: InMemoryBackend) -> None:
        """Multiple workers claiming from same queue — no duplicates."""
        await backend.initialize()
        for i in range(10):
            await backend.enqueue_step(PendingStep(workflow_run_id=f"run-{i}", step_order=1))

        claimed_ids: list[str] = []
        for w in range(10):
            c = await backend.claim_step(f"worker-{w}")
            if c is not None:
                claimed_ids.append(c.workflow_run_id)

        # Each step claimed exactly once
        assert len(claimed_ids) == 10
        assert len(set(claimed_ids)) == 10

    @pytest.mark.asyncio
    async def test_list_workflow_runs_all(self, backend: InMemoryBackend) -> None:
        """list_workflow_runs without status filter returns all runs."""
        await backend.initialize()
        for i in range(5):
            await backend.create_workflow_run(WorkflowRun(id=f"r-{i}", workflow_name="test"))
        await backend.update_workflow_status("r-0", WorkflowStatus.COMPLETED)
        await backend.update_workflow_status("r-1", WorkflowStatus.FAILED)

        all_runs = await backend.list_workflow_runs()
        assert len(all_runs) == 5


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_not_connected(self, backend: InMemoryBackend) -> None:
        assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_connected(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        assert await backend.health_check() is True

    @pytest.mark.asyncio
    async def test_after_close(self, backend: InMemoryBackend) -> None:
        await backend.initialize()
        await backend.close()
        assert await backend.health_check() is False
