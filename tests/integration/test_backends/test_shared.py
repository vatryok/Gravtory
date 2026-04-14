"""Shared integration tests — run against each backend via the parameterized fixture."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.core.types import (
    Compensation,
    DLQEntry,
    PendingStep,
    Schedule,
    ScheduleType,
    Signal,
    StepOutput,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
    WorkflowRun,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Schema ────────────────────────────────────────────────────────


class TestSchemaCreation:
    @pytest.mark.asyncio
    async def test_tables_exist(self, backend: Backend) -> None:
        """initialize() creates tables without error."""
        assert await backend.health_check()

    @pytest.mark.asyncio
    async def test_idempotent_initialize(self, backend: Backend) -> None:
        """Calling initialize() twice does not raise."""
        await backend.initialize()
        assert await backend.health_check()


# ── Workflow runs ─────────────────────────────────────────────────


class TestWorkflowRunCRUD:
    @pytest.mark.asyncio
    async def test_create_and_get(self, backend: Backend) -> None:
        run = WorkflowRun(id="wf-1", workflow_name="TestWF")
        await backend.create_workflow_run(run)
        got = await backend.get_workflow_run("wf-1")
        assert got is not None
        assert got.id == "wf-1"
        assert got.workflow_name == "TestWF"
        assert got.status == WorkflowStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_not_found(self, backend: Backend) -> None:
        got = await backend.get_workflow_run("does-not-exist")
        assert got is None

    @pytest.mark.asyncio
    async def test_update_status(self, backend: Backend) -> None:
        run = WorkflowRun(id="wf-upd", workflow_name="TestWF")
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("wf-upd", WorkflowStatus.RUNNING)
        got = await backend.get_workflow_run("wf-upd")
        assert got is not None
        assert got.status == WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_update_status_with_error(self, backend: Backend) -> None:
        run = WorkflowRun(id="wf-err", workflow_name="TestWF")
        await backend.create_workflow_run(run)
        await backend.update_workflow_status(
            "wf-err",
            WorkflowStatus.FAILED,
            error_message="boom",
            error_traceback="Traceback ...",
        )
        got = await backend.get_workflow_run("wf-err")
        assert got is not None
        assert got.status == WorkflowStatus.FAILED
        assert got.error_message == "boom"
        assert got.completed_at is not None

    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, backend: Backend) -> None:
        for i in range(3):
            await backend.create_workflow_run(WorkflowRun(id=f"list-{i}", workflow_name="ListWF"))
        runs = await backend.list_workflow_runs()
        assert len(runs) == 3

    @pytest.mark.asyncio
    async def test_list_with_status_filter(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="f1", workflow_name="WF"))
        await backend.create_workflow_run(WorkflowRun(id="f2", workflow_name="WF"))
        await backend.update_workflow_status("f1", WorkflowStatus.COMPLETED)
        runs = await backend.list_workflow_runs(status=WorkflowStatus.COMPLETED)
        assert len(runs) == 1
        assert runs[0].id == "f1"


# ── Step outputs ──────────────────────────────────────────────────


class TestStepCheckpoint:
    @pytest.mark.asyncio
    async def test_checkpoint_and_get(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="cp-1", workflow_name="WF"))
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="cp-1",
                step_order=1,
                step_name="s1",
                output_data=b"hello",
                status=StepStatus.COMPLETED,
                duration_ms=42,
            )
        )
        out = await backend.get_step_output("cp-1", 1)
        assert out is not None
        assert out.step_name == "s1"
        assert out.output_data == b"hello"
        assert out.duration_ms == 42

    @pytest.mark.asyncio
    async def test_checkpoint_idempotent(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="cp-idem", workflow_name="WF"))
        so = StepOutput(
            workflow_run_id="cp-idem",
            step_order=1,
            step_name="s1",
            output_data=b"first",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(so)
        # Second write with different data — should be no-op
        so2 = StepOutput(
            workflow_run_id="cp-idem",
            step_order=1,
            step_name="s1",
            output_data=b"second",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(so2)
        out = await backend.get_step_output("cp-idem", 1)
        assert out is not None
        assert out.output_data == b"first"

    @pytest.mark.asyncio
    async def test_get_completed_steps(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="cp-all", workflow_name="WF"))
        for i in range(1, 4):
            await backend.save_step_output(
                StepOutput(
                    workflow_run_id="cp-all",
                    step_order=i,
                    step_name=f"s{i}",
                    status=StepStatus.COMPLETED,
                )
            )
        outputs = await backend.get_step_outputs("cp-all")
        assert len(outputs) == 3
        assert [o.step_order for o in outputs] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_checkpoint_updates_current_step(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="cp-cur", workflow_name="WF"))
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="cp-cur", step_order=2, step_name="s2", status=StepStatus.COMPLETED
            )
        )
        run = await backend.get_workflow_run("cp-cur")
        assert run is not None
        assert run.current_step == 2


# ── Pending steps ─────────────────────────────────────────────────


class TestPendingStepLifecycle:
    @pytest.mark.asyncio
    async def test_enqueue_claim_complete(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="ps-1", workflow_name="WF"))
        await backend.enqueue_step(PendingStep(workflow_run_id="ps-1", step_order=1, priority=5))
        claimed = await backend.claim_step("worker-1")
        assert claimed is not None
        assert claimed.workflow_run_id == "ps-1"
        assert claimed.step_order == 1

    @pytest.mark.asyncio
    async def test_claim_returns_none_empty(self, backend: Backend) -> None:
        claimed = await backend.claim_step("worker-1")
        assert claimed is None

    @pytest.mark.asyncio
    async def test_claim_respects_priority(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="ps-pri", workflow_name="WF"))
        await backend.enqueue_step(PendingStep(workflow_run_id="ps-pri", step_order=1, priority=1))
        await backend.enqueue_step(PendingStep(workflow_run_id="ps-pri", step_order=2, priority=10))
        claimed = await backend.claim_step("worker-1")
        assert claimed is not None
        assert claimed.step_order == 2  # Higher priority claimed first


# ── Signals ───────────────────────────────────────────────────────


class TestSignals:
    @pytest.mark.asyncio
    async def test_signal_store_and_retrieve(self, backend: Backend) -> None:
        sig = Signal(workflow_run_id="sig-run", signal_name="payment_received", signal_data=b"ok")
        await backend.send_signal(sig)
        consumed = await backend.consume_signal("sig-run", "payment_received")
        assert consumed is not None
        assert consumed.signal_name == "payment_received"
        assert consumed.consumed is True

    @pytest.mark.asyncio
    async def test_consume_returns_none_when_empty(self, backend: Backend) -> None:
        result = await backend.consume_signal("no-run", "no-signal")
        assert result is None

    @pytest.mark.asyncio
    async def test_signal_consumed_only_once(self, backend: Backend) -> None:
        sig = Signal(workflow_run_id="sig-once", signal_name="ev")
        await backend.send_signal(sig)
        first = await backend.consume_signal("sig-once", "ev")
        assert first is not None
        second = await backend.consume_signal("sig-once", "ev")
        assert second is None


# ── Locks ─────────────────────────────────────────────────────────


class TestLocks:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, backend: Backend) -> None:
        acquired = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert acquired is True
        released = await backend.release_lock("my-lock", "holder-1")
        assert released is True

    @pytest.mark.asyncio
    async def test_prevents_double_acquire(self, backend: Backend) -> None:
        await backend.acquire_lock("exclusive", "holder-A", 60)
        acquired_by_b = await backend.acquire_lock("exclusive", "holder-B", 60)
        assert acquired_by_b is False

    @pytest.mark.asyncio
    async def test_same_holder_can_reacquire(self, backend: Backend) -> None:
        await backend.acquire_lock("reentrant", "holder-X", 60)
        again = await backend.acquire_lock("reentrant", "holder-X", 60)
        assert again is True

    @pytest.mark.asyncio
    async def test_refresh_lock(self, backend: Backend) -> None:
        await backend.acquire_lock("refresh-me", "holder-1", 10)
        refreshed = await backend.refresh_lock("refresh-me", "holder-1", 120)
        assert refreshed is True

    @pytest.mark.asyncio
    async def test_lock_expires(self, backend: Backend) -> None:
        """Expired lock can be re-acquired by another holder."""
        # Acquire with 0-second TTL (immediately expired)
        await backend.acquire_lock("expiring", "holder-A", 0)
        # Another holder should be able to acquire the expired lock
        acquired = await backend.acquire_lock("expiring", "holder-B", 60)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_release_wrong_holder(self, backend: Backend) -> None:
        await backend.acquire_lock("owned", "holder-A", 60)
        released = await backend.release_lock("owned", "holder-B")
        assert released is False


# ── DLQ ───────────────────────────────────────────────────────────


class TestDLQ:
    @pytest.mark.asyncio
    async def test_add_list_remove(self, backend: Backend) -> None:
        entry = DLQEntry(
            workflow_run_id="dlq-run",
            step_order=1,
            error_message="failed",
            error_traceback="tb",
        )
        await backend.add_to_dlq(entry)
        items = await backend.list_dlq()
        assert len(items) >= 1
        assert items[0].workflow_run_id == "dlq-run"
        # Remove
        assert items[0].id is not None
        await backend.remove_from_dlq(items[0].id)
        items_after = await backend.list_dlq()
        assert len(items_after) == 0


# ── Schedules ─────────────────────────────────────────────────────


class TestSchedules:
    @pytest.mark.asyncio
    async def test_schedule_crud(self, backend: Backend) -> None:
        sched = Schedule(
            id="sched-1",
            workflow_name="WF",
            schedule_type=ScheduleType.CRON,
            schedule_config="0 * * * *",
        )
        await backend.save_schedule(sched)
        # Update
        sched.schedule_config = "*/5 * * * *"
        await backend.save_schedule(sched)


# ── Workers ───────────────────────────────────────────────────────


class TestWorkers:
    @pytest.mark.asyncio
    async def test_register_heartbeat_list_deregister(self, backend: Backend) -> None:
        worker = WorkerInfo(worker_id="w-1", node_id="node-a")
        await backend.register_worker(worker)
        await backend.worker_heartbeat("w-1")
        workers = await backend.list_workers()
        assert len(workers) >= 1
        assert any(w.worker_id == "w-1" for w in workers)
        await backend.deregister_worker("w-1")
        workers = await backend.list_workers()
        assert not any(w.worker_id == "w-1" for w in workers)


# ── Compensation ──────────────────────────────────────────────────


class TestCompensation:
    @pytest.mark.asyncio
    async def test_store_get_update(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="comp-run", workflow_name="WF"))
        comp = Compensation(
            workflow_run_id="comp-run",
            step_order=1,
            handler_name="undo_charge",
        )
        await backend.save_compensation(comp)
        comps = await backend.get_compensations("comp-run")
        assert len(comps) == 1
        assert comps[0].handler_name == "undo_charge"
        assert comps[0].id is not None
        await backend.update_compensation_status(comps[0].id, "completed")


# ── Row mapper correctness ────────────────────────────────────────


class TestRowMappers:
    @pytest.mark.asyncio
    async def test_worker_status_is_enum(self, backend: Backend) -> None:
        worker = WorkerInfo(worker_id="w-enum", node_id="n1")
        await backend.register_worker(worker)
        workers = await backend.list_workers()
        w = next(w for w in workers if w.worker_id == "w-enum")
        assert isinstance(w.status, WorkerStatus)

    @pytest.mark.asyncio
    async def test_schedule_type_is_enum(self, backend: Backend) -> None:
        sched = Schedule(
            id="sched-enum",
            workflow_name="WF",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="30s",
        )
        await backend.save_schedule(sched)

    @pytest.mark.asyncio
    async def test_compensation_status_is_enum(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="comp-enum", workflow_name="WF"))
        comp = Compensation(workflow_run_id="comp-enum", step_order=1, handler_name="undo")
        await backend.save_compensation(comp)
        comps = await backend.get_compensations("comp-enum")
        assert isinstance(comps[0].status, StepStatus)
