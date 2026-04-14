"""Tests for backends.sqlite — SQLiteBackend coverage gaps.

Covers: lifecycle, CRUD for workflow runs, steps, signals, compensations,
schedules, locks, DLQ, workers, pending steps, parallel results, concurrency.
"""

from __future__ import annotations

import pytest

from gravtory.backends.sqlite import SQLiteBackend, _parse_dt, _parse_path
from gravtory.core.types import (
    Compensation,
    DLQEntry,
    PendingStep,
    Schedule,
    ScheduleType,
    Signal,
    SignalWait,
    StepOutput,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
    WorkflowRun,
    WorkflowStatus,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore:SQLiteBackend is intended for development/testing only:UserWarning"
)


@pytest.fixture
async def db() -> SQLiteBackend:
    backend = SQLiteBackend("sqlite://:memory:")
    await backend.initialize()
    yield backend
    await backend.close()


class TestParsePath:
    def test_sqlite_triple_slash(self) -> None:
        assert _parse_path("sqlite:///my.db") == "my.db"

    def test_sqlite_double_slash_memory(self) -> None:
        assert _parse_path("sqlite://:memory:") == ":memory:"

    def test_sqlite_double_slash_path(self) -> None:
        assert _parse_path("sqlite://some.db") == "some.db"

    def test_plain_path(self) -> None:
        assert _parse_path("/tmp/my.db") == "/tmp/my.db"


class TestParseDt:
    def test_none(self) -> None:
        assert _parse_dt(None) is None

    def test_datetime_passthrough(self) -> None:
        from datetime import datetime, timezone

        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert _parse_dt(dt) is dt

    def test_iso_with_tz(self) -> None:
        result = _parse_dt("2025-01-01T12:00:00+00:00")
        assert result is not None
        assert result.year == 2025

    def test_iso_without_tz(self) -> None:
        result = _parse_dt("2025-01-01T12:00:00")
        assert result is not None
        assert result.tzinfo is not None  # Should be set to UTC

    def test_space_format(self) -> None:
        result = _parse_dt("2025-01-01 12:00:00")
        assert result is not None

    def test_unparseable(self) -> None:
        assert _parse_dt("not-a-date") is None


class TestSQLiteLifecycle:
    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self) -> None:
        backend = SQLiteBackend("sqlite://:memory:")
        await backend.initialize()
        assert await backend.health_check() is True
        await backend.close()

    @pytest.mark.asyncio
    async def test_health_check_after_close(self) -> None:
        backend = SQLiteBackend("sqlite://:memory:")
        await backend.initialize()
        await backend.close()
        assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_double_initialize(self) -> None:
        backend = SQLiteBackend("sqlite://:memory:")
        await backend.initialize()
        await backend.initialize()  # Should be idempotent
        assert await backend.health_check() is True
        await backend.close()


class TestWorkflowRunsCRUD:
    @pytest.mark.asyncio
    async def test_create_and_get(self, db: SQLiteBackend) -> None:
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await db.create_workflow_run(run)
        result = await db.get_workflow_run("run-1")
        assert result is not None
        assert result.id == "run-1"
        assert result.status == WorkflowStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, db: SQLiteBackend) -> None:
        result = await db.get_workflow_run("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_status_completed(self, db: SQLiteBackend) -> None:
        run = WorkflowRun(id="run-2", workflow_name="wf", status=WorkflowStatus.PENDING)
        await db.create_workflow_run(run)
        await db.update_workflow_status("run-2", WorkflowStatus.COMPLETED)
        result = await db.get_workflow_run("run-2")
        assert result is not None
        assert result.status == WorkflowStatus.COMPLETED
        assert result.completed_at is not None

    @pytest.mark.asyncio
    async def test_update_status_failed_with_error(self, db: SQLiteBackend) -> None:
        run = WorkflowRun(id="run-3", workflow_name="wf", status=WorkflowStatus.RUNNING)
        await db.create_workflow_run(run)
        await db.update_workflow_status(
            "run-3",
            WorkflowStatus.FAILED,
            error_message="boom",
            error_traceback="traceback...",
        )
        result = await db.get_workflow_run("run-3")
        assert result is not None
        assert result.status == WorkflowStatus.FAILED
        assert result.error_message == "boom"

    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, db: SQLiteBackend) -> None:
        for i in range(3):
            run = WorkflowRun(
                id=f"run-{i}",
                workflow_name="wf",
                status=WorkflowStatus.COMPLETED if i < 2 else WorkflowStatus.FAILED,
            )
            await db.create_workflow_run(run)
        all_runs = await db.list_workflow_runs()
        assert len(all_runs) == 3

        completed = await db.list_workflow_runs(status=WorkflowStatus.COMPLETED)
        assert len(completed) == 2

        named = await db.list_workflow_runs(workflow_name="wf")
        assert len(named) == 3

    @pytest.mark.asyncio
    async def test_count_workflow_runs(self, db: SQLiteBackend) -> None:
        for i in range(3):
            run = WorkflowRun(
                id=f"cnt-{i}",
                workflow_name="wf",
                status=WorkflowStatus.COMPLETED if i < 2 else WorkflowStatus.FAILED,
            )
            await db.create_workflow_run(run)
        assert await db.count_workflow_runs() == 3
        assert await db.count_workflow_runs(status=WorkflowStatus.COMPLETED) == 2
        assert await db.count_workflow_runs(workflow_name="wf") == 3

    @pytest.mark.asyncio
    async def test_get_incomplete_runs(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="inc-1", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        await db.create_workflow_run(
            WorkflowRun(id="inc-2", workflow_name="wf", status=WorkflowStatus.COMPLETED)
        )
        incomplete = await db.get_incomplete_runs()
        assert len(incomplete) == 1
        assert incomplete[0].id == "inc-1"


class TestStepOutputsCRUD:
    @pytest.mark.asyncio
    async def test_save_and_get_step_output(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="step-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        so = StepOutput(
            workflow_run_id="step-run",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=100,
        )
        await db.save_step_output(so)

        result = await db.get_step_output("step-run", 1)
        assert result is not None
        assert result.step_name == "s1"

    @pytest.mark.asyncio
    async def test_get_step_outputs(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="so-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        for i in range(3):
            await db.save_step_output(
                StepOutput(
                    workflow_run_id="so-run",
                    step_order=i + 1,
                    step_name=f"s{i + 1}",
                    status=StepStatus.COMPLETED,
                )
            )
        results = await db.get_step_outputs("so-run")
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_get_step_output_nonexistent(self, db: SQLiteBackend) -> None:
        result = await db.get_step_output("nope", 1)
        assert result is None


class TestPendingSteps:
    @pytest.mark.asyncio
    async def test_enqueue_and_claim(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="pend-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        ps = PendingStep(
            workflow_run_id="pend-run",
            step_order=1,
            priority=5,
            max_retries=3,
        )
        await db.enqueue_step(ps)
        claimed = await db.claim_step("worker-1")
        assert claimed is not None
        assert claimed.workflow_run_id == "pend-run"
        assert claimed.worker_id == "worker-1"

    @pytest.mark.asyncio
    async def test_claim_empty(self, db: SQLiteBackend) -> None:
        result = await db.claim_step("worker-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_step(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="comp-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        ps = PendingStep(
            workflow_run_id="comp-run",
            step_order=1,
            priority=5,
            max_retries=3,
        )
        await db.enqueue_step(ps)
        claimed = await db.claim_step("worker-1")
        assert claimed is not None

        output = StepOutput(
            workflow_run_id="comp-run",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=50,
        )
        await db.complete_step(claimed.id, output)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self, db: SQLiteBackend) -> None:
        from datetime import datetime, timezone

        await db.create_workflow_run(
            WorkflowRun(id="fail-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        ps = PendingStep(
            workflow_run_id="fail-run",
            step_order=1,
            priority=5,
            max_retries=3,
        )
        await db.enqueue_step(ps)
        claimed = await db.claim_step("worker-1")
        assert claimed is not None

        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        await db.fail_step(claimed.id, error_message="boom", retry_at=retry_at)

    @pytest.mark.asyncio
    async def test_fail_step_no_retry(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="failnr-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        ps = PendingStep(
            workflow_run_id="failnr-run",
            step_order=1,
            priority=5,
            max_retries=0,
        )
        await db.enqueue_step(ps)
        claimed = await db.claim_step("worker-1")
        assert claimed is not None
        await db.fail_step(claimed.id, error_message="fatal")


class TestSignals:
    @pytest.mark.asyncio
    async def test_send_and_consume(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="sig-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        sig = Signal(
            workflow_run_id="sig-run",
            signal_name="approval",
            signal_data=b'{"ok": true}',
        )
        await db.send_signal(sig)

        result = await db.consume_signal("sig-run", "approval")
        assert result is not None
        assert result.consumed is True

        # Second consume should return None
        result2 = await db.consume_signal("sig-run", "approval")
        assert result2 is None

    @pytest.mark.asyncio
    async def test_register_signal_wait(self, db: SQLiteBackend) -> None:
        from datetime import datetime, timezone

        await db.create_workflow_run(
            WorkflowRun(id="wait-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        wait = SignalWait(
            workflow_run_id="wait-run",
            signal_name="go",
            timeout_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        await db.register_signal_wait(wait)


class TestCompensations:
    @pytest.mark.asyncio
    async def test_save_and_get(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="comp-run2", workflow_name="wf", status=WorkflowStatus.COMPENSATING)
        )
        comp = Compensation(
            workflow_run_id="comp-run2",
            step_order=1,
            handler_name="undo_charge",
            step_output=b"data",
            status="pending",
        )
        await db.save_compensation(comp)
        results = await db.get_compensations("comp-run2")
        assert len(results) == 1
        assert results[0].handler_name == "undo_charge"

    @pytest.mark.asyncio
    async def test_update_compensation_status(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="comp-upd", workflow_name="wf", status=WorkflowStatus.COMPENSATING)
        )
        comp = Compensation(
            workflow_run_id="comp-upd",
            step_order=1,
            handler_name="undo",
            step_output=b"data",
            status="pending",
        )
        await db.save_compensation(comp)
        comps = await db.get_compensations("comp-upd")
        await db.update_compensation_status(comps[0].id, "completed")


class TestSchedules:
    @pytest.mark.asyncio
    async def test_save_and_list(self, db: SQLiteBackend) -> None:
        sched = Schedule(
            id="sched-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        await db.save_schedule(sched)
        all_scheds = await db.list_all_schedules()
        assert len(all_scheds) == 1

    @pytest.mark.asyncio
    async def test_get_due_schedules(self, db: SQLiteBackend) -> None:
        from datetime import datetime, timezone

        sched = Schedule(
            id="due-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="* * * * *",
            enabled=True,
            next_run_at=datetime(2020, 1, 1, tzinfo=timezone.utc),  # In the past
        )
        await db.save_schedule(sched)
        due = await db.get_due_schedules()
        assert len(due) == 1

    @pytest.mark.asyncio
    async def test_update_schedule_last_run(self, db: SQLiteBackend) -> None:
        from datetime import datetime, timezone

        sched = Schedule(
            id="upd-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="* * * * *",
            enabled=True,
        )
        await db.save_schedule(sched)
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        nxt = datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc)
        await db.update_schedule_last_run("upd-1", now, nxt)

    @pytest.mark.asyncio
    async def test_get_all_enabled(self, db: SQLiteBackend) -> None:
        await db.save_schedule(
            Schedule(
                id="en-1",
                workflow_name="wf",
                schedule_type=ScheduleType.CRON,
                schedule_config="*",
                enabled=True,
            )
        )
        await db.save_schedule(
            Schedule(
                id="dis-1",
                workflow_name="wf2",
                schedule_type=ScheduleType.CRON,
                schedule_config="*",
                enabled=False,
            )
        )
        enabled = await db.get_all_enabled_schedules()
        assert len(enabled) == 1


class TestLocks:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, db: SQLiteBackend) -> None:
        assert await db.acquire_lock("my-lock", "holder-1", 60) is True
        assert await db.release_lock("my-lock", "holder-1") is True

    @pytest.mark.asyncio
    async def test_acquire_conflict(self, db: SQLiteBackend) -> None:
        assert await db.acquire_lock("lock-a", "holder-1", 60) is True
        assert await db.acquire_lock("lock-a", "holder-2", 60) is False

    @pytest.mark.asyncio
    async def test_acquire_same_holder(self, db: SQLiteBackend) -> None:
        assert await db.acquire_lock("lock-b", "holder-1", 60) is True
        assert await db.acquire_lock("lock-b", "holder-1", 60) is True

    @pytest.mark.asyncio
    async def test_refresh_lock(self, db: SQLiteBackend) -> None:
        assert await db.acquire_lock("lock-c", "holder-1", 60) is True
        assert await db.refresh_lock("lock-c", "holder-1", 120) is True

    @pytest.mark.asyncio
    async def test_release_nonexistent(self, db: SQLiteBackend) -> None:
        assert await db.release_lock("nolock", "noholder") is False


class TestDLQ:
    @pytest.mark.asyncio
    async def test_add_list_remove(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="dlq-run", workflow_name="wf", status=WorkflowStatus.FAILED)
        )
        entry = DLQEntry(
            workflow_run_id="dlq-run",
            step_order=1,
            error_message="boom",
            error_traceback="tb",
        )
        await db.add_to_dlq(entry)
        entries = await db.list_dlq()
        assert len(entries) == 1
        await db.remove_from_dlq(entries[0].id)
        assert len(await db.list_dlq()) == 0

    @pytest.mark.asyncio
    async def test_purge_dlq(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="purge-run", workflow_name="wf", status=WorkflowStatus.FAILED)
        )
        for i in range(3):
            await db.add_to_dlq(
                DLQEntry(
                    workflow_run_id="purge-run",
                    step_order=i,
                    error_message=f"err-{i}",
                )
            )
        count = await db.purge_dlq()
        assert count == 3
        assert len(await db.list_dlq()) == 0


class TestWorkers:
    @pytest.mark.asyncio
    async def test_register_and_list(self, db: SQLiteBackend) -> None:
        w = WorkerInfo(worker_id="w-1", node_id="node-1", status=WorkerStatus.ACTIVE)
        await db.register_worker(w)
        workers = await db.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "w-1"

    @pytest.mark.asyncio
    async def test_heartbeat_with_task(self, db: SQLiteBackend) -> None:
        w = WorkerInfo(worker_id="w-2", node_id="node-1", status=WorkerStatus.ACTIVE)
        await db.register_worker(w)
        await db.worker_heartbeat("w-2", current_task="run-123")

    @pytest.mark.asyncio
    async def test_heartbeat_without_task(self, db: SQLiteBackend) -> None:
        w = WorkerInfo(worker_id="w-3", node_id="node-1", status=WorkerStatus.ACTIVE)
        await db.register_worker(w)
        await db.worker_heartbeat("w-3")

    @pytest.mark.asyncio
    async def test_deregister(self, db: SQLiteBackend) -> None:
        w = WorkerInfo(worker_id="w-4", node_id="node-1", status=WorkerStatus.ACTIVE)
        await db.register_worker(w)
        await db.deregister_worker("w-4")
        assert len(await db.list_workers()) == 0

    @pytest.mark.asyncio
    async def test_get_stale_workers(self, db: SQLiteBackend) -> None:
        w = WorkerInfo(worker_id="w-5", node_id="node-1", status=WorkerStatus.ACTIVE)
        await db.register_worker(w)
        # With a threshold of 0, any worker is stale
        stale = await db.get_stale_workers(0)
        # Result depends on timing but should not error
        assert isinstance(stale, list)


class TestParallelResults:
    @pytest.mark.asyncio
    async def test_checkpoint_and_get(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="par-run", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        await db.checkpoint_parallel_item("par-run", 1, 0, b"result-0")
        await db.checkpoint_parallel_item("par-run", 1, 1, b"result-1")
        results = await db.get_parallel_results("par-run", 1)
        assert len(results) == 2
        assert results[0] == b"result-0"
        assert results[1] == b"result-1"


class TestConcurrencyLimit:
    @pytest.mark.asyncio
    async def test_check_concurrency_limit(self, db: SQLiteBackend) -> None:
        await db.create_workflow_run(
            WorkflowRun(id="conc-1", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        assert await db.check_concurrency_limit("wf", "default", 2) is True
        await db.create_workflow_run(
            WorkflowRun(id="conc-2", workflow_name="wf", status=WorkflowStatus.RUNNING)
        )
        assert await db.check_concurrency_limit("wf", "default", 2) is False
