"""Tests for backends.postgresql — PostgreSQLBackend with mocked asyncpg pool."""

from __future__ import annotations

import pytest

pytest.importorskip("asyncpg", reason="asyncpg not installed - skipping PostgreSQL backend tests")

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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


def _mock_row(data: dict) -> MagicMock:
    """Create a mock asyncpg.Record that supports dict-style access."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: data[key]
    row.get = lambda key, default=None: data.get(key, default)
    row.keys = lambda: data.keys()
    return row


def _make_pool() -> MagicMock:
    """Create a mock asyncpg pool with acquire() as async context manager.

    Supports both ``async with pool.acquire() as conn:`` and the compound
    ``async with pool.acquire() as conn, conn.transaction():``.

    conn is a MagicMock (not AsyncMock) so that conn.transaction() is a
    synchronous call returning an async-context-manager — matching how
    asyncpg's real Connection.transaction() works.
    """
    pool = MagicMock()
    # conn must be MagicMock so .transaction() returns sync, not a coroutine
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)

    # pool.acquire() → async context manager yielding conn
    acm = MagicMock()
    acm.__aenter__ = AsyncMock(return_value=conn)
    acm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = acm
    pool.close = AsyncMock()

    # conn.transaction() → async context manager (for compound `async with`)
    txn_acm = MagicMock()
    txn_acm.__aenter__ = AsyncMock()
    txn_acm.__aexit__ = AsyncMock(return_value=False)
    conn.transaction.return_value = txn_acm

    return pool, conn


@pytest.fixture
def pg_backend():
    with patch("gravtory.backends.postgresql.asyncpg"):
        from gravtory.backends.postgresql import PostgreSQLBackend

        backend = PostgreSQLBackend("postgresql://localhost/test")
        pool, conn = _make_pool()
        backend._pool = pool
        yield backend, conn


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_initialize(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=_mock_row({"version": 1}))
        conn.execute = AsyncMock()
        await backend.initialize()

    @pytest.mark.asyncio
    async def test_close(self, pg_backend) -> None:
        backend, conn = pg_backend
        await backend.close()
        assert backend._pool is None

    @pytest.mark.asyncio
    async def test_health_check_ok(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="SELECT 1")
        assert await backend.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_no_pool(self) -> None:
        with patch("gravtory.backends.postgresql.asyncpg"):
            from gravtory.backends.postgresql import PostgreSQLBackend

            backend = PostgreSQLBackend("postgresql://localhost/test")
            assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_error(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(side_effect=Exception("connection lost"))
        assert await backend.health_check() is False


class TestWorkflowRuns:
    @pytest.mark.asyncio
    async def test_create_workflow_run(self, pg_backend) -> None:
        backend, conn = pg_backend
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        conn.execute = AsyncMock()
        await backend.create_workflow_run(run)
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_workflow_run_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": "run-1",
                "workflow_name": "wf",
                "workflow_version": 1,
                "namespace": "default",
                "status": "pending",
                "current_step": None,
                "input_data": None,
                "error_message": None,
                "error_traceback": None,
                "parent_run_id": None,
                "created_at": now,
                "updated_at": now,
                "completed_at": None,
                "deadline_at": None,
            }
        )
        conn.fetchrow = AsyncMock(return_value=row)
        result = await backend.get_workflow_run("run-1")
        assert result is not None
        assert result.id == "run-1"

    @pytest.mark.asyncio
    async def test_get_workflow_run_not_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=None)
        result = await backend.get_workflow_run("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_workflow_status(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.update_workflow_status(
            "run-1",
            WorkflowStatus.COMPLETED,
            error_message="done",
            output_data=b"result",
        )
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": "run-1",
                "workflow_name": "wf",
                "workflow_version": 1,
                "namespace": "default",
                "status": "completed",
                "current_step": 1,
                "input_data": None,
                "error_message": None,
                "error_traceback": None,
                "parent_run_id": None,
                "created_at": now,
                "updated_at": now,
                "completed_at": now,
                "deadline_at": None,
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.list_workflow_runs()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_with_filters(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetch = AsyncMock(return_value=[])
        result = await backend.list_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_count_workflow_runs(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=_mock_row({"cnt": 5}))
        result = await backend.count_workflow_runs()
        assert result == 5

    @pytest.mark.asyncio
    async def test_count_with_filters(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=_mock_row({"cnt": 2}))
        result = await backend.count_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert result == 2

    @pytest.mark.asyncio
    async def test_get_incomplete_runs(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": "run-1",
                "workflow_name": "wf",
                "workflow_version": 1,
                "namespace": "default",
                "status": "running",
                "current_step": None,
                "input_data": None,
                "error_message": None,
                "error_traceback": None,
                "parent_run_id": None,
                "created_at": now,
                "updated_at": now,
                "completed_at": None,
                "deadline_at": None,
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.get_incomplete_runs()
        assert len(result) == 1


class TestStepOutputs:
    @pytest.mark.asyncio
    async def test_save_step_output(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=100,
        )
        await backend.save_step_output(so)

    @pytest.mark.asyncio
    async def test_get_step_outputs(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "step_name": "s1",
                "output_data": None,
                "output_type": None,
                "duration_ms": 100,
                "retry_count": 0,
                "status": "completed",
                "error_message": None,
                "created_at": now,
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.get_step_outputs("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_step_output_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "step_name": "s1",
                "output_data": None,
                "output_type": None,
                "duration_ms": 50,
                "retry_count": 0,
                "status": "completed",
                "error_message": None,
                "created_at": now,
            }
        )
        conn.fetchrow = AsyncMock(return_value=row)
        result = await backend.get_step_output("run-1", 1)
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_step_output_not_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=None)
        result = await backend.get_step_output("run-1", 99)
        assert result is None


class TestPendingSteps:
    @pytest.mark.asyncio
    async def test_enqueue_step(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        ps = PendingStep(workflow_run_id="run-1", step_order=1, priority=5, max_retries=3)
        await backend.enqueue_step(ps)
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_claim_step_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "priority": 5,
                "status": "running",
                "worker_id": "w-1",
                "retry_count": 0,
                "max_retries": 3,
            }
        )
        conn.fetchrow = AsyncMock(return_value=row)
        result = await backend.claim_step("w-1")
        assert result is not None
        assert result.worker_id == "w-1"

    @pytest.mark.asyncio
    async def test_claim_step_empty(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=None)
        result = await backend.claim_step("w-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_step(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
        )
        await backend.complete_step(1, so)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        await backend.fail_step(1, error_message="boom", retry_at=retry_at)

    @pytest.mark.asyncio
    async def test_fail_step_no_retry(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.fail_step(1, error_message="fatal")


class TestSignals:
    @pytest.mark.asyncio
    async def test_send_signal(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        sig = Signal(workflow_run_id="run-1", signal_name="go", signal_data=b"data")
        await backend.send_signal(sig)

    @pytest.mark.asyncio
    async def test_consume_signal_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "signal_name": "go",
                "signal_data": b"data",
                "consumed": True,
                "created_at": now,
            }
        )
        conn.fetchrow = AsyncMock(return_value=row)
        result = await backend.consume_signal("run-1", "go")
        assert result is not None

    @pytest.mark.asyncio
    async def test_consume_signal_not_found(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=None)
        result = await backend.consume_signal("run-1", "go")
        assert result is None

    @pytest.mark.asyncio
    async def test_register_signal_wait(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        wait = SignalWait(
            workflow_run_id="run-1",
            signal_name="go",
            timeout_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        await backend.register_signal_wait(wait)


class TestCompensation:
    @pytest.mark.asyncio
    async def test_save_compensation(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        comp = Compensation(
            workflow_run_id="run-1",
            step_order=1,
            handler_name="undo",
            step_output=b"data",
            status="pending",
        )
        await backend.save_compensation(comp)

    @pytest.mark.asyncio
    async def test_get_compensations(self, pg_backend) -> None:
        backend, conn = pg_backend
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "handler_name": "undo",
                "step_output": b"data",
                "status": "completed",
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.get_compensations("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_compensation_status(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.update_compensation_status(1, "completed", error_message="ok")


class TestScheduling:
    @pytest.mark.asyncio
    async def test_save_schedule(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        sched = Schedule(
            id="s-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        await backend.save_schedule(sched)

    @pytest.mark.asyncio
    async def test_get_due_schedules(self, pg_backend) -> None:
        backend, conn = pg_backend
        now = datetime.now(tz=timezone.utc)
        row = _mock_row(
            {
                "id": "s-1",
                "workflow_name": "wf",
                "schedule_type": "cron",
                "schedule_config": "* * * * *",
                "namespace": "default",
                "enabled": True,
                "last_run_at": None,
                "next_run_at": now,
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.get_due_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_schedule_last_run(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        now = datetime.now(tz=timezone.utc)
        await backend.update_schedule_last_run("s-1", now, now)

    @pytest.mark.asyncio
    async def test_get_all_enabled(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetch = AsyncMock(return_value=[])
        result = await backend.get_all_enabled_schedules()
        assert result == []

    @pytest.mark.asyncio
    async def test_list_all_schedules(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetch = AsyncMock(return_value=[])
        result = await backend.list_all_schedules()
        assert result == []


class TestLocks:
    @pytest.mark.asyncio
    async def test_acquire_lock_success(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="INSERT 0 1")
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_lock_conflict(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="INSERT 0 0")
        result = await backend.acquire_lock("my-lock", "holder-2", 60)
        assert result is False

    @pytest.mark.asyncio
    async def test_release_lock(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="DELETE 1")
        result = await backend.release_lock("my-lock", "holder-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_refresh_lock(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="UPDATE 1")
        result = await backend.refresh_lock("my-lock", "holder-1", 120)
        assert result is True


class TestDLQ:
    @pytest.mark.asyncio
    async def test_add_to_dlq(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        entry = DLQEntry(workflow_run_id="run-1", step_order=1, error_message="boom")
        await backend.add_to_dlq(entry)

    @pytest.mark.asyncio
    async def test_list_dlq(self, pg_backend) -> None:
        backend, conn = pg_backend
        row = _mock_row(
            {
                "id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "error_message": "boom",
                "error_traceback": None,
                "retry_count": 0,
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.list_dlq()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_remove_from_dlq(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.remove_from_dlq(1)

    @pytest.mark.asyncio
    async def test_purge_dlq(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock(return_value="DELETE 5")
        result = await backend.purge_dlq()
        assert result == 5


class TestWorkers:
    @pytest.mark.asyncio
    async def test_register_worker(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        w = WorkerInfo(worker_id="w-1", node_id="node-1", status=WorkerStatus.ACTIVE)
        await backend.register_worker(w)

    @pytest.mark.asyncio
    async def test_worker_heartbeat_with_task(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.worker_heartbeat("w-1", current_task="run-1")

    @pytest.mark.asyncio
    async def test_worker_heartbeat_no_task(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.worker_heartbeat("w-1")

    @pytest.mark.asyncio
    async def test_deregister_worker(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.deregister_worker("w-1")

    @pytest.mark.asyncio
    async def test_list_workers(self, pg_backend) -> None:
        backend, conn = pg_backend
        row = _mock_row(
            {
                "worker_id": "w-1",
                "node_id": "node-1",
                "status": "active",
            }
        )
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.list_workers()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_stale_workers(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetch = AsyncMock(return_value=[])
        result = await backend.get_stale_workers(300)
        assert result == []


class TestParallelResults:
    @pytest.mark.asyncio
    async def test_checkpoint_parallel_item(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.execute = AsyncMock()
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"result")

    @pytest.mark.asyncio
    async def test_get_parallel_results(self, pg_backend) -> None:
        backend, conn = pg_backend
        row = _mock_row({"item_index": 0, "output_data": b"result"})
        conn.fetch = AsyncMock(return_value=[row])
        result = await backend.get_parallel_results("run-1", 1)
        assert result == {0: b"result"}


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_check_concurrency_limit(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=_mock_row({"cnt": 1}))
        assert await backend.check_concurrency_limit("wf", "default", 2) is True

    @pytest.mark.asyncio
    async def test_check_concurrency_limit_exceeded(self, pg_backend) -> None:
        backend, conn = pg_backend
        conn.fetchrow = AsyncMock(return_value=_mock_row({"cnt": 5}))
        assert await backend.check_concurrency_limit("wf", "default", 5) is False
