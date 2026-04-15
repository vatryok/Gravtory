"""Unit tests for MySQLBackend (Section 12.1).

These tests verify instantiation, DDL generation, and row mappers
without requiring a running MySQL server.
"""

from __future__ import annotations

import pytest

pytest.importorskip("aiomysql", reason="aiomysql not installed - skipping MySQL backend tests")

from datetime import datetime, timezone

from gravtory.backends.mysql import MySQLBackend, _mysql_schema, _parse_mysql_dsn
from gravtory.core.types import (
    ScheduleType,
    StepStatus,
    WorkerStatus,
    WorkflowStatus,
)


class TestDSNParsing:
    def test_parse_full_dsn(self) -> None:
        result = _parse_mysql_dsn("mysql://user:pass@myhost:3307/mydb")
        assert result["host"] == "myhost"
        assert result["port"] == 3307
        assert result["user"] == "user"
        assert result["password"] == "pass"
        assert result["db"] == "mydb"

    def test_parse_minimal_dsn(self) -> None:
        result = _parse_mysql_dsn("mysql://localhost/testdb")
        assert result["host"] == "localhost"
        assert result["port"] == 3306
        assert result["user"] == "root"
        assert result["password"] == ""
        assert result["db"] == "testdb"


class TestMySQLBackendInit:
    def test_constructor_defaults(self) -> None:
        b = MySQLBackend("mysql://localhost/gravtory")
        assert b._dsn == "mysql://localhost/gravtory"
        assert b._pool is None
        assert b._prefix == "gravtory_"
        assert b._min_size == 2
        assert b._max_size == 10

    def test_constructor_custom(self) -> None:
        b = MySQLBackend(
            "mysql://user:pass@host:3307/db",
            min_pool_size=5,
            max_pool_size=20,
            table_prefix="custom_",
        )
        assert b._min_size == 5
        assert b._max_size == 20
        assert b._prefix == "custom_"


class TestMySQLSchema:
    def test_schema_returns_statements(self) -> None:
        stmts = _mysql_schema("gravtory_")
        assert len(stmts) >= 12
        assert any("workflow_runs" in s for s in stmts)
        assert any("step_outputs" in s for s in stmts)
        assert any("pending_steps" in s for s in stmts)
        assert any("signals" in s for s in stmts)
        assert any("compensations" in s for s in stmts)
        assert any("schedules" in s for s in stmts)
        assert any("locks" in s for s in stmts)
        assert any("dlq" in s for s in stmts)
        assert any("workers" in s for s in stmts)
        assert any("schema_version" in s for s in stmts)

    def test_schema_uses_prefix(self) -> None:
        stmts = _mysql_schema("myapp_")
        joined = " ".join(stmts)
        assert "myapp_workflow_runs" in joined
        assert "myapp_step_outputs" in joined

    def test_schema_uses_innodb(self) -> None:
        stmts = _mysql_schema("gravtory_")
        for s in stmts:
            if "CREATE TABLE" in s:
                assert "ENGINE=InnoDB" in s

    def test_schema_uses_utf8mb4(self) -> None:
        stmts = _mysql_schema("gravtory_")
        for s in stmts:
            if "CREATE TABLE" in s:
                assert "utf8mb4" in s

    def test_schema_uses_datetime6(self) -> None:
        stmts = _mysql_schema("gravtory_")
        joined = " ".join(stmts)
        assert "DATETIME(6)" in joined

    def test_schema_uses_auto_increment(self) -> None:
        stmts = _mysql_schema("gravtory_")
        joined = " ".join(stmts)
        assert "AUTO_INCREMENT" in joined

    def test_schema_uses_longblob(self) -> None:
        stmts = _mysql_schema("gravtory_")
        joined = " ".join(stmts)
        assert "LONGBLOB" in joined


class TestMySQLRowMappers:
    def test_row_to_workflow_run(self) -> None:
        row = {
            "id": "run-1",
            "workflow_name": "OrderWorkflow",
            "workflow_version": 1,
            "namespace": "default",
            "status": "completed",
            "current_step": 3,
            "input_data": None,
            "error_message": None,
            "error_traceback": None,
            "parent_run_id": None,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "completed_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "deadline_at": None,
        }
        wr = MySQLBackend._row_to_workflow_run(row)
        assert wr.id == "run-1"
        assert wr.workflow_name == "OrderWorkflow"
        assert wr.status == WorkflowStatus.COMPLETED
        assert wr.current_step == 3

    def test_row_to_step_output(self) -> None:
        row = {
            "id": 1,
            "workflow_run_id": "run-1",
            "step_order": 1,
            "step_name": "charge",
            "output_data": None,
            "output_type": None,
            "duration_ms": 100,
            "retry_count": 0,
            "status": "completed",
            "error_message": None,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        }
        so = MySQLBackend._row_to_step_output(row)
        assert so.step_name == "charge"
        assert so.status == StepStatus.COMPLETED

    def test_row_to_pending_step(self) -> None:
        row = {
            "id": 1,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "priority": 5,
            "status": "running",
            "worker_id": "w1",
            "retry_count": 1,
            "max_retries": 3,
        }
        ps = MySQLBackend._row_to_pending_step(row)
        assert ps.priority == 5
        assert ps.status == StepStatus.RUNNING
        assert ps.worker_id == "w1"

    def test_row_to_signal(self) -> None:
        row = {
            "id": 1,
            "workflow_run_id": "run-1",
            "signal_name": "approve",
            "signal_data": b"data",
            "consumed": True,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        }
        sig = MySQLBackend._row_to_signal(row)
        assert sig.signal_name == "approve"
        assert sig.consumed is True

    def test_row_to_compensation(self) -> None:
        row = {
            "id": 1,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "handler_name": "undo_charge",
            "step_output": None,
            "status": "completed",
        }
        comp = MySQLBackend._row_to_compensation(row)
        assert comp.handler_name == "undo_charge"
        assert comp.status == StepStatus.COMPLETED

    def test_row_to_schedule(self) -> None:
        row = {
            "id": "sched-1",
            "workflow_name": "DailyReport",
            "schedule_type": "cron",
            "schedule_config": "0 9 * * *",
            "namespace": "default",
            "enabled": True,
            "last_run_at": None,
            "next_run_at": datetime(2025, 6, 1, 9, 0, tzinfo=timezone.utc),
        }
        sched = MySQLBackend._row_to_schedule(row)
        assert sched.workflow_name == "DailyReport"
        assert sched.schedule_type == ScheduleType.CRON

    def test_row_to_dlq_entry(self) -> None:
        row = {
            "id": 1,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "error_message": "boom",
            "error_traceback": "Traceback ...",
            "retry_count": 3,
        }
        dlq = MySQLBackend._row_to_dlq_entry(row)
        assert dlq.error_message == "boom"
        assert dlq.retry_count == 3

    def test_row_to_worker(self) -> None:
        row = {
            "worker_id": "w1",
            "node_id": "node-1",
            "status": "active",
        }
        w = MySQLBackend._row_to_worker(row)
        assert w.worker_id == "w1"
        assert w.status == WorkerStatus.ACTIVE


# ---------------------------------------------------------------------------
# Mock-based async CRUD tests
# ---------------------------------------------------------------------------

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gravtory.core.types import (
    Compensation,
    DLQEntry,
    PendingStep,
    Schedule,
    Signal,
    SignalWait,
    StepOutput,
    WorkerInfo,
    WorkflowRun,
)


def _make_mysql_pool():
    """Create a mock aiomysql pool with acquire() → conn → cursor."""
    pool = MagicMock()
    conn = MagicMock()
    cur = AsyncMock()
    cur.rowcount = 1
    cur.fetchone = AsyncMock(return_value=None)
    cur.fetchall = AsyncMock(return_value=[])
    cur.close = AsyncMock()

    conn.cursor = AsyncMock(return_value=cur)
    conn.begin = AsyncMock()
    conn.commit = AsyncMock()

    acm = MagicMock()
    acm.__aenter__ = AsyncMock(return_value=conn)
    acm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = acm
    pool.close = MagicMock()
    pool.wait_closed = AsyncMock()

    return pool, conn, cur


@pytest.fixture
def mysql_backend():
    with patch("gravtory.backends.mysql.aiomysql"):
        backend = MySQLBackend("mysql://localhost/test")
        pool, conn, cur = _make_mysql_pool()
        backend._pool = pool
        yield backend, conn, cur


class TestMySQLLifecycle:
    @pytest.mark.asyncio
    async def test_close(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.close()
        assert backend._pool is None

    @pytest.mark.asyncio
    async def test_health_check_ok(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        assert await backend.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_no_pool(self) -> None:
        with patch("gravtory.backends.mysql.aiomysql"):
            backend = MySQLBackend("mysql://localhost/test")
            assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_error(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.execute = AsyncMock(side_effect=Exception("conn lost"))
        assert await backend.health_check() is False


class TestMySQLWorkflowRuns:
    @pytest.mark.asyncio
    async def test_create_workflow_run(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await backend.create_workflow_run(run)
        cur.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_workflow_run_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cur.fetchone = AsyncMock(
            return_value={
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
        result = await backend.get_workflow_run("run-1")
        assert result is not None
        assert result.id == "run-1"

    @pytest.mark.asyncio
    async def test_get_workflow_run_not_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value=None)
        result = await backend.get_workflow_run("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_workflow_status(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.update_workflow_status(
            "run-1",
            WorkflowStatus.COMPLETED,
            error_message="done",
            output_data=b"result",
        )
        cur.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        row = {
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
        cur.fetchall = AsyncMock(return_value=[row])
        result = await backend.list_workflow_runs()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_with_filters(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(return_value=[])
        result = await backend.list_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_count_workflow_runs(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"cnt": 5})
        result = await backend.count_workflow_runs()
        assert result == 5

    @pytest.mark.asyncio
    async def test_count_with_filters(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"cnt": 2})
        result = await backend.count_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert result == 2

    @pytest.mark.asyncio
    async def test_get_incomplete_runs(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cur.fetchall = AsyncMock(
            return_value=[
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
            ]
        )
        result = await backend.get_incomplete_runs()
        assert len(result) == 1


class TestMySQLStepOutputs:
    @pytest.mark.asyncio
    async def test_save_step_output(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=100,
        )
        await backend.save_step_output(so)

    @pytest.mark.asyncio
    async def test_get_step_outputs(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cur.fetchall = AsyncMock(
            return_value=[
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
            ]
        )
        result = await backend.get_step_outputs("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_step_output_not_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value=None)
        result = await backend.get_step_output("run-1", 99)
        assert result is None


class TestMySQLPendingSteps:
    @pytest.mark.asyncio
    async def test_enqueue_step(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        ps = PendingStep(workflow_run_id="run-1", step_order=1, priority=5, max_retries=3)
        await backend.enqueue_step(ps)

    @pytest.mark.asyncio
    async def test_claim_step_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(
            side_effect=[
                {"id": 1},  # SELECT id for lock
                {
                    "id": 1,
                    "workflow_run_id": "run-1",
                    "step_order": 1,
                    "priority": 5,
                    "status": "running",
                    "worker_id": "w-1",
                    "retry_count": 0,
                    "max_retries": 3,
                },  # SELECT * after update
            ]
        )
        result = await backend.claim_step("w-1")
        assert result is not None

    @pytest.mark.asyncio
    async def test_claim_step_empty(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value=None)
        result = await backend.claim_step("w-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_step(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
        )
        await backend.complete_step(1, so)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        await backend.fail_step(1, error_message="boom", retry_at=retry_at)

    @pytest.mark.asyncio
    async def test_fail_step_no_retry(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.fail_step(1, error_message="fatal")


class TestMySQLSignals:
    @pytest.mark.asyncio
    async def test_send_signal(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        sig = Signal(workflow_run_id="run-1", signal_name="go", signal_data=b"data")
        await backend.send_signal(sig)

    @pytest.mark.asyncio
    async def test_consume_signal_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cur.fetchone = AsyncMock(
            side_effect=[
                {"id": 1},  # SELECT id for lock
                {
                    "id": 1,
                    "workflow_run_id": "run-1",
                    "signal_name": "go",
                    "signal_data": b"data",
                    "consumed": True,
                    "created_at": now,
                },
            ]
        )
        result = await backend.consume_signal("run-1", "go")
        assert result is not None

    @pytest.mark.asyncio
    async def test_consume_signal_not_found(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value=None)
        result = await backend.consume_signal("run-1", "go")
        assert result is None

    @pytest.mark.asyncio
    async def test_register_signal_wait(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        wait = SignalWait(
            workflow_run_id="run-1",
            signal_name="go",
            timeout_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        await backend.register_signal_wait(wait)


class TestMySQLCompensation:
    @pytest.mark.asyncio
    async def test_save_compensation(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        comp = Compensation(
            workflow_run_id="run-1",
            step_order=1,
            handler_name="undo",
            step_output=b"data",
            status="pending",
        )
        await backend.save_compensation(comp)

    @pytest.mark.asyncio
    async def test_get_compensations(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "workflow_run_id": "run-1",
                    "step_order": 1,
                    "handler_name": "undo",
                    "step_output": b"data",
                    "status": "completed",
                }
            ]
        )
        result = await backend.get_compensations("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_compensation_status(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.update_compensation_status(1, "completed", error_message="ok")


class TestMySQLScheduling:
    @pytest.mark.asyncio
    async def test_save_schedule(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        sched = Schedule(
            id="s-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        await backend.save_schedule(sched)

    @pytest.mark.asyncio
    async def test_get_due_schedules(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cur.fetchall = AsyncMock(
            return_value=[
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
            ]
        )
        result = await backend.get_due_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_schedule_last_run(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        await backend.update_schedule_last_run("s-1", now, now)

    @pytest.mark.asyncio
    async def test_get_all_enabled(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(return_value=[])
        result = await backend.get_all_enabled_schedules()
        assert result == []

    @pytest.mark.asyncio
    async def test_list_all_schedules(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(return_value=[])
        result = await backend.list_all_schedules()
        assert result == []


class TestMySQLLocks:
    @pytest.mark.asyncio
    async def test_acquire_lock_success(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"holder_id": "holder-1"})
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_lock_conflict(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"holder_id": "someone-else"})
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is False

    @pytest.mark.asyncio
    async def test_release_lock(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.rowcount = 1
        result = await backend.release_lock("my-lock", "holder-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_refresh_lock(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.rowcount = 1
        result = await backend.refresh_lock("my-lock", "holder-1", 120)
        assert result is True


class TestMySQLDLQ:
    @pytest.mark.asyncio
    async def test_add_to_dlq(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        entry = DLQEntry(workflow_run_id="run-1", step_order=1, error_message="boom")
        await backend.add_to_dlq(entry)

    @pytest.mark.asyncio
    async def test_list_dlq(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "workflow_run_id": "run-1",
                    "step_order": 1,
                    "error_message": "boom",
                    "error_traceback": None,
                    "retry_count": 0,
                }
            ]
        )
        result = await backend.list_dlq()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_remove_from_dlq(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.remove_from_dlq(1)

    @pytest.mark.asyncio
    async def test_purge_dlq(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"cnt": 3})
        result = await backend.purge_dlq()
        assert result == 3


class TestMySQLWorkers:
    @pytest.mark.asyncio
    async def test_register_worker(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        w = WorkerInfo(worker_id="w-1", node_id="node-1", status=WorkerStatus.ACTIVE)
        await backend.register_worker(w)

    @pytest.mark.asyncio
    async def test_worker_heartbeat_with_task(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.worker_heartbeat("w-1", current_task="run-1")

    @pytest.mark.asyncio
    async def test_worker_heartbeat_no_task(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.worker_heartbeat("w-1")

    @pytest.mark.asyncio
    async def test_deregister_worker(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.deregister_worker("w-1")

    @pytest.mark.asyncio
    async def test_list_workers(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(
            return_value=[
                {
                    "worker_id": "w-1",
                    "node_id": "node-1",
                    "status": "active",
                }
            ]
        )
        result = await backend.list_workers()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_stale_workers(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(return_value=[])
        result = await backend.get_stale_workers(300)
        assert result == []


class TestMySQLParallelResults:
    @pytest.mark.asyncio
    async def test_checkpoint_parallel_item(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"result")

    @pytest.mark.asyncio
    async def test_get_parallel_results(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchall = AsyncMock(
            return_value=[
                {"item_index": 0, "output_data": b"r0"},
                {"item_index": 1, "output_data": b"r1"},
            ]
        )
        result = await backend.get_parallel_results("run-1", 1)
        assert result == {0: b"r0", 1: b"r1"}


class TestMySQLConcurrency:
    @pytest.mark.asyncio
    async def test_check_concurrency_limit(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"cnt": 1})
        assert await backend.check_concurrency_limit("wf", "default", 2) is True

    @pytest.mark.asyncio
    async def test_check_concurrency_exceeded(self, mysql_backend) -> None:
        backend, conn, cur = mysql_backend
        cur.fetchone = AsyncMock(return_value={"cnt": 5})
        assert await backend.check_concurrency_limit("wf", "default", 5) is False
