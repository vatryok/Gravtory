"""Unit tests for MongoDBBackend (Section 12.2).

These tests verify instantiation and document mappers
without requiring a running MongoDB server.
"""

from __future__ import annotations

from datetime import datetime, timezone

from gravtory.backends.mongodb import MongoDBBackend
from gravtory.core.types import (
    ScheduleType,
    StepStatus,
    WorkerStatus,
    WorkflowStatus,
)


class TestMongoDBBackendInit:
    def test_constructor_defaults(self) -> None:
        b = MongoDBBackend("mongodb://localhost:27017/gravtory")
        assert b._dsn == "mongodb://localhost:27017/gravtory"
        assert b._db_name == "gravtory"
        assert b._prefix == "gravtory_"
        assert b._client is None
        assert b._db is None

    def test_constructor_custom(self) -> None:
        b = MongoDBBackend(
            "mongodb://user:pass@host:27018/mydb",
            database_name="custom_db",
            collection_prefix="app_",
        )
        assert b._db_name == "custom_db"
        assert b._prefix == "app_"


class TestMongoDBDocMappers:
    def test_doc_to_workflow_run(self) -> None:
        doc = {
            "_id": "run-1",
            "workflow_name": "OrderWorkflow",
            "workflow_version": 2,
            "namespace": "prod",
            "status": "completed",
            "current_step": 3,
            "input_data": b"input",
            "error_message": None,
            "error_traceback": None,
            "parent_run_id": None,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "completed_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "deadline_at": None,
        }
        wr = MongoDBBackend._doc_to_workflow_run(doc)
        assert wr.id == "run-1"
        assert wr.workflow_name == "OrderWorkflow"
        assert wr.status == WorkflowStatus.COMPLETED
        assert wr.workflow_version == 2
        assert wr.namespace == "prod"

    def test_doc_to_workflow_run_defaults(self) -> None:
        doc = {"_id": "run-2", "workflow_name": "W", "status": "pending"}
        wr = MongoDBBackend._doc_to_workflow_run(doc)
        assert wr.workflow_version == 1
        assert wr.namespace == "default"

    def test_doc_to_step_output(self) -> None:
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 1,
            "step_name": "charge",
            "output_data": b"data",
            "output_type": "json",
            "duration_ms": 150,
            "retry_count": 0,
            "status": "completed",
            "error_message": None,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        }
        so = MongoDBBackend._doc_to_step_output(doc)
        assert so.step_name == "charge"
        assert so.status == StepStatus.COMPLETED
        assert so.duration_ms == 150

    def test_doc_to_pending_step(self) -> None:
        doc = {
            "_id": 5,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "priority": 10,
            "status": "running",
            "worker_id": "w1",
            "retry_count": 1,
            "max_retries": 3,
        }
        ps = MongoDBBackend._doc_to_pending_step(doc)
        assert ps.id == 5
        assert ps.priority == 10
        assert ps.status == StepStatus.RUNNING

    def test_doc_to_signal(self) -> None:
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "signal_name": "approve",
            "signal_data": b"yes",
            "consumed": True,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        }
        sig = MongoDBBackend._doc_to_signal(doc)
        assert sig.signal_name == "approve"
        assert sig.consumed is True

    def test_doc_to_compensation(self) -> None:
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "handler_name": "undo_charge",
            "step_output": b"data",
            "status": "completed",
        }
        comp = MongoDBBackend._doc_to_compensation(doc)
        assert comp.handler_name == "undo_charge"
        assert comp.status == StepStatus.COMPLETED

    def test_doc_to_schedule(self) -> None:
        doc = {
            "_id": "sched-1",
            "workflow_name": "DailyReport",
            "schedule_type": "cron",
            "schedule_config": "0 9 * * *",
            "namespace": "default",
            "enabled": True,
            "last_run_at": None,
            "next_run_at": datetime(2025, 6, 1, 9, 0, tzinfo=timezone.utc),
        }
        sched = MongoDBBackend._doc_to_schedule(doc)
        assert sched.id == "sched-1"
        assert sched.schedule_type == ScheduleType.CRON
        assert sched.enabled is True

    def test_doc_to_dlq_entry(self) -> None:
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 2,
            "error_message": "boom",
            "error_traceback": "Traceback ...",
            "retry_count": 3,
        }
        dlq = MongoDBBackend._doc_to_dlq_entry(doc)
        assert dlq.error_message == "boom"
        assert dlq.retry_count == 3

    def test_doc_to_worker(self) -> None:
        doc = {
            "_id": "w1",
            "node_id": "node-1",
            "status": "active",
        }
        w = MongoDBBackend._doc_to_worker(doc)
        assert w.worker_id == "w1"
        assert w.node_id == "node-1"
        assert w.status == WorkerStatus.ACTIVE

    def test_doc_to_pending_step_invalid_status(self) -> None:
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 1,
            "priority": 0,
            "status": "unknown_status",
            "worker_id": None,
            "retry_count": 0,
            "max_retries": 0,
        }
        ps = MongoDBBackend._doc_to_pending_step(doc)
        assert ps.status == StepStatus.PENDING


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


def _mock_cursor(docs: list[dict]) -> MagicMock:
    """Create a mock MongoDB cursor with chainable sort/skip/limit/to_list."""
    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=docs)
    return cursor


def _mock_update_result(modified: int = 1) -> MagicMock:
    result = MagicMock()
    result.modified_count = modified
    return result


def _mock_delete_result(deleted: int = 1) -> MagicMock:
    result = MagicMock()
    result.deleted_count = deleted
    return result


class _AutoCollections(dict):
    """Dict that auto-creates mock MongoDB collections on access."""

    def __missing__(self, name: str) -> MagicMock:
        col = MagicMock()
        col.insert_one = AsyncMock()
        col.find_one = AsyncMock(return_value=None)
        col.find_one_and_update = AsyncMock(return_value=None)
        col.update_one = AsyncMock(return_value=_mock_update_result())
        col.delete_one = AsyncMock(return_value=_mock_delete_result())
        col.delete_many = AsyncMock(return_value=_mock_delete_result(0))
        col.count_documents = AsyncMock(return_value=0)
        col.create_index = AsyncMock()
        col.find.return_value = _mock_cursor([])
        self[name] = col
        return col


@pytest.fixture
def mongo_backend():
    """Create a MongoDBBackend with mocked motor client and collections."""
    backend = MongoDBBackend("mongodb://localhost/test")
    db = MagicMock()
    backend._client = MagicMock()
    backend._db = db
    collections = _AutoCollections()

    db.__getitem__ = lambda self, name: collections[name]
    yield backend, collections


class TestMongoLifecycle:
    @pytest.mark.asyncio
    async def test_initialize(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        mock_db = MagicMock()
        auto_cols = _AutoCollections()
        mock_db.__getitem__ = lambda self, name: auto_cols[name]
        mock_client = MagicMock()
        mock_client.get_database.return_value = mock_db
        with patch(
            "gravtory.backends.mongodb.motor.motor_asyncio.AsyncIOMotorClient",
            return_value=mock_client,
        ):
            await backend.initialize()

    @pytest.mark.asyncio
    async def test_close(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.close()
        assert backend._client is None
        assert backend._db is None

    @pytest.mark.asyncio
    async def test_health_check_ok(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        backend._client.admin.command = AsyncMock(return_value={"ok": 1})
        assert await backend.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_no_client(self) -> None:
        backend = MongoDBBackend("mongodb://localhost/test")
        assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_error(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        backend._client.admin.command = AsyncMock(side_effect=Exception("down"))
        assert await backend.health_check() is False


class TestMongoWorkflowRuns:
    @pytest.mark.asyncio
    async def test_create_workflow_run(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await backend.create_workflow_run(run)
        cols[f"{backend._p}workflow_runs"].insert_one.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_workflow_run_duplicate(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        from pymongo.errors import DuplicateKeyError

        cols[f"{backend._p}workflow_runs"].insert_one = AsyncMock(
            side_effect=DuplicateKeyError("dup")
        )
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await backend.create_workflow_run(run)  # Should not raise

    @pytest.mark.asyncio
    async def test_get_workflow_run_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cols[f"{backend._p}workflow_runs"].find_one = AsyncMock(
            return_value={
                "_id": "run-1",
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
    async def test_get_workflow_run_not_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].find_one = AsyncMock(return_value=None)
        result = await backend.get_workflow_run("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_workflow_status_terminal(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.update_workflow_status(
            "run-1",
            WorkflowStatus.COMPLETED,
            error_message="done",
            output_data=b"result",
        )
        cols[f"{backend._p}workflow_runs"].update_one.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_workflow_status_nonterminal(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.update_workflow_status("run-1", WorkflowStatus.RUNNING)

    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        doc = {
            "_id": "run-1",
            "workflow_name": "wf",
            "workflow_version": 1,
            "namespace": "default",
            "status": "completed",
            "current_step": 1,
            "created_at": now,
            "updated_at": now,
            "completed_at": now,
        }
        cols[f"{backend._p}workflow_runs"].find.return_value = _mock_cursor([doc])
        result = await backend.list_workflow_runs()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_with_filters(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].find.return_value = _mock_cursor([])
        result = await backend.list_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_count_workflow_runs(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].count_documents = AsyncMock(return_value=5)
        result = await backend.count_workflow_runs()
        assert result == 5

    @pytest.mark.asyncio
    async def test_count_with_filters(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].count_documents = AsyncMock(return_value=2)
        result = await backend.count_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert result == 2

    @pytest.mark.asyncio
    async def test_get_incomplete_runs(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        doc = {
            "_id": "run-1",
            "workflow_name": "wf",
            "status": "running",
            "created_at": now,
            "updated_at": now,
        }
        cols[f"{backend._p}workflow_runs"].find.return_value = _mock_cursor([doc])
        result = await backend.get_incomplete_runs()
        assert len(result) == 1


class TestMongoStepOutputs:
    @pytest.mark.asyncio
    async def test_save_step_output(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        # _next_id needs counters collection
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=100,
        )
        await backend.save_step_output(so)

    @pytest.mark.asyncio
    async def test_get_step_outputs(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        doc = {
            "_id": 1,
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
        cols[f"{backend._p}step_outputs"].find.return_value = _mock_cursor([doc])
        result = await backend.get_step_outputs("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_step_output_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cols[f"{backend._p}step_outputs"].find_one = AsyncMock(
            return_value={
                "_id": 1,
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
        result = await backend.get_step_output("run-1", 1)
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_step_output_not_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}step_outputs"].find_one = AsyncMock(return_value=None)
        result = await backend.get_step_output("run-1", 99)
        assert result is None


class TestMongoPendingSteps:
    @pytest.mark.asyncio
    async def test_enqueue_step(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        ps = PendingStep(workflow_run_id="run-1", step_order=1, priority=5, max_retries=3)
        await backend.enqueue_step(ps)

    @pytest.mark.asyncio
    async def test_claim_step_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}pending_steps"].find_one_and_update = AsyncMock(
            return_value={
                "_id": 1,
                "workflow_run_id": "run-1",
                "step_order": 1,
                "priority": 5,
                "status": "running",
                "worker_id": "w-1",
                "retry_count": 0,
                "max_retries": 3,
            }
        )
        result = await backend.claim_step("w-1")
        assert result is not None

    @pytest.mark.asyncio
    async def test_claim_step_empty(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}pending_steps"].find_one_and_update = AsyncMock(return_value=None)
        result = await backend.claim_step("w-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_step(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
        )
        await backend.complete_step(1, so)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        await backend.fail_step(1, error_message="boom", retry_at=retry_at)

    @pytest.mark.asyncio
    async def test_fail_step_no_retry(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.fail_step(1, error_message="fatal")


class TestMongoSignals:
    @pytest.mark.asyncio
    async def test_send_signal(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        sig = Signal(workflow_run_id="run-1", signal_name="go", signal_data=b"data")
        await backend.send_signal(sig)

    @pytest.mark.asyncio
    async def test_consume_signal_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cols[f"{backend._p}signals"].find_one_and_update = AsyncMock(
            return_value={
                "_id": 1,
                "workflow_run_id": "run-1",
                "signal_name": "go",
                "signal_data": b"data",
                "consumed": True,
                "created_at": now,
            }
        )
        result = await backend.consume_signal("run-1", "go")
        assert result is not None

    @pytest.mark.asyncio
    async def test_consume_signal_not_found(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}signals"].find_one_and_update = AsyncMock(return_value=None)
        result = await backend.consume_signal("run-1", "go")
        assert result is None

    @pytest.mark.asyncio
    async def test_register_signal_wait(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        wait = SignalWait(
            workflow_run_id="run-1",
            signal_name="go",
            timeout_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        await backend.register_signal_wait(wait)


class TestMongoCompensation:
    @pytest.mark.asyncio
    async def test_save_compensation(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        comp = Compensation(
            workflow_run_id="run-1",
            step_order=1,
            handler_name="undo",
            step_output=b"data",
            status="pending",
        )
        await backend.save_compensation(comp)

    @pytest.mark.asyncio
    async def test_get_compensations(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 1,
            "handler_name": "undo",
            "step_output": b"data",
            "status": "completed",
        }
        cols[f"{backend._p}compensations"].find.return_value = _mock_cursor([doc])
        result = await backend.get_compensations("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_compensation_status(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.update_compensation_status(1, "completed", error_message="ok")


class TestMongoScheduling:
    @pytest.mark.asyncio
    async def test_save_schedule(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        sched = Schedule(
            id="s-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        await backend.save_schedule(sched)

    @pytest.mark.asyncio
    async def test_get_due_schedules(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        doc = {
            "_id": "s-1",
            "workflow_name": "wf",
            "schedule_type": "cron",
            "schedule_config": "* * * * *",
            "namespace": "default",
            "enabled": True,
            "last_run_at": None,
            "next_run_at": now,
        }
        cols[f"{backend._p}schedules"].find.return_value = _mock_cursor([doc])
        result = await backend.get_due_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_schedule_last_run(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        await backend.update_schedule_last_run("s-1", now, now)

    @pytest.mark.asyncio
    async def test_get_all_enabled(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}schedules"].find.return_value = _mock_cursor([])
        result = await backend.get_all_enabled_schedules()
        assert result == []

    @pytest.mark.asyncio
    async def test_list_all_schedules(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}schedules"].find.return_value = _mock_cursor([])
        result = await backend.list_all_schedules()
        assert result == []


class TestMongoLocks:
    @pytest.mark.asyncio
    async def test_acquire_lock_insert_success(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_lock_duplicate_then_takeover(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        from pymongo.errors import DuplicateKeyError

        cols[f"{backend._p}locks"].insert_one = AsyncMock(side_effect=DuplicateKeyError("dup"))
        cols[f"{backend._p}locks"].update_one = AsyncMock(return_value=_mock_update_result(1))
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_lock_duplicate_blocked(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        from pymongo.errors import DuplicateKeyError

        cols[f"{backend._p}locks"].insert_one = AsyncMock(side_effect=DuplicateKeyError("dup"))
        cols[f"{backend._p}locks"].update_one = AsyncMock(return_value=_mock_update_result(0))
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is False

    @pytest.mark.asyncio
    async def test_release_lock(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}locks"].delete_one = AsyncMock(return_value=_mock_delete_result(1))
        result = await backend.release_lock("my-lock", "holder-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_refresh_lock(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}locks"].update_one = AsyncMock(return_value=_mock_update_result(1))
        result = await backend.refresh_lock("my-lock", "holder-1", 120)
        assert result is True


class TestMongoDLQ:
    @pytest.mark.asyncio
    async def test_add_to_dlq(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}counters"].find_one_and_update = AsyncMock(return_value={"seq": 1})
        entry = DLQEntry(workflow_run_id="run-1", step_order=1, error_message="boom")
        await backend.add_to_dlq(entry)

    @pytest.mark.asyncio
    async def test_list_dlq(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        doc = {
            "_id": 1,
            "workflow_run_id": "run-1",
            "step_order": 1,
            "error_message": "boom",
            "error_traceback": None,
            "retry_count": 0,
        }
        cols[f"{backend._p}dlq"].find.return_value = _mock_cursor([doc])
        result = await backend.list_dlq()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_remove_from_dlq(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.remove_from_dlq(1)

    @pytest.mark.asyncio
    async def test_purge_dlq(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}dlq"].delete_many = AsyncMock(return_value=_mock_delete_result(5))
        result = await backend.purge_dlq()
        assert result == 5


class TestMongoWorkers:
    @pytest.mark.asyncio
    async def test_register_worker(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        w = WorkerInfo(worker_id="w-1", node_id="node-1", status=WorkerStatus.ACTIVE)
        await backend.register_worker(w)

    @pytest.mark.asyncio
    async def test_worker_heartbeat_with_task(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.worker_heartbeat("w-1", current_task="run-1")

    @pytest.mark.asyncio
    async def test_worker_heartbeat_no_task(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.worker_heartbeat("w-1")

    @pytest.mark.asyncio
    async def test_deregister_worker(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.deregister_worker("w-1")

    @pytest.mark.asyncio
    async def test_list_workers(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        doc = {"_id": "w-1", "node_id": "node-1", "status": "active"}
        cols[f"{backend._p}workers"].find.return_value = _mock_cursor([doc])
        result = await backend.list_workers()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_stale_workers(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workers"].find.return_value = _mock_cursor([])
        result = await backend.get_stale_workers(300)
        assert result == []


class TestMongoParallelResults:
    @pytest.mark.asyncio
    async def test_checkpoint_parallel_item(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"result")

    @pytest.mark.asyncio
    async def test_get_parallel_results(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        docs = [
            {"item_index": 0, "output_data": b"r0"},
            {"item_index": 1, "output_data": b"r1"},
        ]
        cols[f"{backend._p}parallel_results"].find.return_value = _mock_cursor(docs)
        result = await backend.get_parallel_results("run-1", 1)
        assert result == {0: b"r0", 1: b"r1"}


class TestMongoConcurrency:
    @pytest.mark.asyncio
    async def test_check_concurrency_limit(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].count_documents = AsyncMock(return_value=1)
        assert await backend.check_concurrency_limit("wf", "default", 2) is True

    @pytest.mark.asyncio
    async def test_check_concurrency_exceeded(self, mongo_backend) -> None:
        backend, cols = mongo_backend
        cols[f"{backend._p}workflow_runs"].count_documents = AsyncMock(return_value=5)
        assert await backend.check_concurrency_limit("wf", "default", 5) is False
