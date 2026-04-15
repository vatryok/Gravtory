"""Unit tests for RedisBackend (Section 12.3).

These tests verify instantiation, helper functions, and hash mappers
without requiring a running Redis server.
"""

from __future__ import annotations

import pytest

pytest.importorskip("redis", reason="redis not installed - skipping Redis backend tests")

from datetime import datetime, timezone

from gravtory.backends.redis import (
    RedisBackend,
    _bytes_or_none,
    _dt_to_str,
    _str_to_dt,
    _to_bool,
    _to_int,
    _to_str,
)
from gravtory.core.types import (
    ScheduleType,
    StepStatus,
    WorkerStatus,
    WorkflowStatus,
)


class TestRedisBackendInit:
    def test_constructor_defaults(self) -> None:
        b = RedisBackend("redis://localhost:6379/0")
        assert b._dsn == "redis://localhost:6379/0"
        assert b._prefix == "gravtory:"
        assert b._client is None

    def test_constructor_custom(self) -> None:
        b = RedisBackend("redis://host:6380/1", key_prefix="app:")
        assert b._prefix == "app:"
        assert b._p == "app:"


class TestRedisHelpers:
    def test_dt_to_str_none(self) -> None:
        assert _dt_to_str(None) == ""

    def test_dt_to_str(self) -> None:
        dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _dt_to_str(dt)
        assert "2025-01-01" in result

    def test_str_to_dt_none(self) -> None:
        assert _str_to_dt(None) is None
        assert _str_to_dt("") is None
        assert _str_to_dt(b"") is None

    def test_str_to_dt_valid(self) -> None:
        dt = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = _str_to_dt(dt.isoformat())
        assert result is not None
        assert result.year == 2025
        assert result.month == 6

    def test_str_to_dt_bytes(self) -> None:
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result = _str_to_dt(dt.isoformat().encode())
        assert result is not None
        assert result.year == 2025

    def test_str_to_dt_invalid(self) -> None:
        assert _str_to_dt("not-a-date") is None

    def test_to_str(self) -> None:
        assert _to_str(None) == ""
        assert _to_str(b"hello") == "hello"
        assert _to_str("world") == "world"
        assert _to_str(42) == "42"

    def test_to_int(self) -> None:
        assert _to_int(None) == 0
        assert _to_int(b"42") == 42
        assert _to_int("7") == 7
        assert _to_int("invalid") == 0

    def test_to_bool(self) -> None:
        assert _to_bool(None) is False
        assert _to_bool(b"1") is True
        assert _to_bool("true") is True
        assert _to_bool("yes") is True
        assert _to_bool("0") is False
        assert _to_bool("false") is False

    def test_bytes_or_none(self) -> None:
        assert _bytes_or_none(None) is None
        assert _bytes_or_none(b"") is None
        assert _bytes_or_none(b"data") == b"data"
        assert _bytes_or_none("text") == b"text"
        assert _bytes_or_none("") is None


class TestRedisHashMappers:
    def test_hash_to_workflow_run(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"run-1",
            b"workflow_name": b"OrderWorkflow",
            b"workflow_version": b"2",
            b"namespace": b"prod",
            b"status": b"completed",
            b"current_step": b"3",
            b"input_data": b"",
            b"output_data": b"",
            b"error_message": b"",
            b"error_traceback": b"",
            b"parent_run_id": b"",
            b"created_at": b"2025-01-01T00:00:00+00:00",
            b"updated_at": b"2025-01-01T00:00:00+00:00",
            b"completed_at": b"2025-01-01T00:00:00+00:00",
            b"deadline_at": b"",
        }
        wr = RedisBackend._hash_to_workflow_run("run-1", data)
        assert wr.id == "run-1"
        assert wr.workflow_name == "OrderWorkflow"
        assert wr.status == WorkflowStatus.COMPLETED
        assert wr.workflow_version == 2
        assert wr.namespace == "prod"
        assert wr.current_step == 3

    def test_hash_to_step_output(self) -> None:
        data: dict[bytes, bytes] = {
            b"workflow_run_id": b"run-1",
            b"step_order": b"1",
            b"step_name": b"charge",
            b"output_data": b"result",
            b"output_type": b"json",
            b"duration_ms": b"150",
            b"retry_count": b"0",
            b"status": b"completed",
            b"error_message": b"",
            b"created_at": b"2025-01-01T00:00:00+00:00",
        }
        so = RedisBackend._hash_to_step_output(data)
        assert so.step_name == "charge"
        assert so.status == StepStatus.COMPLETED
        assert so.duration_ms == 150

    def test_hash_to_pending_step(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"5",
            b"workflow_run_id": b"run-1",
            b"step_order": b"2",
            b"priority": b"10",
            b"status": b"running",
            b"worker_id": b"w1",
            b"retry_count": b"1",
            b"max_retries": b"3",
        }
        ps = RedisBackend._hash_to_pending_step(data)
        assert ps.id == 5
        assert ps.priority == 10
        assert ps.status == StepStatus.RUNNING
        assert ps.worker_id == "w1"

    def test_hash_to_signal(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"1",
            b"workflow_run_id": b"run-1",
            b"signal_name": b"approve",
            b"signal_data": b"yes",
            b"consumed": b"1",
            b"created_at": b"2025-01-01T00:00:00+00:00",
        }
        sig = RedisBackend._hash_to_signal(data)
        assert sig.signal_name == "approve"
        assert sig.consumed is True

    def test_hash_to_compensation(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"1",
            b"workflow_run_id": b"run-1",
            b"step_order": b"2",
            b"handler_name": b"undo_charge",
            b"step_output": b"data",
            b"status": b"completed",
        }
        comp = RedisBackend._hash_to_compensation(data)
        assert comp.handler_name == "undo_charge"
        assert comp.status == StepStatus.COMPLETED

    def test_hash_to_schedule(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"sched-1",
            b"workflow_name": b"DailyReport",
            b"schedule_type": b"cron",
            b"schedule_config": b"0 9 * * *",
            b"namespace": b"default",
            b"enabled": b"1",
            b"last_run_at": b"",
            b"next_run_at": b"2025-06-01T09:00:00+00:00",
        }
        sched = RedisBackend._hash_to_schedule(data)
        assert sched.id == "sched-1"
        assert sched.schedule_type == ScheduleType.CRON
        assert sched.enabled is True

    def test_hash_to_dlq_entry(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"1",
            b"workflow_run_id": b"run-1",
            b"step_order": b"2",
            b"error_message": b"boom",
            b"error_traceback": b"Traceback ...",
            b"retry_count": b"3",
        }
        dlq = RedisBackend._hash_to_dlq_entry(data)
        assert dlq.error_message == "boom"
        assert dlq.retry_count == 3

    def test_hash_to_worker(self) -> None:
        data: dict[bytes, bytes] = {
            b"worker_id": b"w1",
            b"node_id": b"node-1",
            b"status": b"active",
        }
        w = RedisBackend._hash_to_worker(data)
        assert w.worker_id == "w1"
        assert w.node_id == "node-1"
        assert w.status == WorkerStatus.ACTIVE

    def test_hash_to_pending_step_invalid_status(self) -> None:
        data: dict[bytes, bytes] = {
            b"id": b"1",
            b"workflow_run_id": b"run-1",
            b"step_order": b"1",
            b"priority": b"0",
            b"status": b"unknown_status",
            b"worker_id": b"",
            b"retry_count": b"0",
            b"max_retries": b"0",
        }
        ps = RedisBackend._hash_to_pending_step(data)
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


def _make_redis_client() -> MagicMock:
    """Create a mock redis.asyncio client with pipeline support."""
    client = MagicMock()
    # Basic async methods
    client.ping = AsyncMock()
    client.script_load = AsyncMock(return_value="sha1hash")
    client.aclose = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.hset = AsyncMock()
    client.hget = AsyncMock(return_value=None)
    client.hgetall = AsyncMock(return_value={})
    client.hincrby = AsyncMock()
    client.incr = AsyncMock(return_value=1)
    client.sadd = AsyncMock()
    client.srem = AsyncMock()
    client.smembers = AsyncMock(return_value=set())
    client.sinter = AsyncMock(return_value=set())
    client.zadd = AsyncMock()
    client.zrevrange = AsyncMock(return_value=[])
    client.expire = AsyncMock()
    client.delete = AsyncMock()
    client.evalsha = AsyncMock(return_value=1)

    # Pipeline returns a mock with same methods + execute
    pipe = MagicMock()
    pipe.hset = MagicMock()
    pipe.sadd = MagicMock()
    pipe.srem = MagicMock()
    pipe.zadd = MagicMock()
    pipe.delete = MagicMock()
    pipe.hincrby = MagicMock()
    pipe.execute = AsyncMock(return_value=[])
    client.pipeline.return_value = pipe

    return client, pipe


@pytest.fixture
def redis_backend():
    backend = RedisBackend("redis://localhost/0")
    client, pipe = _make_redis_client()
    backend._client = client
    backend._checkpoint_sha = "ckpt_sha"
    backend._claim_sha = "claim_sha"
    backend._consume_signal_sha = "consume_sha"
    backend._release_lock_sha = "release_sha"
    backend._refresh_lock_sha = "refresh_sha"
    backend._create_run_sha = "create_sha"
    yield backend, client, pipe


class TestRedisLifecycle:
    @pytest.mark.asyncio
    async def test_initialize(self) -> None:
        with patch("gravtory.backends.redis.aioredis") as mock_aioredis:
            mock_client = MagicMock()
            mock_client.ping = AsyncMock()
            mock_client.script_load = AsyncMock(return_value="sha")
            mock_aioredis.from_url.return_value = mock_client
            backend = RedisBackend("redis://localhost/0")
            await backend.initialize()
            assert backend._client is not None
            assert mock_client.script_load.await_count == 6

    @pytest.mark.asyncio
    async def test_close(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.close()
        client.aclose.assert_awaited_once()
        assert backend._client is None

    @pytest.mark.asyncio
    async def test_close_no_client(self) -> None:
        backend = RedisBackend("redis://localhost/0")
        await backend.close()  # No error

    @pytest.mark.asyncio
    async def test_health_check_ok(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        assert await backend.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_no_client(self) -> None:
        backend = RedisBackend("redis://localhost/0")
        assert await backend.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_error(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.ping = AsyncMock(side_effect=Exception("down"))
        assert await backend.health_check() is False


class TestRedisWorkflowRuns:
    @pytest.mark.asyncio
    async def test_create_workflow_run(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await backend.create_workflow_run(run)
        client.evalsha.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_workflow_run_duplicate(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=0)  # Already exists
        run = WorkflowRun(id="run-1", workflow_name="wf", status=WorkflowStatus.PENDING)
        await backend.create_workflow_run(run)
        # Pipeline should not be called for indexes
        pipe.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_workflow_run_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"run-1",
                b"workflow_name": b"wf",
                b"workflow_version": b"1",
                b"namespace": b"default",
                b"status": b"pending",
                b"current_step": b"",
                b"input_data": b"",
                b"output_data": b"",
                b"error_message": b"",
                b"error_traceback": b"",
                b"parent_run_id": b"",
                b"created_at": b"2025-01-01T00:00:00+00:00",
                b"updated_at": b"2025-01-01T00:00:00+00:00",
                b"completed_at": b"",
                b"deadline_at": b"",
            }
        )
        result = await backend.get_workflow_run("run-1")
        assert result is not None
        assert result.id == "run-1"

    @pytest.mark.asyncio
    async def test_get_workflow_run_not_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.hgetall = AsyncMock(return_value={})
        result = await backend.get_workflow_run("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_workflow_status_terminal(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.hget = AsyncMock(return_value=b"running")
        await backend.update_workflow_status(
            "run-1",
            WorkflowStatus.COMPLETED,
            error_message="done",
            output_data=b"result",
        )
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_workflow_status_nonterminal(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.hget = AsyncMock(return_value=b"pending")
        await backend.update_workflow_status("run-1", WorkflowStatus.RUNNING)
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_list_workflow_runs_single_set(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"run-1"})
        client.zrevrange = AsyncMock(return_value=[b"run-1"])
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"run-1",
                b"workflow_name": b"wf",
                b"workflow_version": b"1",
                b"namespace": b"default",
                b"status": b"pending",
                b"current_step": b"",
                b"input_data": b"",
                b"output_data": b"",
                b"error_message": b"",
                b"error_traceback": b"",
                b"parent_run_id": b"",
                b"created_at": b"2025-01-01T00:00:00+00:00",
                b"updated_at": b"",
                b"completed_at": b"",
                b"deadline_at": b"",
            }
        )
        result = await backend.list_workflow_runs()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_workflow_runs_with_filters(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.sinter = AsyncMock(return_value=set())
        client.zrevrange = AsyncMock(return_value=[])
        result = await backend.list_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_count_workflow_runs(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"run-1", b"run-2"})
        result = await backend.count_workflow_runs()
        assert result == 2

    @pytest.mark.asyncio
    async def test_count_with_filters(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        # First call: ns ids, second: status ids, third: wf ids
        client.smembers = AsyncMock(
            side_effect=[
                {b"run-1", b"run-2"},  # ns
                {b"run-1"},  # status
                {b"run-1"},  # workflow
            ]
        )
        result = await backend.count_workflow_runs(
            status=WorkflowStatus.FAILED,
            workflow_name="wf",
        )
        assert result == 1

    @pytest.mark.asyncio
    async def test_get_incomplete_runs(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(
            side_effect=[
                {b"run-1"},  # running
                set(),  # pending
            ]
        )
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"run-1",
                b"workflow_name": b"wf",
                b"workflow_version": b"1",
                b"namespace": b"default",
                b"status": b"running",
                b"current_step": b"",
                b"input_data": b"",
                b"output_data": b"",
                b"error_message": b"",
                b"error_traceback": b"",
                b"parent_run_id": b"",
                b"created_at": b"2025-01-01T00:00:00+00:00",
                b"updated_at": b"",
                b"completed_at": b"",
                b"deadline_at": b"",
            }
        )
        result = await backend.get_incomplete_runs()
        assert len(result) == 1


class TestRedisStepOutputs:
    @pytest.mark.asyncio
    async def test_save_step_output(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=100,
        )
        await backend.save_step_output(so)
        client.evalsha.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_step_outputs(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"1"})
        client.hgetall = AsyncMock(
            return_value={
                b"workflow_run_id": b"run-1",
                b"step_order": b"1",
                b"step_name": b"s1",
                b"output_data": b"",
                b"output_type": b"",
                b"duration_ms": b"100",
                b"retry_count": b"0",
                b"status": b"completed",
                b"error_message": b"",
                b"created_at": b"2025-01-01T00:00:00+00:00",
            }
        )
        result = await backend.get_step_outputs("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_step_output_not_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.hgetall = AsyncMock(return_value={})
        result = await backend.get_step_output("run-1", 99)
        assert result is None


class TestRedisPendingSteps:
    @pytest.mark.asyncio
    async def test_enqueue_step(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        ps = PendingStep(workflow_run_id="run-1", step_order=1, priority=5, max_retries=3)
        await backend.enqueue_step(ps)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_claim_step_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=b"1")
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"1",
                b"workflow_run_id": b"run-1",
                b"step_order": b"1",
                b"priority": b"5",
                b"status": b"running",
                b"worker_id": b"w-1",
                b"retry_count": b"0",
                b"max_retries": b"3",
            }
        )
        result = await backend.claim_step("w-1")
        assert result is not None

    @pytest.mark.asyncio
    async def test_claim_step_empty(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=None)
        result = await backend.claim_step("w-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_complete_step(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        so = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
        )
        await backend.complete_step(1, so)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        client.hget = AsyncMock(return_value=b"5")
        await backend.fail_step(1, error_message="boom", retry_at=retry_at)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_fail_step_no_retry(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.fail_step(1, error_message="fatal")
        client.hset.assert_awaited()


class TestRedisSignals:
    @pytest.mark.asyncio
    async def test_send_signal(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        sig = Signal(workflow_run_id="run-1", signal_name="go", signal_data=b"data")
        await backend.send_signal(sig)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_consume_signal_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=b"1")
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"1",
                b"workflow_run_id": b"run-1",
                b"signal_name": b"go",
                b"signal_data": b"data",
                b"consumed": b"1",
                b"created_at": b"2025-01-01T00:00:00+00:00",
            }
        )
        result = await backend.consume_signal("run-1", "go")
        assert result is not None

    @pytest.mark.asyncio
    async def test_consume_signal_not_found(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=None)
        result = await backend.consume_signal("run-1", "go")
        assert result is None

    @pytest.mark.asyncio
    async def test_register_signal_wait(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        wait = SignalWait(
            workflow_run_id="run-1",
            signal_name="go",
            timeout_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        await backend.register_signal_wait(wait)
        client.hset.assert_awaited()


class TestRedisCompensation:
    @pytest.mark.asyncio
    async def test_save_compensation(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        comp = Compensation(
            workflow_run_id="run-1",
            step_order=1,
            handler_name="undo",
            step_output=b"data",
            status="pending",
        )
        await backend.save_compensation(comp)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_compensations(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"1"})
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"1",
                b"workflow_run_id": b"run-1",
                b"step_order": b"2",
                b"handler_name": b"undo",
                b"step_output": b"data",
                b"status": b"completed",
            }
        )
        result = await backend.get_compensations("run-1")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_compensation_status(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.update_compensation_status(1, "completed", error_message="ok")
        pipe.execute.assert_awaited()


class TestRedisScheduling:
    @pytest.mark.asyncio
    async def test_save_schedule(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        sched = Schedule(
            id="s-1",
            workflow_name="wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        await backend.save_schedule(sched)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_due_schedules(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"s-1"})
        now_str = _dt_to_str(datetime(2020, 1, 1, tzinfo=timezone.utc))
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"s-1",
                b"workflow_name": b"wf",
                b"schedule_type": b"cron",
                b"schedule_config": b"* * * * *",
                b"namespace": b"default",
                b"enabled": b"1",
                b"last_run_at": b"",
                b"next_run_at": now_str.encode(),
            }
        )
        result = await backend.get_due_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_schedule_last_run(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        await backend.update_schedule_last_run("s-1", now, now)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_all_enabled(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"s-1"})
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"s-1",
                b"workflow_name": b"wf",
                b"schedule_type": b"cron",
                b"schedule_config": b"* * * * *",
                b"namespace": b"default",
                b"enabled": b"1",
                b"last_run_at": b"",
                b"next_run_at": b"",
            }
        )
        result = await backend.get_all_enabled_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_all_enabled_skips_disabled(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"s-1"})
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"s-1",
                b"workflow_name": b"wf",
                b"schedule_type": b"cron",
                b"schedule_config": b"* * * * *",
                b"namespace": b"default",
                b"enabled": b"0",
                b"last_run_at": b"",
                b"next_run_at": b"",
            }
        )
        result = await backend.get_all_enabled_schedules()
        assert result == []

    @pytest.mark.asyncio
    async def test_list_all_schedules(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"s-1"})
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"s-1",
                b"workflow_name": b"wf",
                b"schedule_type": b"cron",
                b"schedule_config": b"* * * * *",
                b"namespace": b"default",
                b"enabled": b"1",
                b"last_run_at": b"",
                b"next_run_at": b"",
            }
        )
        result = await backend.list_all_schedules()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_all_schedules_skips_empty(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"s-1"})
        client.hgetall = AsyncMock(return_value={})
        result = await backend.list_all_schedules()
        assert result == []


class TestRedisLocks:
    @pytest.mark.asyncio
    async def test_acquire_lock_success(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.set = AsyncMock(return_value=True)
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_lock_existing_own(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.set = AsyncMock(return_value=None)  # nx=True failed
        client.get = AsyncMock(return_value=b"holder-1")
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is True
        client.expire.assert_awaited()

    @pytest.mark.asyncio
    async def test_acquire_lock_conflict(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.set = AsyncMock(return_value=None)
        client.get = AsyncMock(return_value=b"someone-else")
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is False

    @pytest.mark.asyncio
    async def test_acquire_lock_none_current(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.set = AsyncMock(return_value=None)
        client.get = AsyncMock(return_value=None)
        result = await backend.acquire_lock("my-lock", "holder-1", 60)
        assert result is False

    @pytest.mark.asyncio
    async def test_release_lock(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=1)
        result = await backend.release_lock("my-lock", "holder-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_release_lock_not_held(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=0)
        result = await backend.release_lock("my-lock", "holder-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_refresh_lock(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=1)
        result = await backend.refresh_lock("my-lock", "holder-1", 120)
        assert result is True

    @pytest.mark.asyncio
    async def test_refresh_lock_not_held(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.evalsha = AsyncMock(return_value=0)
        result = await backend.refresh_lock("my-lock", "holder-1", 120)
        assert result is False


class TestRedisDLQ:
    @pytest.mark.asyncio
    async def test_add_to_dlq(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        entry = DLQEntry(workflow_run_id="run-1", step_order=1, error_message="boom")
        await backend.add_to_dlq(entry)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_list_dlq(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.zrevrange = AsyncMock(return_value=[b"1"])
        client.hgetall = AsyncMock(
            return_value={
                b"id": b"1",
                b"workflow_run_id": b"run-1",
                b"step_order": b"1",
                b"error_message": b"boom",
                b"error_traceback": b"",
                b"retry_count": b"0",
            }
        )
        result = await backend.list_dlq()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_dlq_skips_empty(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.zrevrange = AsyncMock(return_value=[b"1"])
        client.hgetall = AsyncMock(return_value={})
        result = await backend.list_dlq()
        assert result == []

    @pytest.mark.asyncio
    async def test_remove_from_dlq(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.remove_from_dlq(1)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_purge_dlq(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"1", b"2"})
        result = await backend.purge_dlq()
        assert result == 2
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_purge_dlq_empty(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value=set())
        result = await backend.purge_dlq()
        assert result == 0


class TestRedisWorkers:
    @pytest.mark.asyncio
    async def test_register_worker(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        w = WorkerInfo(worker_id="w-1", node_id="node-1", status=WorkerStatus.ACTIVE)
        await backend.register_worker(w)
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_worker_heartbeat_with_task(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.worker_heartbeat("w-1", current_task="run-1")
        client.hset.assert_awaited()

    @pytest.mark.asyncio
    async def test_worker_heartbeat_no_task(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.worker_heartbeat("w-1")
        client.hset.assert_awaited()

    @pytest.mark.asyncio
    async def test_deregister_worker(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.deregister_worker("w-1")
        pipe.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_list_workers(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"w-1"})
        client.hgetall = AsyncMock(
            return_value={
                b"worker_id": b"w-1",
                b"node_id": b"node-1",
                b"status": b"active",
            }
        )
        result = await backend.list_workers()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_workers_skips_empty(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"w-1"})
        client.hgetall = AsyncMock(return_value={})
        result = await backend.list_workers()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_stale_workers(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"w-1"})
        old_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
        client.hgetall = AsyncMock(
            return_value={
                b"worker_id": b"w-1",
                b"node_id": b"node-1",
                b"status": b"active",
                b"last_heartbeat": _dt_to_str(old_dt).encode(),
            }
        )
        result = await backend.get_stale_workers(300)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_stale_workers_no_heartbeat(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"w-1"})
        client.hgetall = AsyncMock(
            return_value={
                b"worker_id": b"w-1",
                b"node_id": b"node-1",
                b"status": b"active",
                b"last_heartbeat": b"",
            }
        )
        result = await backend.get_stale_workers(300)
        assert len(result) == 1


class TestRedisParallelResults:
    @pytest.mark.asyncio
    async def test_checkpoint_parallel_item(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"result")
        client.set.assert_awaited()
        client.sadd.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_parallel_results(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"0", b"1"})
        client.get = AsyncMock(side_effect=[b"r0", b"r1"])
        result = await backend.get_parallel_results("run-1", 1)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_parallel_results_skips_none(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(return_value={b"0"})
        client.get = AsyncMock(return_value=None)
        result = await backend.get_parallel_results("run-1", 1)
        assert result == {}


class TestRedisConcurrency:
    @pytest.mark.asyncio
    async def test_check_concurrency_limit(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(
            side_effect=[
                {b"run-1"},  # workflow
                {b"run-1"},  # ns
                {b"run-1"},  # running
                set(),  # pending
            ]
        )
        assert await backend.check_concurrency_limit("wf", "default", 2) is True

    @pytest.mark.asyncio
    async def test_check_concurrency_exceeded(self, redis_backend) -> None:
        backend, client, pipe = redis_backend
        client.smembers = AsyncMock(
            side_effect=[
                {b"run-1", b"run-2", b"run-3"},  # workflow
                {b"run-1", b"run-2", b"run-3"},  # ns
                {b"run-1", b"run-2"},  # running
                {b"run-3"},  # pending
            ]
        )
        assert await backend.check_concurrency_limit("wf", "default", 3) is False


class TestBackendAutoDetection:
    def test_create_mysql_backend(self) -> None:
        from gravtory.backends import create_backend
        from gravtory.backends.mysql import MySQLBackend as _MySQLBackend

        b = create_backend("mysql://localhost/test")
        assert isinstance(b, _MySQLBackend)

    def test_create_mongodb_backend(self) -> None:
        from gravtory.backends import create_backend
        from gravtory.backends.mongodb import MongoDBBackend

        b = create_backend("mongodb://localhost:27017/test")
        assert isinstance(b, MongoDBBackend)

    def test_create_redis_backend(self) -> None:
        from gravtory.backends import create_backend

        b = create_backend("redis://localhost:6379/0")
        assert isinstance(b, RedisBackend)

    def test_create_redis_ssl_backend(self) -> None:
        from gravtory.backends import create_backend

        b = create_backend("rediss://localhost:6380/0")
        assert isinstance(b, RedisBackend)

    def test_create_mongodb_srv_backend(self) -> None:
        from gravtory.backends import create_backend
        from gravtory.backends.mongodb import MongoDBBackend

        b = create_backend("mongodb+srv://cluster.example.com/test")
        assert isinstance(b, MongoDBBackend)

    def test_unknown_backend_raises(self) -> None:
        import pytest

        from gravtory.backends import create_backend

        with pytest.raises(Exception, match="Unknown backend"):
            create_backend("ftp://localhost/db")
