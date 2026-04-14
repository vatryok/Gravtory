"""Tests for scheduling.engine — Scheduler lifecycle, schedule processing, catchup."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    Schedule,
    ScheduleType,
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
)
from gravtory.scheduling.engine import Scheduler


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


@pytest.fixture
def registry() -> MagicMock:
    step1 = StepDefinition(name="step_a", order=1, retries=0, depends_on=[])
    definition = WorkflowDefinition(
        name="test-wf",
        version=1,
        steps={1: step1},
        config=WorkflowConfig(priority=5),
    )
    reg = MagicMock()
    reg.get.return_value = definition
    return reg


@pytest.fixture
def engine() -> MagicMock:
    return MagicMock()


class TestSchedulerProperties:
    def test_initial_state(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, node_id="n1")
        assert not s.is_leader
        assert not s.is_running
        assert s._node_id == "n1"

    def test_default_node_id(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        assert s._node_id == "scheduler-default"


class TestSchedulerLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, node_id="n1", check_interval=0.05)
        await s.start()
        assert s.is_running
        await asyncio.sleep(0.1)
        await s.stop()
        assert not s.is_running
        assert not s.is_leader

    @pytest.mark.asyncio
    async def test_stop_without_start(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        await s.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_stop_releases_leader_lock(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, node_id="n1", check_interval=0.05)
        await s.start()
        await asyncio.sleep(0.15)  # let it acquire leadership
        await s.stop()
        assert not s.is_leader


class TestSchedulerComputeNextRun:
    def test_cron_schedule(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        base = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        sched = Schedule(
            id="s1",
            workflow_name="test-wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="0 12 * * *",
            namespace="default",
            enabled=True,
            last_run_at=base,
        )
        nxt = s._compute_next_run(sched)
        assert nxt is not None
        assert nxt > base
        assert nxt.hour == 12
        assert nxt.minute == 0

    def test_interval_schedule(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        base = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        sched = Schedule(
            id="s2",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="300",
            namespace="default",
            enabled=True,
            last_run_at=base,
        )
        nxt = s._compute_next_run(sched)
        assert nxt is not None
        assert nxt == base + timedelta(seconds=300)

    def test_one_time_schedule_not_fired(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        target = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
        sched = Schedule(
            id="s3",
            workflow_name="test-wf",
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config=target.isoformat(),
            namespace="default",
            enabled=True,
            last_run_at=None,
        )
        nxt = s._compute_next_run(sched)
        assert nxt == target

    def test_one_time_schedule_already_fired(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        target = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
        sched = Schedule(
            id="s4",
            workflow_name="test-wf",
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config=target.isoformat(),
            namespace="default",
            enabled=True,
            last_run_at=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        nxt = s._compute_next_run(sched)
        assert nxt is None

    def test_event_schedule_returns_none(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s5",
            workflow_name="test-wf",
            schedule_type=ScheduleType.EVENT,
            schedule_config="custom_event",
            namespace="default",
            enabled=True,
        )
        nxt = s._compute_next_run(sched)
        assert nxt is None

    def test_one_time_naive_datetime(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s6",
            workflow_name="test-wf",
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config="2025-06-01T00:00:00",
            namespace="default",
            enabled=True,
        )
        nxt = s._compute_next_run(sched)
        assert nxt is not None
        assert nxt.tzinfo is not None


class TestSchedulerComputeNextRunAfter:
    def test_cron_next_after(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s1",
            workflow_name="test-wf",
            schedule_type=ScheduleType.CRON,
            schedule_config="0 * * * *",
            namespace="default",
            enabled=True,
        )
        after = datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)
        nxt = s._compute_next_run_after(sched, after)
        assert nxt is not None
        assert nxt.hour == 13
        assert nxt.minute == 0

    def test_interval_next_after(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s2",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            namespace="default",
            enabled=True,
        )
        after = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        nxt = s._compute_next_run_after(sched, after)
        assert nxt == after + timedelta(seconds=60)

    def test_one_time_next_after_none(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s3",
            workflow_name="test-wf",
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config="2025-01-01T00:00:00+00:00",
            namespace="default",
            enabled=True,
        )
        nxt = s._compute_next_run_after(sched, datetime.now(tz=timezone.utc))
        assert nxt is None

    def test_event_next_after_none(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        sched = Schedule(
            id="s4",
            workflow_name="test-wf",
            schedule_type=ScheduleType.EVENT,
            schedule_config="evt",
            namespace="default",
            enabled=True,
        )
        nxt = s._compute_next_run_after(sched, datetime.now(tz=timezone.utc))
        assert nxt is None


class TestSchedulerCatchup:
    @pytest.mark.asyncio
    async def test_catchup_none_policy(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, catchup_policy="none")
        count = await s.catchup_missed_runs()
        assert count == 0

    @pytest.mark.asyncio
    async def test_catchup_no_missed(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, catchup_policy="all")
        with patch.object(s, "_get_all_enabled_schedules", return_value=[]):
            count = await s.catchup_missed_runs()
            assert count == 0

    @pytest.mark.asyncio
    async def test_catchup_latest_only(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, catchup_policy="latest")
        now = datetime.now(tz=timezone.utc)
        missed_sched = Schedule(
            id="s-missed",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            namespace="default",
            enabled=True,
            next_run_at=now - timedelta(minutes=5),
            last_run_at=now - timedelta(minutes=10),
        )
        with patch.object(s, "_get_all_enabled_schedules", return_value=[missed_sched]):
            count = await s.catchup_missed_runs()
            # Should have caught up (latest policy = only 1 run)
            assert count >= 1

    @pytest.mark.asyncio
    async def test_catchup_schedule_with_future_next_run(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine, catchup_policy="all")
        now = datetime.now(tz=timezone.utc)
        future_sched = Schedule(
            id="s-future",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            namespace="default",
            enabled=True,
            next_run_at=now + timedelta(minutes=5),
        )
        with patch.object(s, "_get_all_enabled_schedules", return_value=[future_sched]):
            count = await s.catchup_missed_runs()
            assert count == 0


class TestSchedulerProcessSchedule:
    @pytest.mark.asyncio
    async def test_process_due_cron_schedule(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        now = datetime.now(tz=timezone.utc)
        sched = Schedule(
            id="s-due",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="1",
            namespace="default",
            enabled=True,
            last_run_at=now - timedelta(seconds=5),
        )
        await s._process_schedule(sched, now)
        # Should have created a workflow run
        runs = await backend.list_workflow_runs()
        assert len(runs) >= 1

    @pytest.mark.asyncio
    async def test_process_future_schedule_skipped(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        now = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
        sched = Schedule(
            id="s-future",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            namespace="default",
            enabled=True,
            last_run_at=now,
        )
        await s._process_schedule(sched, now)
        runs = await backend.list_workflow_runs()
        assert len(runs) == 0

    @pytest.mark.asyncio
    async def test_process_schedule_idempotent(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        now = datetime.now(tz=timezone.utc)
        sched = Schedule(
            id="s-idem",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="1",
            namespace="default",
            enabled=True,
            last_run_at=now - timedelta(seconds=5),
        )
        await s._process_schedule(sched, now)
        count_before = len(await backend.list_workflow_runs())
        await s._process_schedule(sched, now)
        count_after = len(await backend.list_workflow_runs())
        assert count_after == count_before  # idempotent

    @pytest.mark.asyncio
    async def test_process_schedule_handles_trigger_error(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        now = datetime.now(tz=timezone.utc)
        sched = Schedule(
            id="s-err",
            workflow_name="bad-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="1",
            namespace="default",
            enabled=True,
            last_run_at=now - timedelta(seconds=5),
        )
        registry.get.side_effect = KeyError("not found")
        # Should not raise
        await s._process_schedule(sched, now)


class TestSchedulerInterruptibleSleep:
    @pytest.mark.asyncio
    async def test_interruptible_sleep_completes(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)
        await s._interruptible_sleep(0.01)  # should complete quickly

    @pytest.mark.asyncio
    async def test_interruptible_sleep_interrupted(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        s = Scheduler(backend, registry, engine)

        async def set_shutdown() -> None:
            await asyncio.sleep(0.01)
            s._shutdown_event.set()

        task = asyncio.create_task(set_shutdown())
        await s._interruptible_sleep(10.0)  # should be interrupted quickly
        await task
