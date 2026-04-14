"""Tests for the Scheduler engine — leader election, schedule triggering, catchup."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    Schedule,
    ScheduleType,
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)
from gravtory.scheduling.engine import Scheduler


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _setup() -> tuple[InMemoryBackend, WorkflowRegistry, ExecutionEngine]:
    backend = InMemoryBackend()
    registry = WorkflowRegistry()

    async def dummy_step() -> str:
        return "ok"

    defn = WorkflowDefinition(
        name="test-wf",
        version=1,
        steps={1: StepDefinition(order=1, name="s1", function=dummy_step)},
        config=WorkflowConfig(),
    )
    registry.register(defn)
    engine = ExecutionEngine(registry, backend)
    return backend, registry, engine


class TestSchedulerLeaderElection:
    @pytest.mark.asyncio
    async def test_acquires_leader_lock(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
            leader_ttl=30.0,
        )
        await scheduler.start()
        await asyncio.sleep(0.15)
        assert scheduler.is_leader
        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_second_node_not_leader(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        s1 = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
            leader_ttl=30.0,
        )
        s2 = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-2",
            check_interval=0.05,
            leader_ttl=30.0,
        )
        await s1.start()
        await asyncio.sleep(0.1)
        await s2.start()
        await asyncio.sleep(0.1)
        assert s1.is_leader
        assert not s2.is_leader
        await s1.stop()
        await s2.stop()

    @pytest.mark.asyncio
    async def test_shutdown_releases_lock(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
        )
        await scheduler.start()
        await asyncio.sleep(0.1)
        await scheduler.stop()
        assert not scheduler.is_leader
        # Lock should be released
        assert "gravtory_scheduler" not in backend._locks


class TestSchedulerTriggers:
    @pytest.mark.asyncio
    async def test_due_schedule_triggers_workflow(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        sched = Schedule(
            id="sched-1",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            enabled=True,
            next_run_at=_now() - timedelta(seconds=10),
            created_at=_now() - timedelta(minutes=5),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
        )
        await scheduler.start()
        await asyncio.sleep(0.2)
        await scheduler.stop()

        # Check that a workflow run was created
        runs = [r for r in backend._runs.values() if r.workflow_name == "test-wf"]
        assert len(runs) >= 1
        assert runs[0].status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING)

    @pytest.mark.asyncio
    async def test_idempotent_trigger(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        sched = Schedule(
            id="sched-2",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            enabled=True,
            next_run_at=_now() - timedelta(seconds=5),
            created_at=_now() - timedelta(minutes=5),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
        )
        await scheduler.start()
        await asyncio.sleep(0.15)
        await scheduler.stop()

        runs = [r for r in backend._runs.values() if r.workflow_name == "test-wf"]
        # Should only trigger once (idempotent)
        assert len(runs) == 1

    @pytest.mark.asyncio
    async def test_schedule_update_after_trigger(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        sched = Schedule(
            id="sched-3",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="3600",
            enabled=True,
            next_run_at=_now() - timedelta(seconds=5),
            created_at=_now() - timedelta(hours=2),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            check_interval=0.05,
        )
        await scheduler.start()
        await asyncio.sleep(0.15)
        await scheduler.stop()

        updated = backend._schedules["sched-3"]
        assert updated.last_run_at is not None
        assert updated.next_run_at is not None
        assert updated.next_run_at > _now()


class TestMissedRunCatchup:
    @pytest.mark.asyncio
    async def test_catchup_all(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        # Schedule that should have fired 3 times (every 60s, 3 minutes overdue)
        sched = Schedule(
            id="sched-catchup",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            enabled=True,
            next_run_at=_now() - timedelta(minutes=3),
            last_run_at=_now() - timedelta(minutes=4),
            created_at=_now() - timedelta(hours=1),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            catchup_policy="all",
        )
        count = await scheduler.catchup_missed_runs()
        assert count >= 3

    @pytest.mark.asyncio
    async def test_catchup_latest(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        sched = Schedule(
            id="sched-catchup-latest",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            enabled=True,
            next_run_at=_now() - timedelta(minutes=3),
            last_run_at=_now() - timedelta(minutes=4),
            created_at=_now() - timedelta(hours=1),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            catchup_policy="latest",
        )
        count = await scheduler.catchup_missed_runs()
        assert count == 1

    @pytest.mark.asyncio
    async def test_catchup_none(self) -> None:
        backend, registry, engine = _setup()
        await backend.initialize()
        sched = Schedule(
            id="sched-catchup-none",
            workflow_name="test-wf",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            enabled=True,
            next_run_at=_now() - timedelta(minutes=3),
            last_run_at=_now() - timedelta(minutes=4),
            created_at=_now() - timedelta(hours=1),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend,
            registry,
            engine,
            node_id="node-1",
            catchup_policy="none",
        )
        count = await scheduler.catchup_missed_runs()
        assert count == 0


class TestSchedulerGapFill:
    """Gap-fill tests for scheduler edge cases."""

    @pytest.mark.asyncio
    async def test_catchup_no_schedules(self) -> None:
        """Catchup with no schedules returns 0."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        engine = ExecutionEngine(backend, registry)
        scheduler = Scheduler(backend, registry, engine, node_id="node-gap")
        count = await scheduler.catchup_missed_runs()
        assert count == 0

    @pytest.mark.asyncio
    async def test_is_leader_initially_false(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        engine = ExecutionEngine(backend, registry)
        scheduler = Scheduler(backend, registry, engine, node_id="node-gap")
        assert scheduler.is_leader is False

    @pytest.mark.asyncio
    async def test_is_running_initially_false(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        engine = ExecutionEngine(backend, registry)
        scheduler = Scheduler(backend, registry, engine, node_id="node-gap")
        assert scheduler.is_running is False
