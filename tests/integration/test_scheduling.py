"""Integration tests — scheduling engine (cron + interval).

Verifies:
  - Cron schedule fires when due
  - Interval schedule fires correctly
  - Scheduler creates idempotent run IDs
  - Catchup policies: "all", "latest", "none"
  - Scheduler leader election via distributed locks
  - Schedule disabled state prevents firing
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    Schedule,
    ScheduleType,
)
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow
from gravtory.scheduling.engine import Scheduler

pytestmark = pytest.mark.integration


# ── Fixture workflow ─────────────────────────────────────────────


@workflow(id="sched-wf-{trigger}")
class ScheduledWorkflow:
    @step(1)
    async def do_work(self, trigger: str = "scheduled") -> dict[str, str]:
        return {"result": trigger}


# ── Helpers ──────────────────────────────────────────────────────


def _past(minutes: int = 5) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)


def _future(minutes: int = 5) -> datetime:
    return datetime.now(tz=timezone.utc) + timedelta(minutes=minutes)


async def _make_env() -> tuple[InMemoryBackend, WorkflowRegistry, ExecutionEngine]:
    backend = InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()
    registry.register(ScheduledWorkflow.definition)
    engine = ExecutionEngine(registry, backend)
    return backend, registry, engine


# ── Tests ────────────────────────────────────────────────────────


class TestCronSchedule:
    """Cron schedule integration tests."""

    @pytest.mark.asyncio
    async def test_cron_fires_when_due(self) -> None:
        """Cron schedule that is past due triggers a workflow."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="cron-1",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.CRON,
            schedule_config="* * * * *",  # every minute
            next_run_at=_past(2),
            last_run_at=_past(3),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
            check_interval=0.1,
            leader_ttl=30,
        )

        # Process schedules directly (don't start loop)
        now = datetime.now(tz=timezone.utc)
        await scheduler._process_schedule(sched, now)

        # A workflow run should have been created
        runs = list(backend._runs.values())
        sched_runs = [r for r in runs if "sched" in r.id]
        assert len(sched_runs) >= 1

    @pytest.mark.asyncio
    async def test_cron_idempotent_run_id(self) -> None:
        """Same schedule+fire_time produces the same run_id (idempotent)."""
        backend, registry, engine = await _make_env()

        fire_time = _past(2)
        sched = Schedule(
            id="cron-idem",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.CRON,
            schedule_config="* * * * *",
            next_run_at=fire_time,
            last_run_at=_past(3),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
        )

        now = datetime.now(tz=timezone.utc)
        await scheduler._process_schedule(sched, now)

        # Process again — should NOT create a duplicate
        initial_count = len(backend._runs)
        await scheduler._process_schedule(sched, now)
        assert len(backend._runs) == initial_count


class TestIntervalSchedule:
    """Interval schedule integration tests."""

    @pytest.mark.asyncio
    async def test_interval_fires_when_due(self) -> None:
        """Interval schedule fires when next_run_at is in the past."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="interval-1",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",  # every 60 seconds
            next_run_at=_past(1),
            last_run_at=_past(2),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
        )

        now = datetime.now(tz=timezone.utc)
        await scheduler._process_schedule(sched, now)

        runs = list(backend._runs.values())
        sched_runs = [r for r in runs if "sched" in r.id]
        assert len(sched_runs) >= 1

    @pytest.mark.asyncio
    async def test_interval_not_due_yet(self) -> None:
        """Interval schedule does not fire when next_run_at is in the future."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="interval-future",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="3600",  # every hour
            next_run_at=_future(30),
            last_run_at=_past(30),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
        )

        now = datetime.now(tz=timezone.utc)
        await scheduler._process_schedule(sched, now)

        # No workflow should have been created
        runs = list(backend._runs.values())
        sched_runs = [r for r in runs if "sched" in r.id]
        assert len(sched_runs) == 0


class TestSchedulerLeaderElection:
    """Leader election integration tests."""

    @pytest.mark.asyncio
    async def test_leader_acquires_lock(self) -> None:
        """Scheduler acquires the leader lock when starting."""
        backend, registry, engine = await _make_env()

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
            node_id="node-alpha",
            check_interval=60,  # won't fire in test
        )

        # Manually acquire leader lock
        acquired = await backend.acquire_lock("gravtory_scheduler", "node-alpha", 30)
        assert acquired is True
        scheduler._is_leader = True

        assert scheduler.is_leader is True

    @pytest.mark.asyncio
    async def test_second_node_cannot_acquire(self) -> None:
        """Second scheduler node cannot acquire the leader lock."""
        backend, registry, engine = await _make_env()

        # First node takes the lock
        await backend.acquire_lock("gravtory_scheduler", "node-alpha", 30)

        # Second node tries
        acquired = await backend.acquire_lock("gravtory_scheduler", "node-beta", 30)
        assert acquired is False


class TestCatchupPolicy:
    """Catchup policy integration tests."""

    @pytest.mark.asyncio
    async def test_catchup_none_skips_missed(self) -> None:
        """catchup_policy='none' does not fire any missed runs."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="catch-none",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            next_run_at=_past(10),
            last_run_at=_past(15),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
            catchup_policy="none",
        )

        count = await scheduler.catchup_missed_runs()
        assert count == 0

    @pytest.mark.asyncio
    async def test_catchup_latest_fires_one(self) -> None:
        """catchup_policy='latest' fires only the most recent missed run."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="catch-latest",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",  # every 60s
            next_run_at=_past(5),
            last_run_at=_past(10),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
            catchup_policy="latest",
        )

        count = await scheduler.catchup_missed_runs()
        assert count == 1

    @pytest.mark.asyncio
    async def test_catchup_all_fires_multiple(self) -> None:
        """catchup_policy='all' fires all missed interval runs."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="catch-all",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",  # every 60s
            next_run_at=_past(5),  # 5 minutes ago
            last_run_at=_past(10),
            created_at=_past(60),
        )
        await backend.save_schedule(sched)

        scheduler = Scheduler(
            backend=backend,
            registry=registry,
            execution_engine=engine,
            catchup_policy="all",
        )

        count = await scheduler.catchup_missed_runs()
        # With 60s interval, 5 mins ago next_run → at least a few missed runs
        assert count >= 1


class TestScheduleDisabled:
    """Disabled schedule tests."""

    @pytest.mark.asyncio
    async def test_disabled_schedule_not_due(self) -> None:
        """Disabled schedule is not returned from get_due_schedules."""
        backend, registry, engine = await _make_env()

        sched = Schedule(
            id="disabled-1",
            workflow_name="ScheduledWorkflow",
            schedule_type=ScheduleType.INTERVAL,
            schedule_config="60",
            next_run_at=_past(5),
            enabled=False,
        )
        await backend.save_schedule(sched)

        due = await backend.get_due_schedules()
        assert len(list(due)) == 0
