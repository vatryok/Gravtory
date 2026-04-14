# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Scheduler engine — leader-elected loop that evaluates due schedules.

Only ONE scheduler instance runs at a time across the cluster, enforced
via the ``gravtory_scheduler`` distributed lock.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal

from gravtory.core.types import (
    PendingStep,
    Schedule,
    ScheduleType,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.scheduling.cron import CronExpression
from gravtory.scheduling.interval import IntervalSchedule

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.execution import ExecutionEngine
    from gravtory.core.registry import WorkflowRegistry
    from gravtory.scheduling.events import EventBus

logger = logging.getLogger("gravtory.scheduler")

CatchupPolicy = Literal["all", "latest", "none"]

_LOCK_NAME = "gravtory_scheduler"


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class Scheduler:
    """Leader-elected scheduler that checks for due schedules and triggers workflows.

    Args:
        backend: Database backend for schedule persistence and locking.
        registry: Workflow registry for resolving workflow definitions.
        execution_engine: Engine used to trigger workflow runs.
        node_id: Unique identifier for this scheduler node.
        check_interval: Seconds between schedule evaluation cycles.
        leader_ttl: Seconds before the leader lock expires.
        catchup_policy: How to handle missed runs after downtime.
        event_bus: Optional event bus for workflow-completed chaining.
    """

    def __init__(
        self,
        backend: Backend,
        registry: WorkflowRegistry,
        execution_engine: ExecutionEngine,
        *,
        node_id: str = "",
        check_interval: float = 1.0,
        leader_ttl: float = 30.0,
        catchup_policy: CatchupPolicy = "all",
        event_bus: EventBus | None = None,
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._engine = execution_engine
        self._node_id = node_id or "scheduler-default"
        self._check_interval = check_interval
        self._leader_ttl = leader_ttl
        self._catchup_policy: CatchupPolicy = catchup_policy
        self._event_bus = event_bus
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the scheduler loop as an asyncio task."""
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler starting on node '%s'", self._node_id)

    async def stop(self) -> None:
        """Stop the scheduler and release leader lock."""
        self._shutdown_event.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._is_leader:
            await self._backend.release_lock(_LOCK_NAME, self._node_id)
            self._is_leader = False
        logger.info("Scheduler stopped on node '%s'", self._node_id)

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop — acquire leader lock, evaluate schedules."""
        while not self._shutdown_event.is_set():
            try:
                # Attempt to acquire or renew leader lock
                self._is_leader = await self._backend.acquire_lock(
                    _LOCK_NAME, self._node_id, int(self._leader_ttl)
                )

                if not self._is_leader:
                    logger.debug(
                        "Node '%s' is not leader, retrying in 5s",
                        self._node_id,
                    )
                    await self._interruptible_sleep(5.0)
                    continue

                # Renew leader lock BEFORE processing to avoid TTL expiry
                # during long schedule evaluation (prevents duplicate evals)
                await self._backend.refresh_lock(_LOCK_NAME, self._node_id, int(self._leader_ttl))

                # Evaluate due schedules
                schedules = await self._backend.get_due_schedules()
                now = _now()

                for sched in schedules:
                    await self._process_schedule(sched, now)

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Scheduler loop error")

            await self._interruptible_sleep(self._check_interval)

    async def _process_schedule(self, sched: Schedule, now: datetime) -> None:
        """Process a single due schedule — trigger workflow, update timestamps."""
        next_run = self._compute_next_run(sched)
        if next_run is None or next_run > now:
            return

        # Deterministic run_id for idempotency
        run_id = f"{sched.workflow_name}-sched-{sched.id}-{next_run.isoformat()}"

        # Check idempotency
        existing = await self._backend.get_workflow_run(run_id)
        if existing is None:
            try:
                definition = self._registry.get(sched.workflow_name)
                await self._trigger_workflow(definition, run_id, sched)
                logger.info(
                    "Scheduled workflow triggered: %s (run_id=%s)",
                    sched.workflow_name,
                    run_id,
                )
            except Exception:
                logger.exception(
                    "Failed to trigger scheduled workflow '%s'",
                    sched.workflow_name,
                )

        # Update schedule timestamps
        next_after = self._compute_next_run_after(sched, now)
        await self._backend.update_schedule_last_run(
            sched.id,
            last_run_at=now,
            next_run_at=next_after,
        )

    async def _trigger_workflow(
        self,
        definition: WorkflowDefinition,
        run_id: str,
        sched: Schedule,
    ) -> None:
        """Create a workflow run and enqueue its root steps."""
        run = WorkflowRun(
            id=run_id,
            workflow_name=definition.name,
            workflow_version=definition.version,
            namespace=sched.namespace,
            status=WorkflowStatus.PENDING,
        )
        await self._backend.create_workflow_run(run)
        await self._backend.update_workflow_status(run_id, WorkflowStatus.RUNNING)

        for order, step_def in definition.steps.items():
            if not step_def.depends_on:
                await self._backend.enqueue_step(
                    PendingStep(
                        workflow_run_id=run_id,
                        step_order=order,
                        priority=definition.config.priority,
                        max_retries=step_def.retries,
                    )
                )

    def _compute_next_run(self, sched: Schedule) -> datetime | None:
        """Compute the next fire time for a schedule based on its type."""
        if sched.schedule_type == ScheduleType.CRON:
            base = sched.last_run_at or sched.created_at or _now() - timedelta(minutes=1)
            cron = CronExpression(sched.schedule_config)
            return cron.next_fire_time(base)

        if sched.schedule_type == ScheduleType.INTERVAL:
            seconds = float(sched.schedule_config)
            base = sched.last_run_at or sched.created_at or _now()
            interval = IntervalSchedule(seconds=seconds)
            return interval.next_fire_time(base)

        if sched.schedule_type == ScheduleType.ONE_TIME:
            target = datetime.fromisoformat(sched.schedule_config)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            if sched.last_run_at is not None:
                return None  # Already fired
            return target

        # EVENT type — not time-based
        return None

    def _compute_next_run_after(self, sched: Schedule, after: datetime) -> datetime | None:
        """Compute next run time AFTER a given datetime (for DB update)."""
        if sched.schedule_type == ScheduleType.CRON:
            cron = CronExpression(sched.schedule_config)
            return cron.next_fire_time(after)

        if sched.schedule_type == ScheduleType.INTERVAL:
            seconds = float(sched.schedule_config)
            return after + timedelta(seconds=seconds)

        if sched.schedule_type == ScheduleType.ONE_TIME:
            return None  # Won't fire again

        return None

    _CATCHUP_BATCH_SIZE = 10
    _CATCHUP_BATCH_DELAY = 1.0  # seconds between batches
    _CATCHUP_MAX_PER_SCHEDULE = 100  # safety limit per schedule

    async def catchup_missed_runs(self) -> int:
        """Detect and fire missed scheduled runs after downtime.

        Respects the configured catchup_policy:
          - "all": fire all missed runs (batched to prevent thundering herd)
          - "latest": only fire the most recent missed run
          - "none": skip missed runs entirely

        Returns the total number of catchup runs triggered.
        """
        if self._catchup_policy == "none":
            return 0

        now = _now()
        total = 0

        # Get all enabled schedules (not just due ones)
        all_schedules = await self._get_all_enabled_schedules()

        for sched in all_schedules:
            if sched.next_run_at is None or sched.next_run_at >= now:
                continue

            # This schedule has missed at least one run
            missed: list[datetime] = []
            current = sched.next_run_at

            while current < now and len(missed) < self._CATCHUP_MAX_PER_SCHEDULE:
                missed.append(current)
                nxt = self._compute_next_run_after(sched, current)
                if nxt is None or nxt <= current:
                    break
                current = nxt

            if not missed:
                continue

            if self._catchup_policy == "latest":
                missed = [missed[-1]]

            count = 0
            for i, fire_time in enumerate(missed):
                # Throttle: pause between batches to avoid burst-loading the system
                if i > 0 and i % self._CATCHUP_BATCH_SIZE == 0:
                    logger.info(
                        "Catchup batch pause for '%s' (%d/%d triggered so far)",
                        sched.workflow_name,
                        count,
                        len(missed),
                    )
                    await asyncio.sleep(self._CATCHUP_BATCH_DELAY)

                run_id = f"{sched.workflow_name}-sched-catchup-{fire_time.isoformat()}"
                existing = await self._backend.get_workflow_run(run_id)
                if existing is None:
                    try:
                        definition = self._registry.get(sched.workflow_name)
                        await self._trigger_workflow(definition, run_id, sched)
                        count += 1
                    except Exception:
                        logger.exception(
                            "Failed to trigger catchup run for '%s'",
                            sched.workflow_name,
                        )

            if count > 0:
                logger.warning(
                    "Caught up %d missed runs for '%s'",
                    count,
                    sched.workflow_name,
                )
                total += count

            # Update schedule to current time
            next_after = self._compute_next_run_after(sched, now)
            await self._backend.update_schedule_last_run(
                sched.id,
                last_run_at=now,
                next_run_at=next_after,
            )

        return total

    async def _get_all_enabled_schedules(self) -> list[Schedule]:
        """Get all enabled schedules from the backend."""
        return list(await self._backend.get_all_enabled_schedules())

    async def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep that can be interrupted by the shutdown event."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=seconds)
