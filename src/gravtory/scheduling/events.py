# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Event-driven triggers and workflow chaining.

Provides:
  - EventBus: in-process event emission and subscription
  - EventTrigger: links an event name to a workflow
  - Workflow chaining: trigger workflow B when workflow A completes
"""

from __future__ import annotations

import fnmatch
import inspect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from gravtory.backends.base import Backend
    from gravtory.core.execution import ExecutionEngine
    from gravtory.core.registry import WorkflowRegistry
    from gravtory.core.types import Schedule

logger = logging.getLogger("gravtory.events")


@dataclass
class EventTrigger:
    """Links a named event to a workflow for automatic triggering."""

    event_name: str
    workflow_name: str
    namespace: str = "default"


@dataclass
class EventSubscription:
    """Internal subscription record."""

    event_name: str
    callback: Callable[..., Any]


class EventBus:
    """In-process event bus for triggering workflows via custom events.

    Supports:
      - Custom event emission (``emit``)
      - Workflow completion chaining (``emit_workflow_completed``)
      - Subscriber registration from schedule metadata
    """

    def __init__(
        self,
        backend: Backend,
        registry: WorkflowRegistry,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._engine = execution_engine
        self._triggers: list[EventTrigger] = []
        self._subscriptions: list[EventSubscription] = []

    def register_trigger(self, trigger: EventTrigger) -> None:
        """Register a trigger that fires a workflow when an event is emitted."""
        self._triggers.append(trigger)
        logger.debug(
            "Registered trigger: event '%s' → workflow '%s'",
            trigger.event_name,
            trigger.workflow_name,
        )

    def register_triggers_from_schedules(self, schedules: Sequence[Schedule]) -> None:
        """Scan schedule records for EVENT-type schedules and register triggers."""
        from gravtory.core.types import ScheduleType

        for sched in schedules:
            if sched.schedule_type == ScheduleType.EVENT and sched.enabled:
                config = sched.schedule_config
                if config.startswith("workflow:"):
                    # Workflow chaining: trigger after another workflow completes
                    parent_name = config[len("workflow:") :]
                    self.register_trigger(
                        EventTrigger(
                            event_name=f"workflow_completed:{parent_name}",
                            workflow_name=sched.workflow_name,
                            namespace=sched.namespace,
                        )
                    )
                else:
                    # Custom event trigger
                    self.register_trigger(
                        EventTrigger(
                            event_name=config,
                            workflow_name=sched.workflow_name,
                            namespace=sched.namespace,
                        )
                    )

    def subscribe(self, event_name: str, callback: Callable[..., Any]) -> None:
        """Register an arbitrary callback for a named event."""
        self._subscriptions.append(EventSubscription(event_name=event_name, callback=callback))

    def unsubscribe(self, event_name: str, callback: Callable[..., Any]) -> bool:
        """Remove a previously registered callback. Returns True if found."""
        for i, sub in enumerate(self._subscriptions):
            if sub.event_name == event_name and sub.callback is callback:
                self._subscriptions.pop(i)
                return True
        return False

    def remove_trigger(self, workflow_name: str, event_name: str | None = None) -> int:
        """Remove triggers for a workflow, optionally filtered by event name.

        Returns the number of triggers removed.
        """
        before = len(self._triggers)
        self._triggers = [
            t
            for t in self._triggers
            if not (
                t.workflow_name == workflow_name
                and (event_name is None or t.event_name == event_name)
            )
        ]
        removed = before - len(self._triggers)
        if removed:
            logger.debug("Removed %d trigger(s) for workflow '%s'", removed, workflow_name)
        return removed

    def clear(self) -> None:
        """Remove all triggers and subscriptions."""
        self._triggers.clear()
        self._subscriptions.clear()
        logger.debug("EventBus cleared all triggers and subscriptions")

    async def emit(self, event_name: str, data: dict[str, Any] | None = None) -> int:
        """Emit a named event, triggering all subscribed workflows.

        Returns the number of workflows triggered.
        """
        triggered = 0
        event_data = data or {}

        for trigger in self._triggers:
            if fnmatch.fnmatch(event_name, trigger.event_name):
                try:
                    await self._trigger_workflow(trigger, event_data)
                    triggered += 1
                except Exception:
                    logger.exception(
                        "Failed to trigger workflow '%s' for event '%s'",
                        trigger.workflow_name,
                        event_name,
                    )

        # Fire raw subscriptions
        for sub in self._subscriptions:
            if fnmatch.fnmatch(event_name, sub.event_name):
                try:
                    result = sub.callback(event_name, event_data)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception(
                        "Event subscription callback failed for '%s'",
                        event_name,
                    )

        if triggered == 0:
            logger.debug("Event '%s' emitted but no triggers matched", event_name)

        return triggered

    async def emit_workflow_completed(
        self,
        workflow_name: str,
        run_id: str,
        output_data: dict[str, Any] | None = None,
    ) -> int:
        """Emit a workflow-completed event for chaining.

        Called by the execution engine after a workflow completes successfully.
        Passes the parent workflow's output as input to chained workflows.
        """
        event_data = {
            "parent_workflow": workflow_name,
            "parent_run_id": run_id,
            **(output_data or {}),
        }
        return await self.emit(f"workflow_completed:{workflow_name}", event_data)

    async def _trigger_workflow(
        self,
        trigger: EventTrigger,
        event_data: dict[str, Any],
    ) -> None:
        """Create a workflow run from a trigger."""
        import uuid

        from gravtory.core.types import PendingStep, WorkflowRun, WorkflowStatus

        definition = self._registry.get(trigger.workflow_name)
        run_id = f"{trigger.workflow_name}-event-{uuid.uuid4().hex[:12]}"

        # Check idempotency
        existing = await self._backend.get_workflow_run(run_id)
        if existing is not None:
            return

        run = WorkflowRun(
            id=run_id,
            workflow_name=trigger.workflow_name,
            workflow_version=definition.version,
            namespace=trigger.namespace,
            status=WorkflowStatus.PENDING,
        )
        await self._backend.create_workflow_run(run)
        await self._backend.update_workflow_status(run_id, WorkflowStatus.RUNNING)

        # Enqueue root steps (those with no dependencies)
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

        logger.info(
            "Event '%s' triggered workflow '%s' (run_id=%s)",
            trigger.event_name,
            trigger.workflow_name,
            run_id,
        )
