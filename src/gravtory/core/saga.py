# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Saga coordinator — compensation handling for workflow rollback.

When a workflow step fails, the SagaCoordinator runs compensation
handlers for all previously completed steps in REVERSE order.
Compensation is best-effort: if one handler fails, the remaining
handlers still execute.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from gravtory.core.types import Compensation, DLQEntry, StepStatus, WorkflowStatus

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.checkpoint import CheckpointEngine
    from gravtory.core.registry import WorkflowRegistry
    from gravtory.core.types import StepResult, WorkflowDefinition

logger = logging.getLogger("gravtory.saga")


class SagaCoordinator:
    """Manages compensation handlers for saga-enabled workflows."""

    def __init__(self, backend: Backend, registry: WorkflowRegistry) -> None:
        self._backend = backend
        self._registry = registry

    async def register(
        self,
        run_id: str,
        step_order: int,
        step_name: str,
        handler_name: str,
        step_output: bytes | None,
    ) -> None:
        """Register a compensation handler for a completed step.

        Called after each step with ``compensate=`` successfully completes.
        """
        comp = Compensation(
            workflow_run_id=run_id,
            step_order=step_order,
            handler_name=handler_name,
            step_output=step_output,
            status=StepStatus.PENDING,
        )
        await self._backend.save_compensation(comp)

    async def trigger(
        self,
        run_id: str,
        failed_step: int,
        definition: WorkflowDefinition,
        completed_steps: dict[int, StepResult],
        checkpoint_engine: CheckpointEngine | None = None,
    ) -> None:
        """Execute compensation handlers in REVERSE order.

        Algorithm:
          1. Update workflow status to ``compensating``.
          2. Iterate completed steps in descending order.
          3. For each step with a ``compensate`` handler:
             a. Skip if already completed (resume support).
             b. Lookup handler function on workflow class.
             c. Call handler(step_output).
             d. On success: mark as completed.
             e. On failure: mark as failed, add to DLQ, continue (best-effort).
          4. Final status: ``compensated`` if all succeeded,
             ``compensation_failed`` if any failed.
        """
        await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.COMPENSATING)

        any_failed = False

        # Instantiate workflow class so compensation methods get a proper 'self'
        workflow_instance = None
        if definition.workflow_class is not None:
            try:
                workflow_instance = definition.workflow_class()
            except TypeError:
                workflow_instance = None

        # Process in reverse step order
        for order in sorted(completed_steps.keys(), reverse=True):
            step_def = definition.steps.get(order)
            if step_def is None or step_def.compensate is None:
                continue

            result = completed_steps[order]
            if result.status != StepStatus.COMPLETED:
                continue

            handler_name = step_def.compensate
            try:
                handler = self._registry.get_compensation_handler(definition.name, handler_name)
            except Exception:
                logger.error(
                    "Compensation handler '%s' not found for step %d",
                    handler_name,
                    order,
                )
                any_failed = True
                continue

            # Bind the unbound method to the workflow instance
            if workflow_instance is not None:
                bound_handler = handler.__get__(workflow_instance, type(workflow_instance))
            else:
                bound_handler = handler

            try:
                output = result.output
                if asyncio.iscoroutinefunction(bound_handler):
                    await bound_handler(output)
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, bound_handler, output)
            except Exception as exc:
                logger.error(
                    "Compensation '%s' for step %d failed: %s",
                    handler_name,
                    order,
                    exc,
                )
                any_failed = True
                # Add to DLQ
                await self._backend.add_to_dlq(
                    DLQEntry(
                        workflow_run_id=run_id,
                        step_order=order,
                        error_message=f"Compensation '{handler_name}' failed: {exc}",
                    )
                )
                # Best-effort: continue with remaining compensations

        if any_failed:
            await self._backend.validated_update_workflow_status(
                run_id, WorkflowStatus.COMPENSATION_FAILED
            )
        else:
            await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.COMPENSATED)

    async def get_status(self, run_id: str) -> dict[str, int]:
        """Get compensation status for a run.

        Returns:
            Dict with keys: total, completed, failed, pending.
        """
        comps = await self._backend.get_compensations(run_id)
        total = len(comps)
        completed = sum(1 for c in comps if c.status == StepStatus.COMPLETED)
        failed = sum(1 for c in comps if c.status == StepStatus.FAILED)
        pending = total - completed - failed
        return {"total": total, "completed": completed, "failed": failed, "pending": pending}
