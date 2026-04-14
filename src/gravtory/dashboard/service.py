# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Dashboard service layer — mediates between API handlers and backend.

All mutations go through this service so that:
  - State machine transitions are validated consistently
  - Audit logging is centralized
  - Metrics / observability hooks have a single attachment point
  - Future features (webhooks, RBAC) can be added here

This replaces direct backend calls from API handlers for write operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from gravtory.core.types import WorkflowStatus

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.enterprise.audit import AuditLogger

logger = logging.getLogger("gravtory.dashboard.service")


class DashboardService:
    """Thin service layer coordinating dashboard operations.

    Accepts an optional :class:`AuditLogger` for tracking all
    dashboard-initiated mutations.
    """

    def __init__(
        self,
        backend: Backend,
        *,
        audit_logger: AuditLogger | None = None,
    ) -> None:
        self._backend = backend
        self._audit = audit_logger

    @property
    def backend(self) -> Backend:
        return self._backend

    # ── Workflow operations ───────────────────────────────────────

    async def retry_workflow(self, run_id: str, *, actor: str = "dashboard") -> dict[str, Any]:
        """Retry a failed workflow run.

        Validates the run exists and is in a retryable state,
        resets status to PENDING, and logs the action.
        """
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            raise LookupError(f"Workflow run {run_id!r} not found")

        if run.status not in (WorkflowStatus.FAILED, WorkflowStatus.CANCELLED):
            raise ValueError(f"Cannot retry workflow in {run.status.value!r} state")

        await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.PENDING)
        logger.info("Workflow %s retried by %s", run_id, actor)

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="workflow.retried",
                resource_type="workflow",
                resource_id=run_id,
                details={"previous_status": run.status.value},
            )

        return {"run_id": run_id, "status": "retried"}

    async def cancel_workflow(self, run_id: str, *, actor: str = "dashboard") -> dict[str, Any]:
        """Cancel a running or pending workflow."""
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            raise LookupError(f"Workflow run {run_id!r} not found")

        if run.status not in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING):
            raise ValueError(f"Cannot cancel workflow in {run.status.value!r} state")

        await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.CANCELLED)
        logger.info("Workflow %s cancelled by %s", run_id, actor)

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="workflow.cancelled",
                resource_type="workflow",
                resource_id=run_id,
                details={"previous_status": run.status.value},
            )

        return {"run_id": run_id, "status": "cancelled"}

    async def send_signal(
        self,
        run_id: str,
        signal_name: str,
        data: bytes | None = None,
        *,
        actor: str = "dashboard",
    ) -> dict[str, Any]:
        """Send a signal to a workflow run."""
        from gravtory.core.types import Signal

        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            raise LookupError(f"Workflow run {run_id!r} not found")

        signal = Signal(workflow_run_id=run_id, signal_name=signal_name, signal_data=data)
        await self._backend.send_signal(signal)
        logger.info("Signal '%s' sent to %s by %s", signal_name, run_id, actor)

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="signal.sent",
                resource_type="workflow",
                resource_id=run_id,
                details={"signal_name": signal_name},
            )

        return {"run_id": run_id, "status": "signal_sent"}

    # ── DLQ operations ───────────────────────────────────────────

    async def retry_dlq_entry(self, entry_id: int, *, actor: str = "dashboard") -> dict[str, Any]:
        """Retry a DLQ entry: reset workflow status and remove from DLQ."""
        entry = await self._backend.get_dlq_entry(entry_id)
        if entry is None:
            raise LookupError(f"DLQ entry {entry_id!r} not found")

        await self._backend.validated_update_workflow_status(
            entry.workflow_run_id, WorkflowStatus.PENDING
        )
        await self._backend.remove_from_dlq(entry_id)

        logger.info(
            "DLQ entry %s retried (run %s) by %s",
            entry_id,
            entry.workflow_run_id,
            actor,
        )

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="dlq.retried",
                resource_type="dlq",
                resource_id=str(entry_id),
                details={"workflow_run_id": entry.workflow_run_id},
            )

        return {"entry_id": entry_id, "run_id": entry.workflow_run_id}

    async def purge_dlq(
        self, *, namespace: str = "default", actor: str = "dashboard"
    ) -> dict[str, Any]:
        """Purge all DLQ entries."""
        deleted = await self._backend.purge_dlq(namespace=namespace)
        logger.info("DLQ purged (%d entries) by %s", deleted, actor)

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="dlq.purged",
                resource_type="dlq",
                resource_id="*",
                details={"deleted": deleted, "namespace": namespace},
            )

        return {"deleted": deleted}

    # ── Schedule operations ──────────────────────────────────────

    async def toggle_schedule(
        self, schedule_id: str, *, actor: str = "dashboard"
    ) -> dict[str, Any]:
        """Toggle a schedule's enabled state."""
        found = await self._backend.get_schedule(schedule_id)
        if found is None:
            raise LookupError(f"Schedule {schedule_id!r} not found")

        found.enabled = not found.enabled
        await self._backend.save_schedule(found)
        logger.info(
            "Schedule %s toggled to %s by %s",
            schedule_id,
            "enabled" if found.enabled else "disabled",
            actor,
        )

        if self._audit:
            await self._audit.log(
                actor=actor,
                action="schedule.toggled",
                resource_type="schedule",
                resource_id=schedule_id,
                details={"enabled": found.enabled},
            )

        return {"id": schedule_id, "enabled": found.enabled}
