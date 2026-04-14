# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Audit logging — track all significant actions for compliance and debugging.

Provides :class:`AuditLogger` that records and queries audit log entries.

When constructed with a ``backend``, entries are persisted to the
database so they survive process restarts — a hard requirement for
production compliance.  A dedicated ``gravtory.audit`` logger emits
every entry at ``INFO`` level so standard log aggregation pipelines
(ELK, Datadog, etc.) ingest them as well.

Without a backend the logger falls back to an in-memory list, which
is **only suitable for unit tests**.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.enterprise.audit")
audit_file_logger = logging.getLogger("gravtory.audit")


# ── Tracked action constants ─────────────────────────────────────────

WORKFLOW_CREATED = "workflow.created"
WORKFLOW_COMPLETED = "workflow.completed"
WORKFLOW_FAILED = "workflow.failed"
WORKFLOW_RETRIED = "workflow.retried"
WORKFLOW_CANCELLED = "workflow.cancelled"
STEP_COMPLETED = "step.completed"
STEP_FAILED = "step.failed"
STEP_RETRIED = "step.retried"
SIGNAL_SENT = "signal.sent"
SCHEDULE_CREATED = "schedule.created"
SCHEDULE_TOGGLED = "schedule.toggled"
SCHEDULE_TRIGGERED = "schedule.triggered"
DLQ_RETRIED = "dlq.retried"
DLQ_PURGED = "dlq.purged"
CONFIG_CHANGED = "config.changed"


@dataclass
class AuditEntry:
    """A single audit log record."""

    id: int | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    namespace: str = "default"
    actor: str = "system"
    action: str = ""
    resource_type: str = ""
    resource_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    ip_address: str | None = None


class AuditLogger:
    """Records and queries audit log entries.

    Usage::

        audit = AuditLogger(namespace="finance")
        await audit.log(
            actor="user:alice",
            action="workflow.created",
            resource_type="workflow",
            resource_id="run-123",
            details={"workflow_name": "OrderWorkflow"},
        )

        entries = await audit.query(action="workflow.created", limit=10)
    """

    def __init__(
        self,
        namespace: str = "default",
        *,
        backend: Backend | None = None,
    ) -> None:
        self._namespace = namespace
        self._backend = backend
        self._entries: list[AuditEntry] = []
        self._next_id = 1
        self._warned_no_backend = False

    @property
    def namespace(self) -> str:
        return self._namespace

    async def log(
        self,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> AuditEntry:
        """Record an audit log entry.

        Args:
            actor: Who performed the action (e.g. "system", "user:alice", "worker:w1").
            action: Action identifier (e.g. "workflow.created").
            resource_type: Type of resource (e.g. "workflow", "step", "schedule", "dlq").
            resource_id: ID of the affected resource.
            details: Optional action-specific details dict.
            ip_address: Optional IP address for API/dashboard actions.

        Returns:
            The created AuditEntry.
        """
        entry = AuditEntry(
            id=self._next_id,
            timestamp=datetime.now(tz=timezone.utc),
            namespace=self._namespace,
            actor=actor,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
        )
        self._next_id += 1
        self._entries.append(entry)

        # Always emit structured log for external log aggregation
        audit_file_logger.info(
            json.dumps(
                {
                    "audit": True,
                    "namespace": entry.namespace,
                    "actor": entry.actor,
                    "action": entry.action,
                    "resource_type": entry.resource_type,
                    "resource_id": entry.resource_id,
                    "timestamp": entry.timestamp.isoformat(),
                    "details": entry.details,
                },
                default=str,
            )
        )

        if self._backend is None and not self._warned_no_backend:
            logger.warning(
                "AuditLogger has no persistent backend — audit entries will be "
                "lost on process restart. Pass a backend for production use."
            )
            self._warned_no_backend = True

        logger.debug(
            "AUDIT [%s] %s %s %s/%s",
            entry.namespace,
            entry.actor,
            entry.action,
            entry.resource_type,
            entry.resource_id,
        )
        return entry

    async def query(
        self,
        *,
        namespace: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        actor: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """Query audit log entries with optional filters.

        All filters are AND-combined. Results are ordered newest-first.

        Args:
            namespace: Filter by namespace.
            action: Filter by action (e.g. "workflow.failed").
            resource_type: Filter by resource type.
            resource_id: Filter by specific resource ID.
            actor: Filter by actor.
            since: Only entries on or after this time.
            until: Only entries on or before this time.
            limit: Maximum number of entries to return.

        Returns:
            List of matching AuditEntry objects, newest first.
        """
        target_ns = namespace or self._namespace
        results: list[AuditEntry] = []

        for entry in reversed(self._entries):
            if entry.namespace != target_ns:
                continue
            if action is not None and entry.action != action:
                continue
            if resource_type is not None and entry.resource_type != resource_type:
                continue
            if resource_id is not None and entry.resource_id != resource_id:
                continue
            if actor is not None and entry.actor != actor:
                continue
            if since is not None and entry.timestamp < since:
                continue
            if until is not None and entry.timestamp > until:
                continue
            results.append(entry)
            if len(results) >= limit:
                break

        return results

    async def count(
        self,
        *,
        namespace: str | None = None,
        action: str | None = None,
    ) -> int:
        """Count audit entries matching the given filters."""
        entries = await self.query(
            namespace=namespace,
            action=action,
            limit=1_000_000,
        )
        return len(entries)

    def clear(self) -> None:
        """Clear all audit entries (useful for testing)."""
        self._entries.clear()
        self._next_id = 1
