# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Multi-tenancy admin — cross-namespace operations for Gravtory.

Provides :class:`GravtoryAdmin` for listing namespaces, gathering
per-namespace statistics, and migrating workflows between namespaces.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gravtory.core.types import WorkflowRun, WorkflowStatus

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.enterprise.admin")

# Map WorkflowStatus values to NamespaceStats field names
_STATUS_FIELD_MAP: dict[WorkflowStatus, str] = {
    WorkflowStatus.PENDING: "pending",
    WorkflowStatus.RUNNING: "running",
    WorkflowStatus.COMPLETED: "completed",
    WorkflowStatus.FAILED: "failed",
    WorkflowStatus.CANCELLED: "cancelled",
    WorkflowStatus.COMPENSATED: "compensated",
}


@dataclass
class NamespaceStats:
    """Per-namespace workflow statistics."""

    namespace: str = ""
    total: int = 0
    pending: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    compensated: int = 0
    extra: dict[str, int] = field(default_factory=dict)


class GravtoryAdmin:
    """Admin interface for cross-namespace operations.

    Namespaces must be registered explicitly — the Backend interface
    filters by namespace, so automatic discovery is not possible.

    Usage::

        admin = GravtoryAdmin(backend, namespaces=["team-a", "team-b"])
        await admin.initialize()

        namespaces = admin.list_namespaces()
        stats = await admin.stats_by_namespace()
        count = await admin.migrate_namespace("old-team", "new-team")
    """

    def __init__(
        self,
        backend: Backend,
        namespaces: list[str] | None = None,
    ) -> None:
        self._backend = backend
        self._namespaces: set[str] = set(namespaces or ["default"])

    async def initialize(self) -> None:
        """Ensure the backend is ready."""
        await self._backend.initialize()

    async def close(self) -> None:
        """Release backend resources."""
        await self._backend.close()

    # ── Namespace registry ────────────────────────────────────────

    def register_namespace(self, namespace: str) -> None:
        """Register a namespace so it can be queried by admin operations."""
        self._namespaces.add(namespace)

    def unregister_namespace(self, namespace: str) -> None:
        """Remove a namespace from the registry."""
        self._namespaces.discard(namespace)

    def list_namespaces(self) -> list[str]:
        """Return all registered namespaces, sorted."""
        return sorted(self._namespaces)

    # ── Statistics ────────────────────────────────────────────────

    async def stats_by_namespace(
        self,
        namespaces: list[str] | None = None,
    ) -> dict[str, NamespaceStats]:
        """Get workflow counts per namespace.

        Args:
            namespaces: Specific namespaces to query.
                If None, queries all registered namespaces.

        Returns:
            Mapping of namespace → NamespaceStats.
        """
        targets = namespaces if namespaces is not None else self.list_namespaces()

        result: dict[str, NamespaceStats] = {}
        for ns in targets:
            stats = NamespaceStats(namespace=ns)
            for status in WorkflowStatus:
                runs = await self._backend.list_workflow_runs(
                    namespace=ns,
                    status=status,
                    limit=10000,
                )
                count = len(runs)
                attr = _STATUS_FIELD_MAP.get(status)
                if attr is not None:
                    setattr(stats, attr, count)
                else:
                    stats.extra[status.value] = count
                stats.total += count
            result[ns] = stats
        return result

    # ── Migration ─────────────────────────────────────────────────

    async def migrate_namespace(self, from_ns: str, to_ns: str) -> int:
        """Move all workflows from one namespace to another.

        Fetches every run in *from_ns*, changes its namespace to
        *to_ns*, and updates the namespace registry accordingly.

        Returns:
            Count of migrated workflow runs.
        """
        runs = await self._backend.list_workflow_runs(
            namespace=from_ns,
            limit=10000,
        )
        count = 0
        for run in runs:
            run.namespace = to_ns
            await self._backend.update_workflow_status(run.id, run.status)
            count += 1

        if count > 0:
            self._namespaces.add(to_ns)
            logger.info(
                "Migrated %d runs from namespace '%s' to '%s'",
                count,
                from_ns,
                to_ns,
            )
        return count

    # ── Cross-namespace queries ───────────────────────────────────

    async def list_runs_all_namespaces(
        self,
        *,
        namespaces: list[str] | None = None,
        status: WorkflowStatus | None = None,
        limit: int = 100,
    ) -> list[WorkflowRun]:
        """List workflow runs across multiple namespaces.

        Args:
            namespaces: Namespaces to query.
                If None, queries all registered namespaces.
            status: Optional status filter.
            limit: Max runs per namespace.

        Returns:
            Combined list of WorkflowRun objects.
        """
        targets = namespaces if namespaces is not None else self.list_namespaces()

        all_runs: list[WorkflowRun] = []
        for ns in targets:
            runs = await self._backend.list_workflow_runs(
                namespace=ns,
                status=status,
                limit=limit,
            )
            all_runs.extend(runs)
        return all_runs
