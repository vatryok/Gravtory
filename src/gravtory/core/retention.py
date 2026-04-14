# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Data retention policy — automatic cleanup of old workflow data.

Provides :class:`RetentionPolicy` that periodically purges completed
workflow runs, step outputs, and DLQ entries older than a configurable
TTL.  This prevents unbounded database growth in long-running deployments.

Usage::

    from gravtory.core.retention import RetentionPolicy

    policy = RetentionPolicy(
        backend=backend,
        completed_ttl_days=90,
        failed_ttl_days=180,
        dlq_ttl_days=30,
    )
    # Run once (e.g. from a scheduled task or CLI command)
    stats = await policy.enforce()
    print(f"Purged {stats.workflows_deleted} workflows, {stats.dlq_deleted} DLQ entries")

    # Or run as a background loop
    task = await policy.start_background(interval_hours=24)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.core.retention")


@dataclass
class RetentionStats:
    """Summary of a single retention enforcement run."""

    workflows_deleted: int = 0
    dlq_deleted: int = 0
    duration_ms: float = 0.0
    errors: list[str] | None = None


class RetentionPolicy:
    """Configurable data retention with TTL-based cleanup.

    Args:
        backend: The storage backend to purge from.
        completed_ttl_days: Delete completed workflows older than this.
            Set to 0 to disable.
        failed_ttl_days: Delete failed workflows older than this.
            Set to 0 to disable.
        dlq_ttl_days: Delete DLQ entries older than this.
            Set to 0 to disable.
        dry_run: If True, log what would be deleted without actually deleting.
        namespace: Only purge data in this namespace.
    """

    def __init__(
        self,
        backend: Backend,
        *,
        completed_ttl_days: int = 90,
        failed_ttl_days: int = 180,
        dlq_ttl_days: int = 30,
        dry_run: bool = False,
        namespace: str = "default",
    ) -> None:
        self._backend = backend
        self._completed_ttl = timedelta(days=completed_ttl_days) if completed_ttl_days > 0 else None
        self._failed_ttl = timedelta(days=failed_ttl_days) if failed_ttl_days > 0 else None
        self._dlq_ttl = timedelta(days=dlq_ttl_days) if dlq_ttl_days > 0 else None
        self._dry_run = dry_run
        self._namespace = namespace
        self._task: asyncio.Task[None] | None = None

    async def enforce(self) -> RetentionStats:
        """Run one retention enforcement pass.

        Scans completed/failed workflows and DLQ entries, deleting those
        older than the configured TTL.  Returns a summary of what was purged.
        """
        import time

        start = time.monotonic()
        stats = RetentionStats()
        errors: list[str] = []
        now = datetime.now(tz=timezone.utc)

        # Purge completed workflows
        if self._completed_ttl is not None:
            cutoff = now - self._completed_ttl
            try:
                count = await self._purge_workflows_before(cutoff, status="completed")
                stats.workflows_deleted += count
                logger.info(
                    "Retention: purged %d completed workflows older than %s",
                    count,
                    cutoff.isoformat(),
                )
            except Exception as exc:
                errors.append(f"completed purge failed: {exc}")
                logger.error("Retention error (completed): %s", exc)

        # Purge failed workflows
        if self._failed_ttl is not None:
            cutoff = now - self._failed_ttl
            try:
                count = await self._purge_workflows_before(cutoff, status="failed")
                stats.workflows_deleted += count
                logger.info(
                    "Retention: purged %d failed workflows older than %s",
                    count,
                    cutoff.isoformat(),
                )
            except Exception as exc:
                errors.append(f"failed purge failed: {exc}")
                logger.error("Retention error (failed): %s", exc)

        # Purge old DLQ entries
        if self._dlq_ttl is not None:
            cutoff = now - self._dlq_ttl
            try:
                count = await self._purge_dlq_before(cutoff)
                stats.dlq_deleted += count
                logger.info(
                    "Retention: purged %d DLQ entries older than %s",
                    count,
                    cutoff.isoformat(),
                )
            except Exception as exc:
                errors.append(f"DLQ purge failed: {exc}")
                logger.error("Retention error (DLQ): %s", exc)

        stats.duration_ms = (time.monotonic() - start) * 1000
        stats.errors = errors if errors else None

        logger.info(
            "Retention pass complete: %d workflows + %d DLQ entries purged in %.1fms%s",
            stats.workflows_deleted,
            stats.dlq_deleted,
            stats.duration_ms,
            " (DRY RUN)" if self._dry_run else "",
        )
        return stats

    async def _purge_workflows_before(self, cutoff: datetime, *, status: str) -> int:
        """Delete workflow runs with the given status completed before cutoff."""
        from gravtory.core.types import WorkflowStatus

        status_enum = WorkflowStatus(status)
        runs = await self._backend.list_workflow_runs(
            namespace=self._namespace,
            status=status_enum,
            limit=10000,
        )

        count = 0
        for run in runs:
            completed_at = getattr(run, "completed_at", None) or getattr(run, "updated_at", None)
            if completed_at is None:
                continue
            if completed_at < cutoff:
                if not self._dry_run:
                    await self._backend.delete_workflow_run(run.id)
                count += 1
        return count

    async def _purge_dlq_before(self, cutoff: datetime) -> int:
        """Delete DLQ entries older than cutoff."""
        entries = await self._backend.list_dlq(namespace=self._namespace, limit=10000)
        count = 0
        for entry in entries:
            if entry.created_at is not None and entry.created_at < cutoff:
                if not self._dry_run:
                    await self._backend.remove_from_dlq(entry.id)  # type: ignore[arg-type]
                count += 1
        return count

    async def start_background(self, *, interval_hours: float = 24) -> asyncio.Task[None]:
        """Start a background task that runs enforce() periodically.

        Returns the asyncio Task so the caller can cancel it on shutdown.
        """

        async def _loop() -> None:
            while True:
                try:
                    await self.enforce()
                except Exception:
                    logger.exception("Retention background pass failed")
                await asyncio.sleep(interval_hours * 3600)

        self._task = asyncio.create_task(_loop())
        logger.info("Retention background task started (interval=%sh)", interval_hours)
        return self._task

    def stop_background(self) -> None:
        """Cancel the background retention task if running."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            logger.info("Retention background task stopped")
