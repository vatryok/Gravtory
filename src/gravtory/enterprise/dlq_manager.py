# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Full DLQ management — inspection, auto-retry, alerting, and purge.

Provides :class:`DLQManager` for advanced dead letter queue operations
including rule-based auto-retry and threshold alerting.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from gravtory.core.types import DLQEntry, PendingStep

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from gravtory.backends.base import Backend

    AlertCallback = Callable[[str, dict[str, Any]], Awaitable[None]]

logger = logging.getLogger("gravtory.enterprise.dlq_manager")


@dataclass
class DLQRetryRule:
    """A rule that determines when DLQ entries should be auto-retried.

    Args:
        error_pattern: Regex pattern to match against the error_message.
        delay: Minimum age of the entry before it becomes eligible for
            auto-retry.  Entries younger than *delay* are skipped.
        max_retries: Maximum number of auto-retries for matching entries.
    """

    error_pattern: str
    delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    max_retries: int = 3


@dataclass
class DLQInspection:
    """Full details of a DLQ entry for human inspection."""

    entry_id: int | None
    workflow_run_id: str
    step_order: int
    error_message: str | None
    error_traceback: str | None
    step_input: bytes | None
    retry_count: int
    created_at: datetime | None
    workflow_name: str | None = None
    workflow_status: str | None = None
    namespace: str | None = None


class DLQManager:
    """Advanced dead letter queue management.

    Usage::

        manager = DLQManager(
            backend=backend,
            rules=[
                DLQRetryRule(error_pattern="ConnectionError", delay=timedelta(minutes=5)),
                DLQRetryRule(error_pattern="TimeoutError", delay=timedelta(minutes=15), max_retries=2),
            ],
            alert_callback=my_alert_fn,
            alert_threshold=50,
        )

        # Inspect an entry
        details = await manager.inspect(entry_id=42)

        # Process auto-retries
        retried = await manager.process_auto_retry()

        # Check threshold alerts
        await manager.check_threshold()

        # Purge old entries
        purged = await manager.purge(older_than=timedelta(days=7))
    """

    def __init__(
        self,
        backend: Backend,
        *,
        rules: list[DLQRetryRule] | None = None,
        alert_callback: AlertCallback | None = None,
        alert_threshold: int = 100,
        namespace: str = "default",
    ) -> None:
        self._backend = backend
        self._rules = rules or []
        self._alert_callback = alert_callback
        self._alert_threshold = alert_threshold
        self._namespace = namespace

    @property
    def rules(self) -> list[DLQRetryRule]:
        """Return the configured retry rules."""
        return list(self._rules)

    async def inspect(self, entry_id: int) -> DLQInspection | None:
        """Get full details of a DLQ entry including workflow context.

        Args:
            entry_id: The DLQ entry ID.

        Returns:
            DLQInspection with full context, or None if not found.
        """
        entry = await self._find_entry(entry_id)
        if entry is None:
            return None

        run = await self._backend.get_workflow_run(entry.workflow_run_id)
        return DLQInspection(
            entry_id=entry.id,
            workflow_run_id=entry.workflow_run_id,
            step_order=entry.step_order,
            error_message=entry.error_message,
            error_traceback=entry.error_traceback,
            step_input=entry.step_input,
            retry_count=entry.retry_count,
            created_at=entry.created_at,
            workflow_name=run.workflow_name if run else None,
            workflow_status=run.status.value if run else None,
            namespace=run.namespace if run else None,
        )

    async def list_entries(self, *, limit: int = 100) -> list[DLQEntry]:
        """List DLQ entries for the configured namespace."""
        entries = await self._backend.list_dlq(
            namespace=self._namespace,
            limit=limit,
        )
        return list(entries)

    async def retry_entry(self, entry_id: int) -> bool:
        """Retry a single DLQ entry by re-enqueuing its step.

        Removes the entry from the DLQ and creates a new pending step.

        Returns:
            True if the entry was found and retried.
        """
        target = await self._find_entry(entry_id)
        if target is None:
            return False

        step = PendingStep(
            workflow_run_id=target.workflow_run_id,
            step_order=target.step_order,
            retry_count=target.retry_count,
        )
        await self._backend.enqueue_step(step)
        await self._backend.remove_from_dlq(entry_id)
        logger.info("Retried DLQ entry %d for run %s", entry_id, target.workflow_run_id)
        return True

    async def process_auto_retry(self) -> int:
        """Process all DLQ entries against auto-retry rules.

        For each entry, checks if any rule's ``error_pattern`` matches,
        the entry hasn't exceeded ``max_retries``, and the entry is at
        least ``delay`` old.  Matching entries are re-enqueued.

        Returns:
            Number of entries retried.
        """
        if not self._rules:
            return 0

        entries = await self._backend.list_dlq(
            namespace=self._namespace,
            limit=10000,
        )

        # Collect IDs to retry first, then retry — avoids O(n²) re-fetching.
        ids_to_retry: list[int] = []
        now = datetime.now(tz=timezone.utc)

        for entry in entries:
            for rule in self._rules:
                if self._matches_rule(entry, rule, now):
                    assert entry.id is not None
                    ids_to_retry.append(entry.id)
                    break  # Only match first rule

        retried = 0
        for eid in ids_to_retry:
            success = await self.retry_entry(eid)
            if success:
                retried += 1

        return retried

    def _matches_rule(
        self,
        entry: DLQEntry,
        rule: DLQRetryRule,
        now: datetime,
    ) -> bool:
        """Check if a DLQ entry matches an auto-retry rule."""
        if entry.retry_count >= rule.max_retries:
            return False
        if entry.error_message is None:
            return False
        if not re.search(rule.error_pattern, entry.error_message):
            return False
        # Respect the delay — skip entries that are too young.
        return not (entry.created_at is not None and now - entry.created_at < rule.delay)

    async def check_threshold(self) -> bool:
        """Check if DLQ size exceeds the alert threshold.

        If exceeded and an alert_callback is configured, fires the alert.

        Returns:
            True if threshold was exceeded.
        """
        count = await self._backend.count_dlq(namespace=self._namespace)
        exceeded = count > self._alert_threshold

        if exceeded:
            logger.warning(
                "DLQ threshold exceeded: size=%d threshold=%d namespace=%s",
                count,
                self._alert_threshold,
                self._namespace,
                extra={
                    "alert_type": "dlq_threshold",
                    "dlq_size": count,
                    "dlq_threshold": self._alert_threshold,
                    "namespace": self._namespace,
                },
            )
            if self._alert_callback is not None:
                try:
                    await self._alert_callback(
                        "dlq_threshold",
                        {
                            "size": count,
                            "threshold": self._alert_threshold,
                            "namespace": self._namespace,
                        },
                    )
                except Exception:
                    logger.exception("DLQ threshold alert callback failed")

        return exceeded

    async def purge(
        self,
        *,
        older_than: timedelta | None = None,
    ) -> int:
        """Delete DLQ entries, optionally filtering by age.

        Args:
            older_than: If provided, only purge entries older than this duration.
                If None, purge all entries.

        Returns:
            Number of entries purged.
        """
        batch_size = 500
        now = datetime.now(tz=timezone.utc)
        purged = 0

        while True:
            entries = await self._backend.list_dlq(
                namespace=self._namespace,
                limit=batch_size,
            )
            if not entries:
                break
            batch_purged = 0
            for entry in entries:
                if (
                    older_than is not None
                    and entry.created_at is not None
                    and now - entry.created_at < older_than
                ):
                    continue
                assert entry.id is not None
                await self._backend.remove_from_dlq(entry.id)
                batch_purged += 1
            purged += batch_purged
            if batch_purged == 0 or len(entries) < batch_size:
                break

        logger.info("Purged %d DLQ entries (namespace=%s)", purged, self._namespace)
        return purged

    async def count(self) -> int:
        """Count DLQ entries for the configured namespace."""
        return await self._backend.count_dlq(namespace=self._namespace)

    # ── Internal helpers ──────────────────────────────────────────

    async def _find_entry(self, entry_id: int) -> DLQEntry | None:
        """Lookup a single DLQ entry by ID."""
        return await self._backend.get_dlq_entry(entry_id)
