"""Tests for full DLQ management — DLQManager."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import DLQEntry, WorkflowRun, WorkflowStatus
from gravtory.enterprise.dlq_manager import DLQInspection, DLQManager, DLQRetryRule


class TestDLQManagement:
    """DLQManager inspection, auto-retry, alerting, and purge tests."""

    @pytest.fixture()
    async def backend(self) -> InMemoryBackend:
        b = InMemoryBackend()
        await b.initialize()
        return b

    @pytest.fixture()
    async def seeded_backend(self) -> InMemoryBackend:
        """Backend with a workflow run and DLQ entries."""
        b = InMemoryBackend()
        await b.initialize()
        await b.create_workflow_run(
            WorkflowRun(
                id="run-1",
                workflow_name="OrderWorkflow",
                namespace="default",
                status=WorkflowStatus.FAILED,
            )
        )
        await b.add_to_dlq(
            DLQEntry(
                workflow_run_id="run-1",
                step_order=1,
                error_message="ConnectionError: timeout",
                retry_count=0,
            )
        )
        await b.add_to_dlq(
            DLQEntry(
                workflow_run_id="run-1",
                step_order=2,
                error_message="ValueError: bad input",
                retry_count=0,
            )
        )
        return b

    @pytest.mark.asyncio()
    async def test_dlq_inspect(self, seeded_backend: InMemoryBackend) -> None:
        """Inspection returns full details including workflow context."""
        manager = DLQManager(seeded_backend)
        entries = await seeded_backend.list_dlq()
        assert len(entries) >= 1
        entry_id = entries[0].id
        assert entry_id is not None

        inspection = await manager.inspect(entry_id)
        assert inspection is not None
        assert isinstance(inspection, DLQInspection)
        assert inspection.workflow_run_id == "run-1"
        assert inspection.workflow_name == "OrderWorkflow"
        assert inspection.workflow_status == "failed"

    @pytest.mark.asyncio()
    async def test_dlq_inspect_not_found(self, backend: InMemoryBackend) -> None:
        """Inspection of non-existent entry returns None."""
        manager = DLQManager(backend)
        result = await manager.inspect(99999)
        assert result is None

    @pytest.mark.asyncio()
    async def test_dlq_auto_retry_matching(self, seeded_backend: InMemoryBackend) -> None:
        """Auto-retry processes entries matching the error pattern."""
        rules = [
            DLQRetryRule(
                error_pattern="ConnectionError",
                delay=timedelta(seconds=0),  # No delay — eligible immediately
                max_retries=3,
            ),
        ]
        manager = DLQManager(seeded_backend, rules=rules)

        retried = await manager.process_auto_retry()
        # Only the ConnectionError entry should match
        assert retried == 1

    @pytest.mark.asyncio()
    async def test_dlq_auto_retry_no_match(self, seeded_backend: InMemoryBackend) -> None:
        """Auto-retry skips entries not matching any rule."""
        rules = [
            DLQRetryRule(
                error_pattern="SomeOtherError",
                max_retries=3,
            ),
        ]
        manager = DLQManager(seeded_backend, rules=rules)
        retried = await manager.process_auto_retry()
        assert retried == 0

    @pytest.mark.asyncio()
    async def test_dlq_auto_retry_respects_max_retries(
        self,
        backend: InMemoryBackend,
    ) -> None:
        """Auto-retry skips entries that have exceeded max_retries."""
        await backend.add_to_dlq(
            DLQEntry(
                workflow_run_id="run-1",
                step_order=1,
                error_message="ConnectionError: timeout",
                retry_count=5,  # Already retried 5 times
            )
        )
        rules = [
            DLQRetryRule(error_pattern="ConnectionError", max_retries=3),
        ]
        manager = DLQManager(backend, rules=rules)
        retried = await manager.process_auto_retry()
        assert retried == 0

    @pytest.mark.asyncio()
    async def test_dlq_threshold_alert(self, backend: InMemoryBackend) -> None:
        """Threshold alert fires when DLQ size exceeds threshold."""
        # Add entries exceeding threshold
        for i in range(5):
            await backend.add_to_dlq(
                DLQEntry(
                    workflow_run_id=f"run-{i}",
                    step_order=1,
                    error_message="error",
                )
            )

        alerts_fired: list[dict[str, Any]] = []

        async def alert_cb(name: str, details: dict[str, Any]) -> None:
            alerts_fired.append({"name": name, **details})

        manager = DLQManager(
            backend,
            alert_callback=alert_cb,
            alert_threshold=3,
        )
        exceeded = await manager.check_threshold()
        assert exceeded is True
        assert len(alerts_fired) == 1
        assert alerts_fired[0]["name"] == "dlq_threshold"
        assert alerts_fired[0]["size"] == 5

    @pytest.mark.asyncio()
    async def test_dlq_threshold_not_exceeded(self, backend: InMemoryBackend) -> None:
        """Threshold check returns False when under limit."""
        await backend.add_to_dlq(
            DLQEntry(
                workflow_run_id="run-1",
                step_order=1,
                error_message="error",
            )
        )
        manager = DLQManager(backend, alert_threshold=100)
        exceeded = await manager.check_threshold()
        assert exceeded is False

    @pytest.mark.asyncio()
    async def test_dlq_purge_all(self, seeded_backend: InMemoryBackend) -> None:
        """Purge without age filter removes all entries."""
        manager = DLQManager(seeded_backend)
        initial = await manager.count()
        assert initial >= 2

        purged = await manager.purge()
        assert purged == initial
        assert await manager.count() == 0

    @pytest.mark.asyncio()
    async def test_dlq_retry_entry(self, seeded_backend: InMemoryBackend) -> None:
        """retry_entry re-enqueues a specific entry and removes it from DLQ."""
        manager = DLQManager(seeded_backend)
        entries = await manager.list_entries()
        entry_id = entries[0].id
        assert entry_id is not None

        success = await manager.retry_entry(entry_id)
        assert success is True

        # Entry should be removed from DLQ
        remaining = await manager.list_entries()
        remaining_ids = [e.id for e in remaining]
        assert entry_id not in remaining_ids

    @pytest.mark.asyncio()
    async def test_dlq_auto_retry_respects_delay(
        self,
        backend: InMemoryBackend,
    ) -> None:
        """Auto-retry skips entries that are younger than the rule's delay."""
        await backend.add_to_dlq(
            DLQEntry(
                workflow_run_id="run-1",
                step_order=1,
                error_message="ConnectionError: timeout",
                retry_count=0,
            )
        )
        # Rule with 1-hour delay — entry was just created so it's too young
        rules = [
            DLQRetryRule(
                error_pattern="ConnectionError",
                delay=timedelta(hours=1),
                max_retries=3,
            ),
        ]
        manager = DLQManager(backend, rules=rules)
        retried = await manager.process_auto_retry()
        assert retried == 0  # Skipped because entry is too young

    def test_dlq_retry_rule_defaults(self) -> None:
        """DLQRetryRule has sensible defaults."""
        rule = DLQRetryRule(error_pattern="Error")
        assert rule.delay == timedelta(minutes=5)
        assert rule.max_retries == 3


class TestDLQGapFill:
    """Gap-fill tests for DLQ management edge cases."""

    @pytest.mark.asyncio()
    async def test_count_empty(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        manager = DLQManager(backend)
        assert await manager.count() == 0

    @pytest.mark.asyncio()
    async def test_purge_empty(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        manager = DLQManager(backend)
        purged = await manager.purge()
        assert purged == 0

    @pytest.mark.asyncio()
    async def test_threshold_zero(self) -> None:
        """With alert_threshold=0, any entry exceeds the threshold."""
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.add_to_dlq(
            DLQEntry(
                workflow_run_id="r",
                step_order=1,
                error_message="e",
            )
        )
        manager = DLQManager(backend, alert_threshold=0)
        assert await manager.check_threshold() is True
