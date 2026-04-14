"""Tests for the data retention / TTL policy."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.retention import RetentionPolicy, RetentionStats
from gravtory.core.types import DLQEntry, WorkflowRun, WorkflowStatus


async def _seed(backend: InMemoryBackend) -> None:
    """Insert old and recent workflow runs + DLQ entries."""
    await backend.initialize()
    now = datetime.now(tz=timezone.utc)

    # Old completed run (120 days ago)
    old_run = WorkflowRun(
        id="old-completed",
        workflow_name="TestWF",
        status=WorkflowStatus.COMPLETED,
        completed_at=now - timedelta(days=120),
    )
    await backend.create_workflow_run(old_run)

    # Recent completed run (10 days ago)
    recent_run = WorkflowRun(
        id="recent-completed",
        workflow_name="TestWF",
        status=WorkflowStatus.COMPLETED,
        completed_at=now - timedelta(days=10),
    )
    await backend.create_workflow_run(recent_run)

    # Old failed run (200 days ago)
    old_failed = WorkflowRun(
        id="old-failed",
        workflow_name="TestWF",
        status=WorkflowStatus.FAILED,
        completed_at=now - timedelta(days=200),
    )
    await backend.create_workflow_run(old_failed)

    # Running run (should never be purged)
    running = WorkflowRun(
        id="running-1",
        workflow_name="TestWF",
        status=WorkflowStatus.RUNNING,
    )
    await backend.create_workflow_run(running)

    # Old DLQ entry
    dlq_old = DLQEntry(
        id="dlq-old",
        workflow_run_id="old-failed",
        step_order=1,
        error_message="boom",
        created_at=now - timedelta(days=60),
    )
    await backend.add_to_dlq(dlq_old)

    # Recent DLQ entry
    dlq_new = DLQEntry(
        id="dlq-new",
        workflow_run_id="recent-completed",
        step_order=1,
        error_message="oops",
        created_at=now - timedelta(days=5),
    )
    await backend.add_to_dlq(dlq_new)


class TestRetentionPolicy:
    @pytest.mark.asyncio
    async def test_enforce_purges_old_runs(self) -> None:
        backend = InMemoryBackend()
        await _seed(backend)

        policy = RetentionPolicy(
            backend,
            completed_ttl_days=90,
            failed_ttl_days=180,
            dlq_ttl_days=30,
        )
        stats = await policy.enforce()

        assert stats.workflows_deleted >= 2  # old-completed + old-failed
        assert stats.dlq_deleted >= 1  # dlq-old
        assert stats.errors is None

        # Recent run should still exist
        recent = await backend.get_workflow_run("recent-completed")
        assert recent is not None

        # Running run should still exist
        running = await backend.get_workflow_run("running-1")
        assert running is not None

        # Old run should be gone
        old = await backend.get_workflow_run("old-completed")
        assert old is None

    @pytest.mark.asyncio
    async def test_dry_run_does_not_delete(self) -> None:
        backend = InMemoryBackend()
        await _seed(backend)

        policy = RetentionPolicy(
            backend,
            completed_ttl_days=90,
            failed_ttl_days=180,
            dlq_ttl_days=30,
            dry_run=True,
        )
        stats = await policy.enforce()

        assert stats.workflows_deleted >= 2
        # But data should still exist since dry_run=True
        old = await backend.get_workflow_run("old-completed")
        assert old is not None

    @pytest.mark.asyncio
    async def test_zero_ttl_disables_purge(self) -> None:
        backend = InMemoryBackend()
        await _seed(backend)

        policy = RetentionPolicy(
            backend,
            completed_ttl_days=0,
            failed_ttl_days=0,
            dlq_ttl_days=0,
        )
        stats = await policy.enforce()

        assert stats.workflows_deleted == 0
        assert stats.dlq_deleted == 0

    @pytest.mark.asyncio
    async def test_stats_dataclass(self) -> None:
        stats = RetentionStats()
        assert stats.workflows_deleted == 0
        assert stats.dlq_deleted == 0
        assert stats.errors is None
