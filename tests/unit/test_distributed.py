"""Tests for distributed coordination — stale worker detection and task reclamation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    PendingStep,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
)
from gravtory.workers.distributed import detect_and_reclaim_stale_tasks


class TestStaleWorkerDetection:
    @pytest.mark.asyncio
    async def test_no_stale_workers(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(WorkerInfo(worker_id="w1", status=WorkerStatus.ACTIVE))
        await backend.worker_heartbeat("w1")
        reclaimed = await detect_and_reclaim_stale_tasks(
            backend, stale_threshold=timedelta(minutes=5)
        )
        assert reclaimed == 0
        workers = await backend.list_workers()
        assert len(workers) == 1

    @pytest.mark.asyncio
    async def test_stale_worker_detected_and_deregistered(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(WorkerInfo(worker_id="stale-w", status=WorkerStatus.ACTIVE))
        # Manually set heartbeat to old time
        worker = backend._workers["stale-w"]
        worker.last_heartbeat = datetime.now(tz=timezone.utc) - timedelta(minutes=10)

        await detect_and_reclaim_stale_tasks(backend, stale_threshold=timedelta(minutes=5))
        workers = await backend.list_workers()
        assert len(workers) == 0

    @pytest.mark.asyncio
    async def test_stale_worker_tasks_reclaimed(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(WorkerInfo(worker_id="stale-w", status=WorkerStatus.ACTIVE))
        worker = backend._workers["stale-w"]
        worker.last_heartbeat = datetime.now(tz=timezone.utc) - timedelta(minutes=10)

        # Add a task claimed by the stale worker
        step = PendingStep(
            workflow_run_id="run-1",
            step_order=1,
            max_retries=0,
        )
        await backend.enqueue_step(step)
        # Simulate claiming
        claimed = await backend.claim_step("stale-w")
        assert claimed is not None

        reclaimed = await detect_and_reclaim_stale_tasks(
            backend, stale_threshold=timedelta(minutes=5)
        )
        assert reclaimed == 1

        # Verify task is back to pending
        pending = [
            s
            for s in backend._pending_steps.values()
            if s.workflow_run_id == "run-1" and s.status == StepStatus.PENDING
        ]
        assert len(pending) == 1

    @pytest.mark.asyncio
    async def test_mixed_stale_and_active_workers(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(WorkerInfo(worker_id="active-w", status=WorkerStatus.ACTIVE))
        await backend.worker_heartbeat("active-w")
        await backend.register_worker(WorkerInfo(worker_id="stale-w", status=WorkerStatus.ACTIVE))
        backend._workers["stale-w"].last_heartbeat = datetime.now(tz=timezone.utc) - timedelta(
            minutes=10
        )

        await detect_and_reclaim_stale_tasks(backend, stale_threshold=timedelta(minutes=5))
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "active-w"

    @pytest.mark.asyncio
    async def test_no_heartbeat_treated_as_stale(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(WorkerInfo(worker_id="no-hb", status=WorkerStatus.ACTIVE))
        # Don't heartbeat — last_heartbeat stays None
        backend._workers["no-hb"].last_heartbeat = None

        await detect_and_reclaim_stale_tasks(backend, stale_threshold=timedelta(minutes=5))
        workers = await backend.list_workers()
        assert len(workers) == 0


class TestDistributedGapFill:
    """Gap-fill tests for distributed coordination edge cases."""

    @pytest.mark.asyncio
    async def test_empty_cluster_no_error(self) -> None:
        """detect_and_reclaim_stale_tasks with no workers doesn't error."""
        backend = InMemoryBackend()
        await backend.initialize()
        reclaimed = await detect_and_reclaim_stale_tasks(
            backend, stale_threshold=timedelta(minutes=5)
        )
        assert reclaimed == 0

    @pytest.mark.asyncio
    async def test_multiple_stale_workers(self) -> None:
        """All stale workers detected and cleaned up."""
        backend = InMemoryBackend()
        await backend.initialize()
        for i in range(5):
            await backend.register_worker(
                WorkerInfo(worker_id=f"stale-{i}", status=WorkerStatus.ACTIVE)
            )
            backend._workers[f"stale-{i}"].last_heartbeat = datetime.now(
                tz=timezone.utc
            ) - timedelta(minutes=20)
        await detect_and_reclaim_stale_tasks(backend, stale_threshold=timedelta(minutes=5))
        workers = await backend.list_workers()
        assert len(workers) == 0

    @pytest.mark.asyncio
    async def test_stopped_workers_ignored(self) -> None:
        """Workers with STOPPED status are not considered stale."""
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.register_worker(
            WorkerInfo(worker_id="stopped-w", status=WorkerStatus.STOPPED)
        )
        backend._workers["stopped-w"].last_heartbeat = datetime.now(tz=timezone.utc) - timedelta(
            minutes=20
        )
        reclaimed = await detect_and_reclaim_stale_tasks(
            backend, stale_threshold=timedelta(minutes=5)
        )
        assert reclaimed == 0
