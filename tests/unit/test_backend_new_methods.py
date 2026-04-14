"""Tests for newly added Backend abstract method implementations on InMemoryBackend."""

from __future__ import annotations

from datetime import timedelta

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    DLQEntry,
    WorkerInfo,
    WorkerStatus,
    WorkflowRun,
    WorkflowStatus,
)


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


class TestCountWorkflowRuns:
    @pytest.mark.asyncio
    async def test_count_empty(self, backend: InMemoryBackend) -> None:
        count = await backend.count_workflow_runs()
        assert count == 0

    @pytest.mark.asyncio
    async def test_count_all(self, backend: InMemoryBackend) -> None:
        for i in range(3):
            await backend.create_workflow_run(
                WorkflowRun(
                    id=f"r{i}",
                    workflow_name="wf",
                    workflow_version=1,
                    status=WorkflowStatus.RUNNING,
                )
            )
        assert await backend.count_workflow_runs() == 3

    @pytest.mark.asyncio
    async def test_count_by_status(self, backend: InMemoryBackend) -> None:
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf", workflow_version=1, status=WorkflowStatus.RUNNING
            )
        )
        await backend.create_workflow_run(
            WorkflowRun(
                id="r2", workflow_name="wf", workflow_version=1, status=WorkflowStatus.COMPLETED
            )
        )
        await backend.update_workflow_status("r2", WorkflowStatus.COMPLETED)
        assert await backend.count_workflow_runs(status=WorkflowStatus.RUNNING) == 1

    @pytest.mark.asyncio
    async def test_count_by_workflow_name(self, backend: InMemoryBackend) -> None:
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf-a", workflow_version=1, status=WorkflowStatus.RUNNING
            )
        )
        await backend.create_workflow_run(
            WorkflowRun(
                id="r2", workflow_name="wf-b", workflow_version=1, status=WorkflowStatus.RUNNING
            )
        )
        assert await backend.count_workflow_runs(workflow_name="wf-a") == 1


class TestGetIncompleteRuns:
    @pytest.mark.asyncio
    async def test_empty(self, backend: InMemoryBackend) -> None:
        runs = await backend.get_incomplete_runs()
        assert runs == []

    @pytest.mark.asyncio
    async def test_returns_running_and_pending(self, backend: InMemoryBackend) -> None:
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf", workflow_version=1, status=WorkflowStatus.RUNNING
            )
        )
        await backend.create_workflow_run(
            WorkflowRun(
                id="r2", workflow_name="wf", workflow_version=1, status=WorkflowStatus.PENDING
            )
        )
        await backend.create_workflow_run(
            WorkflowRun(
                id="r3", workflow_name="wf", workflow_version=1, status=WorkflowStatus.COMPLETED
            )
        )
        await backend.update_workflow_status("r3", WorkflowStatus.COMPLETED)
        runs = await backend.get_incomplete_runs()
        ids = {r.id for r in runs}
        assert "r1" in ids
        assert "r2" in ids
        assert "r3" not in ids


class TestCheckpointParallelItem:
    @pytest.mark.asyncio
    async def test_checkpoint_and_retrieve(self, backend: InMemoryBackend) -> None:
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"result-0")
        await backend.checkpoint_parallel_item("run-1", 1, 1, b"result-1")
        results = await backend.get_parallel_results("run-1", 1)
        assert results == {0: b"result-0", 1: b"result-1"}

    @pytest.mark.asyncio
    async def test_overwrite(self, backend: InMemoryBackend) -> None:
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"old")
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"new")
        results = await backend.get_parallel_results("run-1", 1)
        assert results[0] == b"new"

    @pytest.mark.asyncio
    async def test_empty_results(self, backend: InMemoryBackend) -> None:
        results = await backend.get_parallel_results("nonexistent", 1)
        assert results == {}

    @pytest.mark.asyncio
    async def test_different_steps_isolated(self, backend: InMemoryBackend) -> None:
        await backend.checkpoint_parallel_item("run-1", 1, 0, b"step1")
        await backend.checkpoint_parallel_item("run-1", 2, 0, b"step2")
        assert await backend.get_parallel_results("run-1", 1) == {0: b"step1"}
        assert await backend.get_parallel_results("run-1", 2) == {0: b"step2"}


class TestPurgeDLQ:
    @pytest.mark.asyncio
    async def test_purge_empty(self, backend: InMemoryBackend) -> None:
        count = await backend.purge_dlq()
        assert count == 0

    @pytest.mark.asyncio
    async def test_purge_with_entries(self, backend: InMemoryBackend) -> None:
        for i in range(5):
            await backend.add_to_dlq(
                DLQEntry(
                    workflow_run_id=f"run-{i}",
                    step_order=1,
                    error_message="fail",
                )
            )
        count = await backend.purge_dlq()
        assert count == 5
        remaining = await backend.list_dlq()
        assert len(remaining) == 0


class TestWorkerHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_with_current_task(self, backend: InMemoryBackend) -> None:
        await backend.register_worker(
            WorkerInfo(
                worker_id="w1",
                node_id="n1",
                status=WorkerStatus.ACTIVE,
            )
        )
        await backend.worker_heartbeat("w1", current_task="task-42")
        workers = await backend.list_workers()
        assert workers[0].current_task == "task-42"

    @pytest.mark.asyncio
    async def test_heartbeat_without_current_task(self, backend: InMemoryBackend) -> None:
        await backend.register_worker(
            WorkerInfo(
                worker_id="w1",
                node_id="n1",
                status=WorkerStatus.ACTIVE,
            )
        )
        await backend.worker_heartbeat("w1")
        workers = await backend.list_workers()
        assert workers[0].worker_id == "w1"

    @pytest.mark.asyncio
    async def test_heartbeat_nonexistent_worker(self, backend: InMemoryBackend) -> None:
        await backend.worker_heartbeat("nonexistent")  # should not raise


class TestGetStaleWorkers:
    @pytest.mark.asyncio
    async def test_no_stale(self, backend: InMemoryBackend) -> None:
        await backend.register_worker(
            WorkerInfo(
                worker_id="w1",
                node_id="n1",
                status=WorkerStatus.ACTIVE,
            )
        )
        stale = await backend.get_stale_workers(300)
        assert len(stale) == 0

    @pytest.mark.asyncio
    async def test_stale_worker_detected(self, backend: InMemoryBackend) -> None:
        from datetime import datetime, timezone

        await backend.register_worker(
            WorkerInfo(
                worker_id="w1",
                node_id="n1",
                status=WorkerStatus.ACTIVE,
            )
        )
        # Manually make heartbeat old
        backend._workers["w1"].last_heartbeat = datetime.now(tz=timezone.utc) - timedelta(
            minutes=10
        )
        stale = await backend.get_stale_workers(60)
        assert len(stale) == 1
        assert stale[0].worker_id == "w1"

    @pytest.mark.asyncio
    async def test_null_heartbeat_is_stale(self, backend: InMemoryBackend) -> None:
        await backend.register_worker(
            WorkerInfo(
                worker_id="w1",
                node_id="n1",
                status=WorkerStatus.ACTIVE,
            )
        )
        backend._workers["w1"].last_heartbeat = None
        stale = await backend.get_stale_workers(60)
        assert len(stale) == 1
