"""Load / stress tests for concurrent workflow execution.

These tests verify correctness under high concurrency, not just throughput.
Run with: pytest tests/load/ -v --timeout=120

To skip in normal CI, these are marked with ``pytest.mark.slow``.
"""

from __future__ import annotations

import asyncio

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import WorkflowRun, WorkflowStatus

pytestmark = [pytest.mark.slow, pytest.mark.asyncio]


class TestConcurrentWorkflowCreation:
    """Verify backend handles many concurrent creates without data loss."""

    async def test_100_concurrent_creates(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        async def create(i: int) -> None:
            run = WorkflowRun(
                id=f"stress-{i}",
                workflow_name="StressWF",
                status=WorkflowStatus.PENDING,
            )
            await backend.create_workflow_run(run)

        await asyncio.gather(*(create(i) for i in range(100)))

        count = await backend.count_workflow_runs()
        assert count == 100

    async def test_500_concurrent_creates(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        async def create(i: int) -> None:
            run = WorkflowRun(
                id=f"stress-{i}",
                workflow_name="StressWF",
                status=WorkflowStatus.PENDING,
            )
            await backend.create_workflow_run(run)

        await asyncio.gather(*(create(i) for i in range(500)))

        count = await backend.count_workflow_runs()
        assert count == 500


class TestConcurrentStatusUpdates:
    """Verify status transitions under contention."""

    async def test_concurrent_status_transitions(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        # Create runs
        for i in range(50):
            run = WorkflowRun(
                id=f"tx-{i}",
                workflow_name="TxWF",
                status=WorkflowStatus.PENDING,
            )
            await backend.create_workflow_run(run)

        # Transition all to RUNNING concurrently
        async def to_running(i: int) -> None:
            await backend.validated_update_workflow_status(f"tx-{i}", WorkflowStatus.RUNNING)

        await asyncio.gather(*(to_running(i) for i in range(50)))

        runs = await backend.list_workflow_runs(status=WorkflowStatus.RUNNING)
        assert len(runs) == 50

    async def test_interleaved_complete_and_fail(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        for i in range(100):
            run = WorkflowRun(
                id=f"mix-{i}",
                workflow_name="MixWF",
                status=WorkflowStatus.RUNNING,
            )
            await backend.create_workflow_run(run)

        async def complete(i: int) -> None:
            await backend.validated_update_workflow_status(f"mix-{i}", WorkflowStatus.COMPLETED)

        async def fail(i: int) -> None:
            await backend.validated_update_workflow_status(f"mix-{i}", WorkflowStatus.FAILED)

        tasks = []
        for i in range(100):
            if i % 2 == 0:
                tasks.append(complete(i))
            else:
                tasks.append(fail(i))
        await asyncio.gather(*tasks)

        completed = await backend.count_workflow_runs(status=WorkflowStatus.COMPLETED)
        failed = await backend.count_workflow_runs(status=WorkflowStatus.FAILED)
        assert completed == 50
        assert failed == 50


class TestConcurrentReadsAndWrites:
    """Ensure reads return consistent data during concurrent writes."""

    async def test_reads_during_writes(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()

        created = 0
        read_errors = 0

        async def writer() -> None:
            nonlocal created
            for i in range(200):
                run = WorkflowRun(
                    id=f"rw-{i}",
                    workflow_name="RWWF",
                    status=WorkflowStatus.PENDING,
                )
                await backend.create_workflow_run(run)
                created += 1
                await asyncio.sleep(0)  # yield

        async def reader() -> None:
            nonlocal read_errors
            for _ in range(100):
                try:
                    runs = await backend.list_workflow_runs(limit=1000)
                    # Should never raise, and length should be consistent
                    assert isinstance(runs, (list, tuple))
                except Exception:
                    read_errors += 1
                await asyncio.sleep(0)

        await asyncio.gather(writer(), reader(), reader())
        assert created == 200
        assert read_errors == 0


class TestHighDLQVolume:
    """Verify DLQ operations at scale."""

    async def test_large_dlq_purge(self) -> None:
        from gravtory.core.types import DLQEntry

        backend = InMemoryBackend()
        await backend.initialize()

        for i in range(500):
            entry = DLQEntry(
                id=f"dlq-{i}",
                workflow_run_id=f"run-{i}",
                step_order=1,
                error_message=f"error-{i}",
            )
            await backend.add_to_dlq(entry)

        deleted = await backend.purge_dlq()
        assert deleted == 500

        entries = await backend.list_dlq()
        assert len(entries) == 0
