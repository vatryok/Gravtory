"""Tests for concurrency control — check_concurrency_limit on backends."""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import WorkflowRun, WorkflowStatus


class TestConcurrencyControl:
    @pytest.mark.asyncio
    async def test_under_limit_returns_true(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run = WorkflowRun(
            id="r1", workflow_name="wf", namespace="default", status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run)
        assert await backend.check_concurrency_limit("wf", "default", 5) is True

    @pytest.mark.asyncio
    async def test_at_limit_returns_false(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        for i in range(3):
            run = WorkflowRun(
                id=f"r{i}", workflow_name="wf", namespace="default", status=WorkflowStatus.RUNNING
            )
            await backend.create_workflow_run(run)
        assert await backend.check_concurrency_limit("wf", "default", 3) is False

    @pytest.mark.asyncio
    async def test_completed_runs_not_counted(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        for i in range(3):
            run = WorkflowRun(
                id=f"r{i}", workflow_name="wf", namespace="default", status=WorkflowStatus.COMPLETED
            )
            await backend.create_workflow_run(run)
        assert await backend.check_concurrency_limit("wf", "default", 3) is True

    @pytest.mark.asyncio
    async def test_different_namespaces_independent(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run1 = WorkflowRun(
            id="r1", workflow_name="wf", namespace="ns-a", status=WorkflowStatus.RUNNING
        )
        run2 = WorkflowRun(
            id="r2", workflow_name="wf", namespace="ns-b", status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run1)
        await backend.create_workflow_run(run2)
        assert await backend.check_concurrency_limit("wf", "ns-a", 1) is False
        assert await backend.check_concurrency_limit("wf", "ns-b", 1) is False
        assert await backend.check_concurrency_limit("wf", "ns-a", 2) is True

    @pytest.mark.asyncio
    async def test_pending_counted_as_active(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run = WorkflowRun(
            id="r1", workflow_name="wf", namespace="default", status=WorkflowStatus.PENDING
        )
        await backend.create_workflow_run(run)
        assert await backend.check_concurrency_limit("wf", "default", 1) is False

    @pytest.mark.asyncio
    async def test_different_workflow_names_independent(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run1 = WorkflowRun(
            id="r1", workflow_name="wf-a", namespace="default", status=WorkflowStatus.RUNNING
        )
        run2 = WorkflowRun(
            id="r2", workflow_name="wf-b", namespace="default", status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run1)
        await backend.create_workflow_run(run2)
        assert await backend.check_concurrency_limit("wf-a", "default", 1) is False
        assert await backend.check_concurrency_limit("wf-b", "default", 2) is True


class TestConcurrencyGapFill:
    """Gap-fill tests for concurrency control edge cases."""

    @pytest.mark.asyncio
    async def test_zero_limit_always_false(self) -> None:
        """A limit of 0 blocks all new runs."""
        backend = InMemoryBackend()
        await backend.initialize()
        assert await backend.check_concurrency_limit("wf", "default", 0) is False

    @pytest.mark.asyncio
    async def test_high_limit_allows_many(self) -> None:
        """A very high limit allows many concurrent runs."""
        backend = InMemoryBackend()
        await backend.initialize()
        for i in range(50):
            await backend.create_workflow_run(
                WorkflowRun(
                    id=f"r{i}",
                    workflow_name="wf",
                    namespace="default",
                    status=WorkflowStatus.RUNNING,
                )
            )
        assert await backend.check_concurrency_limit("wf", "default", 100) is True
        assert await backend.check_concurrency_limit("wf", "default", 50) is False

    @pytest.mark.asyncio
    async def test_failed_runs_not_counted(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf", namespace="default", status=WorkflowStatus.FAILED
            )
        )
        assert await backend.check_concurrency_limit("wf", "default", 1) is True

    @pytest.mark.asyncio
    async def test_cancelled_runs_not_counted(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf", namespace="default", status=WorkflowStatus.CANCELLED
            )
        )
        assert await backend.check_concurrency_limit("wf", "default", 1) is True
