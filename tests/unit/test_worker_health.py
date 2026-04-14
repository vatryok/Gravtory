"""Tests for workers.health — HealthMonitor heartbeat and liveness."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.workers.health import HealthMonitor


@pytest.fixture
def backend() -> InMemoryBackend:
    return InMemoryBackend()


@pytest.fixture
def monitor(backend: InMemoryBackend) -> HealthMonitor:
    return HealthMonitor(
        backend=backend,
        worker_id="test-worker-1",
        heartbeat_interval=0.1,
        stale_threshold=timedelta(seconds=30),
    )


class TestHealthMonitorProperties:
    def test_initial_state(self, monitor: HealthMonitor) -> None:
        assert not monitor.is_running
        assert monitor.last_heartbeat is None

    def test_set_current_task(self, monitor: HealthMonitor) -> None:
        monitor.set_current_task("task-123")
        assert monitor._current_task == "task-123"
        monitor.set_current_task(None)
        assert monitor._current_task is None


class TestHealthMonitorRegister:
    @pytest.mark.asyncio
    async def test_register(self, backend: InMemoryBackend, monitor: HealthMonitor) -> None:
        await backend.initialize()
        await monitor.register()
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "test-worker-1"
        assert monitor.last_heartbeat is not None


class TestHealthMonitorHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_updates_timestamp(
        self,
        backend: InMemoryBackend,
        monitor: HealthMonitor,
    ) -> None:
        await backend.initialize()
        await monitor.register()
        first_hb = monitor.last_heartbeat
        await asyncio.sleep(0.01)
        await monitor.heartbeat()
        assert monitor.last_heartbeat is not None
        assert monitor.last_heartbeat >= first_hb  # type: ignore[operator]


class TestHealthMonitorLiveness:
    @pytest.mark.asyncio
    async def test_liveness_true_when_recent(
        self,
        backend: InMemoryBackend,
        monitor: HealthMonitor,
    ) -> None:
        await backend.initialize()
        await monitor.register()
        assert await monitor.check_liveness() is True

    @pytest.mark.asyncio
    async def test_liveness_false_when_no_heartbeat(self, monitor: HealthMonitor) -> None:
        assert await monitor.check_liveness() is False


class TestHealthMonitorLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, backend: InMemoryBackend, monitor: HealthMonitor) -> None:
        await backend.initialize()
        await monitor.start()
        assert monitor.is_running
        await asyncio.sleep(0.15)  # let at least one heartbeat fire
        await monitor.stop()
        assert not monitor.is_running
        # Worker should be deregistered
        workers = await backend.list_workers()
        assert len(workers) == 0

    @pytest.mark.asyncio
    async def test_stop_handles_deregister_error(
        self,
        backend: InMemoryBackend,
        monitor: HealthMonitor,
    ) -> None:
        await backend.initialize()
        await monitor.start()
        await asyncio.sleep(0.05)
        # Make deregister fail
        backend.deregister_worker = AsyncMock(side_effect=RuntimeError("fail"))  # type: ignore[method-assign]
        await monitor.stop()  # should not raise
        assert not monitor.is_running

    @pytest.mark.asyncio
    async def test_heartbeat_loop_recovers_from_error(
        self,
        backend: InMemoryBackend,
        monitor: HealthMonitor,
    ) -> None:
        await backend.initialize()
        original_hb = backend.worker_heartbeat
        call_count = 0

        async def flaky_heartbeat(wid: str, current_task: str | None = None) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient error")
            return await original_hb(wid, current_task)

        backend.worker_heartbeat = flaky_heartbeat  # type: ignore[method-assign]
        await monitor.start()
        await asyncio.sleep(0.3)  # enough for multiple heartbeats
        await monitor.stop()
        assert call_count >= 2  # recovered after first error
