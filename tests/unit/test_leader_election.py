"""Tests for scheduling.leader — LeaderElector distributed lock coordination."""

from __future__ import annotations

import asyncio

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.scheduling.leader import LeaderElector


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


class TestLeaderElectorProperties:
    @pytest.mark.asyncio
    async def test_initial_state(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1")
        assert not le.is_leader

    @pytest.mark.asyncio
    async def test_try_acquire(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1", ttl=5.0)
        acquired = await le.try_acquire()
        assert acquired is True
        assert le.is_leader

    @pytest.mark.asyncio
    async def test_try_acquire_contested(self, backend: InMemoryBackend) -> None:
        le1 = LeaderElector(backend, "test-lock", "node-1", ttl=30.0)
        le2 = LeaderElector(backend, "test-lock", "node-2", ttl=30.0)
        await le1.try_acquire()
        acquired = await le2.try_acquire()
        assert acquired is False
        assert not le2.is_leader

    @pytest.mark.asyncio
    async def test_renew(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1", ttl=5.0)
        await le.try_acquire()
        renewed = await le.renew()
        assert renewed is True
        assert le.is_leader

    @pytest.mark.asyncio
    async def test_renew_without_leadership(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1")
        renewed = await le.renew()
        assert renewed is False

    @pytest.mark.asyncio
    async def test_release(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1", ttl=5.0)
        await le.try_acquire()
        assert le.is_leader
        await le.release()
        assert not le.is_leader

    @pytest.mark.asyncio
    async def test_release_when_not_leader(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(backend, "test-lock", "node-1")
        await le.release()  # should not raise
        assert not le.is_leader


class TestLeaderElectorLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(
            backend,
            "test-lock",
            "node-1",
            ttl=5.0,
            renew_interval=0.1,
        )
        await le.start()
        await asyncio.sleep(0.15)
        assert le.is_leader
        await le.stop()
        assert not le.is_leader

    @pytest.mark.asyncio
    async def test_election_loop_recovers_from_error(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(
            backend,
            "test-lock",
            "node-1",
            ttl=5.0,
            renew_interval=0.1,
        )
        call_count = 0
        original_acquire = backend.acquire_lock

        async def flaky_acquire(name: str, holder: str, ttl: int) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient")
            return await original_acquire(name, holder, ttl)

        backend.acquire_lock = flaky_acquire  # type: ignore[method-assign]
        await le.start()
        await asyncio.sleep(0.3)
        await le.stop()
        assert call_count >= 2  # recovered after error

    @pytest.mark.asyncio
    async def test_renew_failure_loses_leadership(self, backend: InMemoryBackend) -> None:
        le = LeaderElector(
            backend,
            "test-lock",
            "node-1",
            ttl=5.0,
            renew_interval=0.1,
        )
        await le.try_acquire()
        assert le.is_leader

        async def fail_refresh(name: str, holder: str, ttl: int) -> bool:
            return False

        backend.refresh_lock = fail_refresh  # type: ignore[method-assign]
        renewed = await le.renew()
        assert renewed is False
        assert not le.is_leader
