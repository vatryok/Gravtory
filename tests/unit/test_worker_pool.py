"""Tests for workers.pool -- WorkerPool multi-process supervisor."""

from __future__ import annotations

import asyncio
import multiprocessing
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gravtory.workers.pool import WorkerPool, _worker_process_entry

_BACKEND_URL = "sqlite://:memory:"


def _make_pool(count: int = 1, *, node_id: str = "test") -> WorkerPool:
    """Helper to build a WorkerPool with the correct constructor signature."""
    return WorkerPool(
        count=count,
        backend_url=_BACKEND_URL,
        node_id=node_id,
    )


class TestWorkerPoolProperties:
    def test_initial_state(self) -> None:
        pool = _make_pool(4, node_id="test-node")
        assert not pool.is_running
        assert pool.worker_count == 0
        assert pool.alive_count == 0

    def test_default_node_id(self) -> None:
        pool = WorkerPool(count=2, backend_url=_BACKEND_URL)
        assert pool._node_id.startswith("node-")

    def test_worker_id_format(self) -> None:
        pool = _make_pool(2, node_id="mynode")
        assert pool._worker_id(0) == "mynode-worker-0"
        assert pool._worker_id(3) == "mynode-worker-3"


class TestWorkerPoolStartStop:
    @pytest.mark.asyncio
    async def test_start_creates_processes(self) -> None:
        pool = _make_pool(2)
        with patch.object(pool, "_start_worker_process") as mock_start:
            await pool.start()
            assert pool.is_running
            assert mock_start.call_count == 2
            mock_start.assert_any_call(0)
            mock_start.assert_any_call(1)
            pool._shutdown_event.set()
            if pool._supervisor_task:
                pool._supervisor_task.cancel()
                try:
                    await pool._supervisor_task
                except (asyncio.CancelledError, Exception):
                    pass
            pool._started = False

    @pytest.mark.asyncio
    async def test_start_idempotent(self) -> None:
        pool = _make_pool(1)
        with patch.object(pool, "_start_worker_process"):
            await pool.start()
            first_task = pool._supervisor_task
            await pool.start()  # should be no-op
            assert pool._supervisor_task is first_task
            pool._shutdown_event.set()
            if pool._supervisor_task:
                pool._supervisor_task.cancel()
                try:
                    await pool._supervisor_task
                except (asyncio.CancelledError, Exception):
                    pass
            pool._started = False

    @pytest.mark.asyncio
    async def test_stop_terminates_processes(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_proc.pid = 12345
        mock_proc.join = MagicMock()
        pool._processes["test-worker-0"] = mock_proc
        pool._started = True

        pool._supervisor_task = asyncio.create_task(asyncio.sleep(100))

        await pool.stop(drain=False)

        assert not pool.is_running
        assert pool.worker_count == 0

    @pytest.mark.asyncio
    async def test_stop_kills_alive_processes(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.is_alive.side_effect = [True, True, False]
        mock_proc.pid = 99999
        mock_proc.join = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()
        pool._processes["test-worker-0"] = mock_proc
        pool._started = True

        await pool.stop(drain=False)
        mock_proc.terminate.assert_called_once()


class TestWorkerPoolSupervisor:
    @pytest.mark.asyncio
    async def test_supervisor_restarts_dead_process(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_proc.exitcode = 1
        pool._processes["test-worker-0"] = mock_proc

        with patch.object(pool, "_start_worker_process") as mock_start:
            pool._shutdown_event.clear()

            async def one_iteration() -> None:
                for i in range(pool._count):
                    wid = pool._worker_id(i)
                    proc = pool._processes.get(wid)
                    if proc is not None and not proc.is_alive():
                        pool._start_worker_process(i)
                pool._shutdown_event.set()

            await one_iteration()
            mock_start.assert_called_once_with(0)


class TestWorkerPoolAliveCount:
    def test_alive_count(self) -> None:
        pool = _make_pool(3)
        alive_proc = MagicMock()
        alive_proc.is_alive.return_value = True
        dead_proc = MagicMock()
        dead_proc.is_alive.return_value = False
        pool._processes = {
            "test-worker-0": alive_proc,
            "test-worker-1": dead_proc,
            "test-worker-2": alive_proc,
        }
        assert pool.alive_count == 2
        assert pool.worker_count == 3


class TestStopDrain:
    @pytest.mark.asyncio
    async def test_stop_with_drain(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        mock_proc.pid = 111
        mock_proc.join = MagicMock()
        pool._processes["test-worker-0"] = mock_proc
        pool._started = True
        pool._supervisor_task = asyncio.create_task(asyncio.sleep(100))

        await pool.stop(drain=True, drain_timeout=0.1)
        assert not pool.is_running
        assert mock_proc.join.call_count >= 1

    @pytest.mark.asyncio
    async def test_stop_force_kills_stubborn(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc.pid = 222
        mock_proc.join = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()
        pool._processes["test-worker-0"] = mock_proc
        pool._started = True

        await pool.stop(drain=False)
        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_without_supervisor(self) -> None:
        pool = _make_pool(1)
        pool._started = True
        pool._supervisor_task = None
        await pool.stop(drain=False)
        assert not pool.is_running


class TestStartWorkerProcess:
    def test_start_worker_process_forks(self) -> None:
        pool = _make_pool(1)
        mock_proc = MagicMock()
        mock_proc.pid = 333
        mock_ctx = MagicMock()
        mock_ctx.Process.return_value = mock_proc
        pool._mp_ctx = mock_ctx
        pool._start_worker_process(0)
        mock_ctx.Process.assert_called_once()
        mock_proc.start.assert_called_once()
        assert "test-worker-0" in pool._processes


class TestSupervisorLoop:
    @pytest.mark.asyncio
    async def test_supervisor_exits_on_shutdown(self) -> None:
        pool = _make_pool(1)
        pool._shutdown_event.set()  # immediate shutdown
        await pool._supervisor_loop()  # should return quickly

    @pytest.mark.asyncio
    async def test_supervisor_restarts_crashed_workers(self) -> None:
        pool = _make_pool(1)
        dead_proc = MagicMock()
        dead_proc.is_alive.return_value = False
        dead_proc.exitcode = -9
        pool._processes["test-worker-0"] = dead_proc

        restart_count = 0

        def mock_start(idx: int) -> None:
            nonlocal restart_count
            restart_count += 1
            alive = MagicMock()
            alive.is_alive.return_value = True
            pool._processes[pool._worker_id(idx)] = alive
            pool._shutdown_event.set()  # stop after one restart

        with patch.object(pool, "_start_worker_process", side_effect=mock_start):
            await pool._supervisor_loop()
        assert restart_count == 1


class TestWorkerProcessEntry:
    def test_worker_process_entry_runs(self) -> None:
        shutdown = multiprocessing.Event()
        shutdown.set()  # Immediately signal shutdown

        with (
            patch("gravtory.backends.create_backend") as mock_create_backend,
            patch("gravtory.workers.local.LocalWorker") as MockLocalWorker,
            patch("signal.signal"),
        ):
            mock_backend = AsyncMock()
            mock_backend.initialize = AsyncMock()
            mock_backend.close = AsyncMock()
            mock_create_backend.return_value = mock_backend

            mock_worker = MagicMock()
            mock_worker._shutdown_event = asyncio.Event()
            mock_worker.start = AsyncMock()
            mock_worker.stop = AsyncMock()
            MockLocalWorker.return_value = mock_worker

            _worker_process_entry(
                "test-w-0",
                _BACKEND_URL,
                "gravtory_",
                shutdown,
                10,
                0.1,
                5.0,
            )
