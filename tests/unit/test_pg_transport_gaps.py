"""Tests for signals.transport — PostgreSQLSignalTransport wait/notify with mocked pool."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.core.errors import SignalTimeoutError
from gravtory.core.types import Signal
from gravtory.signals.transport import PostgreSQLSignalTransport, _channel_name


class TestPostgreSQLTransportWait:
    @pytest.mark.asyncio
    async def test_wait_signal_already_exists(self) -> None:
        """Double-check path: signal arrived between handler check and LISTEN."""
        backend = MagicMock()
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()
        backend._pool = mock_pool

        sig = Signal(
            workflow_run_id="run-1",
            signal_name="approval",
            signal_data=json.dumps({"ok": True}).encode(),
        )
        backend.consume_signal = AsyncMock(return_value=sig)

        transport = PostgreSQLSignalTransport(backend)
        result = await transport.wait("run-1", "approval", timedelta(seconds=5))
        assert result == {"ok": True}

        mock_conn.add_listener.assert_awaited_once()
        mock_conn.remove_listener.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wait_signal_via_notify(self) -> None:
        """Signal arrives via LISTEN/NOTIFY after registration."""
        backend = MagicMock()
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()
        backend._pool = mock_pool

        # First call returns None (not yet), second returns the signal
        sig = Signal(
            workflow_run_id="run-1",
            signal_name="go",
            signal_data=json.dumps({"val": 42}).encode(),
        )
        backend.consume_signal = AsyncMock(side_effect=[None, sig])

        transport = PostgreSQLSignalTransport(backend)

        async def fire_notify() -> None:
            await asyncio.sleep(0.05)
            channel = _channel_name("run-1", "go")
            if channel in transport._listeners:
                futures = transport._listeners[channel]
                for future in futures:
                    if not future.done():
                        future.set_result(True)

        task = asyncio.create_task(fire_notify())
        result = await transport.wait("run-1", "go", timedelta(seconds=5))
        assert result == {"val": 42}
        await task

    @pytest.mark.asyncio
    async def test_wait_timeout(self) -> None:
        """Timeout when no signal arrives."""
        backend = MagicMock()
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()
        backend._pool = mock_pool
        backend.consume_signal = AsyncMock(return_value=None)

        transport = PostgreSQLSignalTransport(backend)
        with pytest.raises(SignalTimeoutError):
            await transport.wait("run-1", "missing", timedelta(milliseconds=50))

    @pytest.mark.asyncio
    async def test_wait_pool_not_initialized(self) -> None:
        backend = MagicMock()
        backend._pool = None
        transport = PostgreSQLSignalTransport(backend)
        with pytest.raises(RuntimeError, match="not initialized"):
            await transport.wait("run-1", "sig", timedelta(seconds=1))

    @pytest.mark.asyncio
    async def test_wait_notify_but_data_not_found(self) -> None:
        """Notification fired but DB has no data — should raise SignalTimeoutError."""
        backend = MagicMock()
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()
        backend._pool = mock_pool

        # Both consume_signal calls return None
        backend.consume_signal = AsyncMock(return_value=None)

        transport = PostgreSQLSignalTransport(backend)

        async def fire_notify() -> None:
            await asyncio.sleep(0.05)
            channel = _channel_name("run-1", "ghost")
            if channel in transport._listeners:
                futures = transport._listeners[channel]
                for future in futures:
                    if not future.done():
                        future.set_result(True)

        task = asyncio.create_task(fire_notify())
        with pytest.raises(SignalTimeoutError):
            await transport.wait("run-1", "ghost", timedelta(seconds=1))
        await task


class TestPostgreSQLTransportNotify:
    @pytest.mark.asyncio
    async def test_notify_sends_pg_notify(self) -> None:
        backend = MagicMock()
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        # pool.acquire() returns an async context manager
        acm = AsyncMock()
        acm.__aenter__ = AsyncMock(return_value=mock_conn)
        acm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire.return_value = acm
        backend._pool = mock_pool

        transport = PostgreSQLSignalTransport(backend)
        await transport.notify("run-1", "approval")

        mock_conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_notify_pool_not_initialized(self) -> None:
        backend = MagicMock()
        backend._pool = None
        transport = PostgreSQLSignalTransport(backend)
        # Should not raise, just return
        await transport.notify("run-1", "sig")


class TestOnNotify:
    def test_on_notify_resolves_future(self) -> None:
        backend = MagicMock()
        backend._pool = None
        transport = PostgreSQLSignalTransport(backend)

        loop = asyncio.new_event_loop()
        future: asyncio.Future[bool] = loop.create_future()
        channel = "test_channel"
        transport._listeners[channel] = [future]

        # Simulate notification callback
        conn = MagicMock()
        transport._on_notify(conn, 0, channel, "")
        assert future.done()
        assert future.result() is True
        loop.close()
