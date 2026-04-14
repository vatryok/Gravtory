"""Tests for signal transports — polling and transport base."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.errors import SignalTimeoutError
from gravtory.core.types import Signal
from gravtory.signals.transport import PollingSignalTransport


async def _make_backend() -> InMemoryBackend:
    backend = InMemoryBackend()
    await backend.initialize()
    return backend


class TestPollingTransport:
    @pytest.mark.asyncio
    async def test_detects_signal(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        # Pre-store a signal
        sig = Signal(
            workflow_run_id="run-1",
            signal_name="ready",
            signal_data=json.dumps({"ok": True}).encode(),
        )
        await backend.send_signal(sig)

        result = await transport.wait("run-1", "ready", timedelta(seconds=1))
        assert result["ok"] is True

    @pytest.mark.asyncio
    async def test_timeout_raises(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        with pytest.raises(SignalTimeoutError):
            await transport.wait("run-2", "missing", timedelta(seconds=0.15))

    @pytest.mark.asyncio
    async def test_signal_consumed_after_poll(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        sig = Signal(
            workflow_run_id="run-3",
            signal_name="once",
            signal_data=json.dumps({"val": 1}).encode(),
        )
        await backend.send_signal(sig)

        await transport.wait("run-3", "once", timedelta(seconds=1))

        # Second wait should timeout — signal was consumed
        with pytest.raises(SignalTimeoutError):
            await transport.wait("run-3", "once", timedelta(seconds=0.15))

    @pytest.mark.asyncio
    async def test_delayed_signal_detected(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        async def delayed_send() -> None:
            await asyncio.sleep(0.1)
            sig = Signal(
                workflow_run_id="run-4",
                signal_name="delayed",
                signal_data=json.dumps({"found": True}).encode(),
            )
            await backend.send_signal(sig)

        bg_task = asyncio.create_task(delayed_send())
        result = await transport.wait("run-4", "delayed", timedelta(seconds=2))
        assert result["found"] is True
        assert bg_task.done()

    @pytest.mark.asyncio
    async def test_notify_is_noop(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        # notify should not raise
        await transport.notify("run-5", "test")

    @pytest.mark.asyncio
    async def test_none_signal_data_returns_empty_dict(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        sig = Signal(
            workflow_run_id="run-6",
            signal_name="empty",
            signal_data=None,
        )
        await backend.send_signal(sig)

        result = await transport.wait("run-6", "empty", timedelta(seconds=1))
        assert result == {}


class TestSignalTransportGapFill:
    """Gap-fill tests for signal transport edge cases."""

    @pytest.mark.asyncio
    async def test_wait_with_json_data(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)
        sig = Signal(
            workflow_run_id="run-json",
            signal_name="data-sig",
            signal_data=json.dumps({"key": "value", "num": 42}),
        )
        await backend.send_signal(sig)
        result = await transport.wait("run-json", "data-sig", timedelta(seconds=1))
        assert result["key"] == "value"
        assert result["num"] == 42

    @pytest.mark.asyncio
    async def test_wait_timeout_raises(self) -> None:
        backend = await _make_backend()
        transport = PollingSignalTransport(backend, poll_interval=0.05)
        with pytest.raises(SignalTimeoutError):
            await transport.wait("run-never", "missing", timedelta(seconds=0.15))


class TestChannelName:
    def test_basic(self) -> None:
        from gravtory.signals.transport import _channel_name

        result = _channel_name("run-123", "approval")
        assert result == "gravtory_sig_run_123_approval"

    def test_special_characters_sanitized(self) -> None:
        from gravtory.signals.transport import _channel_name

        result = _channel_name("run/with.dots", "sig!name@2")
        assert "." not in result
        assert "/" not in result
        assert "!" not in result
        assert "@" not in result
        assert result.startswith("gravtory_sig_")


class TestPostgreSQLSignalTransport:
    @pytest.mark.asyncio
    async def test_notify_no_pool(self) -> None:
        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_backend = MagicMock()
        mock_backend._pool = None
        transport = PostgreSQLSignalTransport(mock_backend)
        # Should not raise when pool is None
        await transport.notify("run-1", "sig-1")

    @pytest.mark.asyncio
    async def test_wait_no_pool_raises(self) -> None:
        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_backend = MagicMock()
        mock_backend._pool = None
        transport = PostgreSQLSignalTransport(mock_backend)
        with pytest.raises(RuntimeError, match="connection pool not initialized"):
            await transport.wait("run-1", "sig-1", timedelta(seconds=1))

    @pytest.mark.asyncio
    async def test_on_notify_resolves_future(self) -> None:
        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_backend = MagicMock()
        mock_backend._pool = None
        transport = PostgreSQLSignalTransport(mock_backend)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()
        channel = "gravtory_sig_run1_ready"
        transport._listeners[channel] = future

        transport._on_notify(None, 0, channel, "")
        assert future.done()
        assert future.result() is True

    @pytest.mark.asyncio
    async def test_on_notify_unknown_channel_noop(self) -> None:
        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_backend = MagicMock()
        mock_backend._pool = None
        transport = PostgreSQLSignalTransport(mock_backend)

        # Should not raise
        transport._on_notify(None, 0, "unknown_channel", "")

    @pytest.mark.asyncio
    async def test_on_notify_already_done_noop(self) -> None:
        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_backend = MagicMock()
        mock_backend._pool = None
        transport = PostgreSQLSignalTransport(mock_backend)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()
        future.set_result(True)  # already resolved
        channel = "gravtory_sig_run2_done"
        transport._listeners[channel] = future

        # Should not raise
        transport._on_notify(None, 0, channel, "")

    @pytest.mark.asyncio
    async def test_notify_with_pool(self) -> None:

        from gravtory.signals.transport import PostgreSQLSignalTransport

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock()
        # Make it an async context manager
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_backend = MagicMock()
        mock_backend._pool = mock_pool
        transport = PostgreSQLSignalTransport(mock_backend)

        await transport.notify("run-1", "approval")
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_is_noop(self) -> None:
        from gravtory.signals.transport import PollingSignalTransport

        backend = await _make_backend()
        transport = PollingSignalTransport(backend)
        await transport.close()  # should not raise
