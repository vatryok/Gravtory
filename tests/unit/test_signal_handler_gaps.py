"""Tests for signals.handler — SignalHandler send, wait, close, _detect_transport."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import Signal
from gravtory.signals.handler import SignalHandler


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


class TestSignalHandlerSend:
    @pytest.mark.asyncio
    async def test_send_stores_and_notifies(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()
        handler = SignalHandler(backend, transport=transport)
        await handler.send("run-1", "approval", {"approved": True})
        transport.notify.assert_awaited_once_with("run-1", "approval")

    @pytest.mark.asyncio
    async def test_send_resolves_local_waiter(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()
        handler = SignalHandler(backend, transport=transport)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict] = loop.create_future()
        handler._waiters[("run-1", "approval")] = future

        await handler.send("run-1", "approval", {"ok": True})
        assert future.done()
        assert future.result() == {"ok": True}

    @pytest.mark.asyncio
    async def test_send_with_none_data(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()
        handler = SignalHandler(backend, transport=transport)
        await handler.send("run-1", "ping")
        transport.notify.assert_awaited_once()


class TestSignalHandlerWait:
    @pytest.mark.asyncio
    async def test_wait_existing_signal(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()
        handler = SignalHandler(backend, transport=transport)

        # Pre-store a signal
        import json

        sig = Signal(
            workflow_run_id="run-1",
            signal_name="approval",
            signal_data=json.dumps({"approved": True}).encode(),
        )
        await backend.send_signal(sig)

        result = await handler.wait("run-1", "approval", timedelta(seconds=5))
        assert result == {"approved": True}

    @pytest.mark.asyncio
    async def test_wait_via_local_future(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()

        # Make transport.wait block forever so the local future wins the race
        async def block_forever(*args: object, **kwargs: object) -> dict:
            await asyncio.sleep(9999)
            return {}

        transport.wait = block_forever
        transport.close = AsyncMock()
        handler = SignalHandler(backend, transport=transport)

        async def send_later() -> None:
            await asyncio.sleep(0.05)
            await handler.send("run-2", "go", {"val": 42})

        task = asyncio.create_task(send_later())
        result = await handler.wait("run-2", "go", timedelta(seconds=5))
        assert result == {"val": 42}
        await task


class TestSignalHandlerClose:
    @pytest.mark.asyncio
    async def test_close_cancels_waiters(self, backend: InMemoryBackend) -> None:
        transport = AsyncMock()
        handler = SignalHandler(backend, transport=transport)

        loop = asyncio.get_running_loop()
        f1: asyncio.Future[dict] = loop.create_future()
        f2: asyncio.Future[dict] = loop.create_future()
        handler._waiters[("run-1", "sig1")] = f1
        handler._waiters[("run-2", "sig2")] = f2

        await handler.close()
        assert f1.cancelled()
        assert f2.cancelled()
        assert len(handler._waiters) == 0
        transport.close.assert_awaited_once()


class TestDetectTransport:
    def test_detect_polling_for_memory(self) -> None:
        from gravtory.signals.transport import PollingSignalTransport

        backend = InMemoryBackend()
        transport = SignalHandler._detect_transport(backend)
        assert isinstance(transport, PollingSignalTransport)
