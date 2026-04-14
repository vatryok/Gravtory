"""Tests for SignalHandler — send, wait, race conditions, timeouts."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.errors import SignalTimeoutError
from gravtory.core.types import Signal
from gravtory.signals.handler import SignalHandler
from gravtory.signals.transport import PollingSignalTransport


async def _make_handler(poll_interval: float = 0.05) -> tuple[InMemoryBackend, SignalHandler]:
    backend = InMemoryBackend()
    await backend.initialize()
    transport = PollingSignalTransport(backend, poll_interval=poll_interval)
    handler = SignalHandler(backend, transport=transport)
    return backend, handler


class TestSignalSend:
    @pytest.mark.asyncio
    async def test_send_stores_in_db(self) -> None:
        backend, handler = await _make_handler()
        await handler.send("run-1", "approval", {"approved": True})

        # Signal should be in DB
        sig = await backend.consume_signal("run-1", "approval")
        assert sig is not None
        assert sig.signal_name == "approval"
        assert sig.consumed is True  # consume_signal marks consumed

    @pytest.mark.asyncio
    async def test_send_with_none_data(self) -> None:
        backend, handler = await _make_handler()
        await handler.send("run-2", "ping", None)

        sig = await backend.consume_signal("run-2", "ping")
        assert sig is not None

    @pytest.mark.asyncio
    async def test_send_resolves_local_waiter(self) -> None:
        _backend, handler = await _make_handler()

        async def delayed_send() -> None:
            await asyncio.sleep(0.05)
            await handler.send("run-3", "go", {"key": "value"})

        bg_task = asyncio.create_task(delayed_send())
        result = await handler.wait("run-3", "go", timedelta(seconds=2))
        assert result == {"key": "value"}
        assert bg_task.done()


class TestSignalWait:
    @pytest.mark.asyncio
    async def test_wait_returns_existing_signal(self) -> None:
        _backend, handler = await _make_handler()

        # Send signal BEFORE waiting
        await handler.send("run-4", "ready", {"status": "ok"})

        # Wait should return immediately
        result = await handler.wait("run-4", "ready", timedelta(seconds=1))
        assert result == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_wait_timeout_raises(self) -> None:
        _backend, handler = await _make_handler()

        with pytest.raises(SignalTimeoutError):
            await handler.wait("run-5", "never", timedelta(seconds=0.1))

    @pytest.mark.asyncio
    async def test_race_condition_signal_before_wait(self) -> None:
        """Signal sent between register_wait and the actual wait."""
        _backend, handler = await _make_handler()

        # Pre-send the signal
        await handler.send("run-6", "early_signal", {"early": True})

        # Wait should find it immediately via the DB check
        result = await handler.wait("run-6", "early_signal", timedelta(seconds=1))
        assert result["early"] is True

    @pytest.mark.asyncio
    async def test_signal_consumed_after_read(self) -> None:
        _backend, handler = await _make_handler()
        await handler.send("run-7", "once", {"val": 42})

        # First wait consumes the signal
        await handler.wait("run-7", "once", timedelta(seconds=1))

        # Second wait should timeout (signal already consumed)
        with pytest.raises(SignalTimeoutError):
            await handler.wait("run-7", "once", timedelta(seconds=0.1))


class TestSignalWaitCrossProcess:
    """Tests that wait() detects signals via the transport (cross-process path)."""

    @pytest.mark.asyncio
    async def test_wait_detects_signal_via_transport_poll(self) -> None:
        """Signal stored directly in DB (simulating another process) is found by transport."""
        backend, handler = await _make_handler(poll_interval=0.05)

        async def delayed_db_insert() -> None:
            await asyncio.sleep(0.1)
            sig = Signal(
                workflow_run_id="run-8",
                signal_name="cross",
                signal_data=json.dumps({"cross": True}).encode(),
            )
            await backend.send_signal(sig)

        bg_task = asyncio.create_task(delayed_db_insert())
        result = await handler.wait("run-8", "cross", timedelta(seconds=2))
        assert result["cross"] is True
        assert bg_task.done()

    @pytest.mark.asyncio
    async def test_wait_prefers_local_future_when_faster(self) -> None:
        """In-process send() resolves wait() before transport polls."""
        _backend, handler = await _make_handler(poll_interval=5.0)

        async def fast_send() -> None:
            await asyncio.sleep(0.05)
            await handler.send("run-9", "fast", {"fast": True})

        bg_task = asyncio.create_task(fast_send())
        result = await handler.wait("run-9", "fast", timedelta(seconds=2))
        assert result["fast"] is True
        assert bg_task.done()

    @pytest.mark.asyncio
    async def test_wait_timeout_with_transport(self) -> None:
        _backend, handler = await _make_handler()

        with pytest.raises(SignalTimeoutError):
            await handler.wait("run-10", "missing", timedelta(seconds=0.15))


class TestSignalHandlerCleanup:
    @pytest.mark.asyncio
    async def test_close_cancels_waiters(self) -> None:
        _backend, handler = await _make_handler()

        # Start a wait in the background
        task = asyncio.create_task(handler.wait("run-11", "never", timedelta(seconds=10)))
        await asyncio.sleep(0.05)  # Let the wait register

        await handler.close()
        assert len(handler._waiters) == 0

        # Task should eventually raise (cancelled or timeout)
        with pytest.raises((asyncio.CancelledError, SignalTimeoutError)):
            await task


class TestSignalHandlerGapFill:
    """Gap-fill tests for signal handler edge cases."""

    @pytest.mark.asyncio
    async def test_send_and_receive_multiple_signals(self) -> None:
        _backend, handler = await _make_handler()
        for i in range(5):
            await handler.send("run-multi", f"sig-{i}", {"n": i})
        for i in range(5):
            data = await handler.wait("run-multi", f"sig-{i}", timedelta(seconds=1))
            assert data["n"] == i

    @pytest.mark.asyncio
    async def test_send_complex_json_data(self) -> None:
        _backend, handler = await _make_handler()
        payload = {"nested": {"key": [1, 2, 3]}, "flag": True}
        await handler.send("run-json", "complex", payload)
        result = await handler.wait("run-json", "complex", timedelta(seconds=1))
        assert result["nested"]["key"] == [1, 2, 3]
        assert result["flag"] is True
