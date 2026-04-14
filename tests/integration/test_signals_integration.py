"""Integration tests — signal handling through the backend.

Tests that signals can be sent, consumed, and that the polling transport
works correctly with a real SQLite backend.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from gravtory.core.errors import SignalTimeoutError
from gravtory.core.types import Signal
from gravtory.signals.transport import PollingSignalTransport

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


class TestSignalRoundTrip:
    @pytest.mark.asyncio
    async def test_send_and_consume(self, backend: Backend) -> None:
        """Signal sent via backend can be consumed."""
        sig = Signal(
            workflow_run_id="sig-run-1",
            signal_name="approval",
            signal_data=json.dumps({"approved": True}),
        )
        await backend.send_signal(sig)

        consumed = await backend.consume_signal("sig-run-1", "approval")
        assert consumed is not None
        assert consumed.signal_name == "approval"
        assert consumed.consumed is True

    @pytest.mark.asyncio
    async def test_consume_only_once(self, backend: Backend) -> None:
        """Signal can only be consumed once."""
        sig = Signal(
            workflow_run_id="sig-run-2",
            signal_name="payment",
            signal_data=json.dumps({"amount": 100}),
        )
        await backend.send_signal(sig)

        first = await backend.consume_signal("sig-run-2", "payment")
        assert first is not None
        second = await backend.consume_signal("sig-run-2", "payment")
        assert second is None

    @pytest.mark.asyncio
    async def test_consume_wrong_name_returns_none(self, backend: Backend) -> None:
        """Consuming a non-existent signal name returns None."""
        sig = Signal(workflow_run_id="sig-run-3", signal_name="exists")
        await backend.send_signal(sig)

        result = await backend.consume_signal("sig-run-3", "does_not_exist")
        assert result is None


class TestPollingTransportIntegration:
    @pytest.mark.asyncio
    async def test_wait_receives_signal(self, backend: Backend) -> None:
        """PollingSignalTransport.wait() picks up a pre-sent signal."""
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        sig = Signal(
            workflow_run_id="poll-run-1",
            signal_name="ready",
            signal_data=json.dumps({"status": "go"}),
        )
        await backend.send_signal(sig)

        result = await transport.wait("poll-run-1", "ready", timedelta(seconds=2))
        assert result["status"] == "go"

    @pytest.mark.asyncio
    async def test_wait_timeout_raises(self, backend: Backend) -> None:
        """PollingSignalTransport.wait() raises on timeout."""
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        with pytest.raises(SignalTimeoutError):
            await transport.wait("poll-run-2", "never", timedelta(seconds=0.15))

    @pytest.mark.asyncio
    async def test_multiple_signals_different_names(self, backend: Backend) -> None:
        """Multiple signals with different names are consumed independently."""
        for name in ["sig_a", "sig_b", "sig_c"]:
            await backend.send_signal(
                Signal(
                    workflow_run_id="poll-run-3",
                    signal_name=name,
                    signal_data=json.dumps({"name": name}),
                )
            )

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        for name in ["sig_a", "sig_b", "sig_c"]:
            result = await transport.wait("poll-run-3", name, timedelta(seconds=2))
            assert result["name"] == name
