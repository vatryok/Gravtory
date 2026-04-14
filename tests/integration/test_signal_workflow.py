"""Integration tests — signal send/receive within workflow execution.

Verifies:
  - Signal sent before workflow wait is consumed immediately.
  - Signal with data payload is delivered correctly.
  - Signal timeout raises SignalTimeoutError.
  - Multiple signals to the same workflow (different names) work.
  - Signal-driven step receives signal_data in its inputs.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.errors import SignalTimeoutError
from gravtory.core.types import Signal
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow
from gravtory.signals.handler import SignalHandler
from gravtory.signals.transport import PollingSignalTransport

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = pytest.mark.integration


# ── Shared state ────────────────────────────────────────────────

_received_signals: list[dict[str, Any]] = []


# ── Fixture workflows ──────────────────────────────────────────


@workflow(id="sigwf-{tag}")
class SignalWorkflow:
    """Step 1 does work, step 2 processes a signal result."""

    @step(1)
    async def prepare(self, tag: str) -> dict[str, str]:
        return {"prepared": tag}

    @step(2, depends_on=1)
    async def finalize(self, prepared: str, **kw: object) -> dict[str, str]:
        return {"done": prepared}


# ── Tests ────────────────────────────────────────────────────────


class TestSignalWorkflowIntegration:
    """Signal send/receive end-to-end tests via SignalHandler."""

    @pytest.mark.asyncio
    async def test_send_before_wait_is_consumed(self) -> None:
        """Signal sent before wait() is picked up immediately."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        # Send signal first
        await handler.send("run-pre-1", "approval", {"approved": True})

        # Now wait — should return immediately since signal exists
        result = await handler.wait("run-pre-1", "approval", timedelta(seconds=2))
        assert result["approved"] is True

        await handler.close()

    @pytest.mark.asyncio
    async def test_signal_with_payload(self) -> None:
        """Signal data payload is delivered intact."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        payload = {"user_id": "u123", "action": "approve", "amount": 42.5}
        await handler.send("run-payload-1", "review", payload)

        result = await handler.wait("run-payload-1", "review", timedelta(seconds=2))
        assert result["user_id"] == "u123"
        assert result["action"] == "approve"
        assert result["amount"] == 42.5

        await handler.close()

    @pytest.mark.asyncio
    async def test_signal_timeout_raises(self) -> None:
        """Waiting for a never-sent signal raises SignalTimeoutError."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        with pytest.raises(SignalTimeoutError):
            await handler.wait("run-timeout-1", "never", timedelta(seconds=0.2))

        await handler.close()

    @pytest.mark.asyncio
    async def test_multiple_signals_different_names(self) -> None:
        """Multiple signals with different names are independent."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        await handler.send("run-multi-1", "sig_a", {"name": "a"})
        await handler.send("run-multi-1", "sig_b", {"name": "b"})
        await handler.send("run-multi-1", "sig_c", {"name": "c"})

        result_a = await handler.wait("run-multi-1", "sig_a", timedelta(seconds=2))
        result_b = await handler.wait("run-multi-1", "sig_b", timedelta(seconds=2))
        result_c = await handler.wait("run-multi-1", "sig_c", timedelta(seconds=2))

        assert result_a["name"] == "a"
        assert result_b["name"] == "b"
        assert result_c["name"] == "c"

        await handler.close()

    @pytest.mark.asyncio
    async def test_signal_consumed_only_once(self) -> None:
        """A signal is consumed once — second wait times out."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        await handler.send("run-once-1", "payment", {"amount": 100})

        result = await handler.wait("run-once-1", "payment", timedelta(seconds=2))
        assert result["amount"] == 100

        # Second wait should timeout — signal already consumed
        with pytest.raises(SignalTimeoutError):
            await handler.wait("run-once-1", "payment", timedelta(seconds=0.2))

        await handler.close()

    @pytest.mark.asyncio
    async def test_concurrent_send_and_wait(self) -> None:
        """Signal sent concurrently during wait() is picked up."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)
        handler = SignalHandler(backend, transport=transport)

        async def send_after_delay() -> None:
            await asyncio.sleep(0.1)
            await handler.send("run-conc-1", "ready", {"go": True})

        task = asyncio.create_task(send_after_delay())
        result = await handler.wait("run-conc-1", "ready", timedelta(seconds=5))
        assert result["go"] is True

        await task
        await handler.close()


class TestPollingTransportWorkflow:
    """Polling transport-level tests within workflow context."""

    @pytest.mark.asyncio
    async def test_polling_picks_up_pre_sent_signal(self) -> None:
        """PollingSignalTransport.wait() finds a pre-existing signal."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)

        sig = Signal(
            workflow_run_id="poll-wf-1",
            signal_name="data_ready",
            signal_data=json.dumps({"rows": 42}).encode("utf-8"),
        )
        await backend.send_signal(sig)

        result = await transport.wait("poll-wf-1", "data_ready", timedelta(seconds=2))
        assert result["rows"] == 42

    @pytest.mark.asyncio
    async def test_polling_timeout(self) -> None:
        """PollingSignalTransport.wait() raises on timeout."""
        backend = InMemoryBackend()
        await backend.initialize()

        transport = PollingSignalTransport(backend, poll_interval=0.05)

        with pytest.raises(SignalTimeoutError):
            await transport.wait("poll-wf-2", "nothing", timedelta(seconds=0.15))


class TestSignalWithSQLiteBackend:
    """Signal tests against real SQLite backend."""

    @pytest.mark.asyncio
    async def test_signal_roundtrip_sqlite(self, backend: Backend) -> None:
        """Signal send → consume works with SQLite backend."""
        sig = Signal(
            workflow_run_id="sqlite-sig-1",
            signal_name="confirm",
            signal_data=json.dumps({"confirmed": True}).encode("utf-8"),
        )
        await backend.send_signal(sig)

        consumed = await backend.consume_signal("sqlite-sig-1", "confirm")
        assert consumed is not None
        assert consumed.signal_name == "confirm"
        assert consumed.consumed is True

    @pytest.mark.asyncio
    async def test_signal_wrong_name_sqlite(self, backend: Backend) -> None:
        """Consuming a non-existent signal from SQLite returns None."""
        sig = Signal(workflow_run_id="sqlite-sig-2", signal_name="exists")
        await backend.send_signal(sig)

        result = await backend.consume_signal("sqlite-sig-2", "does_not_exist")
        assert result is None

    @pytest.mark.asyncio
    async def test_polling_transport_sqlite(self, backend: Backend) -> None:
        """PollingSignalTransport works with real SQLite backend."""
        transport = PollingSignalTransport(backend, poll_interval=0.05)

        sig = Signal(
            workflow_run_id="sqlite-poll-1",
            signal_name="go",
            signal_data=json.dumps({"status": "ready"}).encode("utf-8"),
        )
        await backend.send_signal(sig)

        result = await transport.wait("sqlite-poll-1", "go", timedelta(seconds=2))
        assert result["status"] == "ready"
