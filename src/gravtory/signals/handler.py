# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""SignalHandler — primary interface for sending and receiving signals.

Delegates real-time delivery to a transport (LISTEN/NOTIFY or polling).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from gravtory.core.types import Signal, SignalWait
from gravtory.signals._serde import deserialize_signal_data

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.signals.transport import SignalTransport

logger = logging.getLogger("gravtory.signals.handler")


class SignalHandler:
    """Send and receive signals for workflow runs.

    Integrates with a :class:`SignalTransport` for real-time delivery:
      - PostgreSQLSignalTransport for push-based LISTEN/NOTIFY
      - PollingSignalTransport as fallback for all other backends

    Usage::

        handler = SignalHandler(backend)

        # Send a signal (from external API, dashboard, etc.)
        await handler.send("run-123", "approval", {"approved": True})

        # Wait for a signal (inside a step execution)
        data = await handler.wait("run-123", "approval", timedelta(hours=24))
    """

    def __init__(
        self,
        backend: Backend,
        transport: SignalTransport | None = None,
    ) -> None:
        self._backend = backend
        self._transport = transport or self._detect_transport(backend)
        # Local in-process waiters: (run_id, signal_name) → Future
        self._waiters: dict[tuple[str, str], asyncio.Future[dict[str, Any]]] = {}

    @property
    def transport(self) -> SignalTransport:
        return self._transport

    async def send(
        self,
        run_id: str,
        signal_name: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Send a signal to a workflow run.

        Steps:
          1. Serialize data to JSON bytes
          2. Store signal in DB via backend
          3. Notify transport (for real-time push)
          4. Resolve any local in-process waiter
        """
        serialized = json.dumps(data or {}).encode("utf-8")

        sig = Signal(
            workflow_run_id=run_id,
            signal_name=signal_name,
            signal_data=serialized,
        )
        await self._backend.send_signal(sig)
        logger.info("Signal '%s' sent to run '%s'", signal_name, run_id)

        # Notify transport for push-based delivery
        await self._transport.notify(run_id, signal_name)

        # Resolve local in-process waiter if one exists
        key = (run_id, signal_name)
        future = self._waiters.pop(key, None)
        if future is not None and not future.done():
            future.set_result(data or {})

    async def wait(
        self,
        run_id: str,
        signal_name: str,
        timeout: timedelta,
    ) -> dict[str, Any]:
        """Wait for a signal to arrive.

        Steps:
          1. Register wait in DB (for observability and timeout tracking)
          2. Check if signal already arrived (race condition prevention)
          3. Race local in-process future against transport wait
             - Local future: resolved by ``send()`` in same process (fast path)
             - Transport: polling or LISTEN/NOTIFY (cross-process delivery)

        Raises:
            SignalTimeoutError: If signal is not received within timeout.
        """
        timeout_at = datetime.now(tz=timezone.utc) + timeout
        await self._backend.register_signal_wait(
            SignalWait(
                workflow_run_id=run_id,
                signal_name=signal_name,
                timeout_at=timeout_at,
            )
        )

        # Check if signal already exists (race condition: sent before wait)
        existing = await self._backend.consume_signal(run_id, signal_name)
        if existing is not None:
            logger.debug(
                "Signal '%s' already available for run '%s'",
                signal_name,
                run_id,
            )
            return deserialize_signal_data(existing.signal_data)

        # Race local future against transport wait.
        # Local future handles in-process send(); transport handles
        # cross-process delivery (polling DB or LISTEN/NOTIFY).
        key = (run_id, signal_name)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._waiters[key] = future

        transport_task = asyncio.create_task(
            self._transport.wait(run_id, signal_name, timeout),
        )

        try:
            done, _pending = await asyncio.wait(
                {future, transport_task},
                timeout=timeout.total_seconds(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                # Both timed out
                from gravtory.core.errors import SignalTimeoutError

                raise SignalTimeoutError(signal_name, timeout.total_seconds())

            # Return from whichever finished first
            winner = next(iter(done))
            result: dict[str, Any] = winner.result()
            return result
        finally:
            self._waiters.pop(key, None)
            if not transport_task.done():
                transport_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await transport_task

    @staticmethod
    def _detect_transport(backend: Backend) -> SignalTransport:
        """Auto-detect the best transport for the given backend.

        PostgreSQLBackend → PostgreSQLSignalTransport (LISTEN/NOTIFY)
        Everything else   → PollingSignalTransport (DB polling)
        """
        from gravtory.signals.transport import (
            PollingSignalTransport,
            PostgreSQLSignalTransport,
        )

        if type(backend).__name__ == "PostgreSQLBackend":
            logger.info("Using PostgreSQL LISTEN/NOTIFY signal transport")
            return PostgreSQLSignalTransport(backend)

        logger.info(
            "Using polling signal transport (backend: %s)",
            type(backend).__name__,
        )
        return PollingSignalTransport(backend)

    async def close(self) -> None:
        """Cleanup transport resources."""
        await self._transport.close()

        # Cancel any pending waiters
        for _key, future in self._waiters.items():
            if not future.done():
                future.cancel()
        self._waiters.clear()
