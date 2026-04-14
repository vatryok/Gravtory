# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Signal transports — real-time delivery mechanisms.

Provides:
  - SignalTransport: abstract base for signal delivery
  - PollingSignalTransport: fallback for any backend (polls DB)
  - PostgreSQLSignalTransport: real-time via LISTEN/NOTIFY
"""

from __future__ import annotations

import abc
import asyncio
import logging
from typing import TYPE_CHECKING, Any

from gravtory.core.errors import SignalTimeoutError
from gravtory.signals._serde import deserialize_signal_data

if TYPE_CHECKING:
    from datetime import timedelta

    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.signals.transport")


class SignalTransport(abc.ABC):
    """Abstract base for signal delivery transports."""

    @abc.abstractmethod
    async def wait(
        self,
        run_id: str,
        signal_name: str,
        timeout: timedelta,
    ) -> dict[str, Any]:
        """Wait for a signal to arrive. Returns the deserialized signal data."""
        ...

    @abc.abstractmethod
    async def notify(self, run_id: str, signal_name: str) -> None:
        """Notify that a signal has been stored (for push-based transports)."""
        ...

    async def close(self) -> None:
        """Cleanup resources (override if needed)."""


class PollingSignalTransport(SignalTransport):
    """Polls the DB for signals at a configurable interval.

    Works with every backend. Used as fallback when no push-based
    transport is available (SQLite, MySQL, MongoDB, Redis).
    """

    def __init__(self, backend: Backend, poll_interval: float = 1.0) -> None:
        self._backend = backend
        self._poll_interval = poll_interval

    async def wait(
        self,
        run_id: str,
        signal_name: str,
        timeout: timedelta,
    ) -> dict[str, Any]:
        """Poll the DB until a signal is found or timeout expires."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout.total_seconds()

        while loop.time() < deadline:
            signal = await self._backend.consume_signal(run_id, signal_name)
            if signal is not None:
                return deserialize_signal_data(signal.signal_data)

            remaining = deadline - loop.time()
            sleep_time = min(self._poll_interval, max(remaining, 0))
            if sleep_time <= 0:
                break
            await asyncio.sleep(sleep_time)

        raise SignalTimeoutError(signal_name, timeout.total_seconds())

    async def notify(self, run_id: str, signal_name: str) -> None:
        """No-op for polling transport — data is already in DB."""


class PostgreSQLSignalTransport(SignalTransport):
    """Real-time signal delivery via PostgreSQL LISTEN/NOTIFY.

    Uses a **single shared connection** for all LISTEN channels to avoid
    pool exhaustion. Multiple concurrent wait() calls multiplex through
    one dedicated connection with channel→Future dispatch.

    Channel name convention: ``gravtory_sig_{run_id}_{signal_name}``
    """

    def __init__(self, backend: Any) -> None:
        self._backend = backend
        self._listeners: dict[str, list[asyncio.Future[bool]]] = {}
        self._shared_conn: Any = None
        self._lock = asyncio.Lock()

    async def _ensure_shared_conn(self) -> Any:
        """Lazily acquire a single dedicated connection for all LISTEN channels."""
        if self._shared_conn is not None:
            return self._shared_conn
        async with self._lock:
            if self._shared_conn is not None:
                return self._shared_conn
            pool = self._backend._pool
            if pool is None:
                raise RuntimeError("PostgreSQL connection pool not initialized")
            self._shared_conn = await pool.acquire()
            return self._shared_conn

    async def wait(
        self,
        run_id: str,
        signal_name: str,
        timeout: timedelta,
    ) -> dict[str, Any]:
        """Wait for a signal using LISTEN/NOTIFY on the shared connection."""
        channel = _channel_name(run_id, signal_name)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()

        conn = await self._ensure_shared_conn()

        # Register listener for this channel (multiple waiters possible)
        async with self._lock:
            is_new_channel = channel not in self._listeners
            self._listeners.setdefault(channel, []).append(future)
            if is_new_channel:
                await conn.add_listener(channel, self._on_notify)

        try:
            # Double-check: signal may have arrived between handler check and LISTEN
            existing = await self._backend.consume_signal(run_id, signal_name)
            if existing is not None:
                return deserialize_signal_data(existing.signal_data)

            try:
                await asyncio.wait_for(future, timeout=timeout.total_seconds())
            except asyncio.TimeoutError:
                raise SignalTimeoutError(signal_name, timeout.total_seconds()) from None

            # Signal notification received — read data from DB
            signal = await self._backend.consume_signal(run_id, signal_name)
            if signal is not None:
                return deserialize_signal_data(signal.signal_data)

            logger.error(
                "Signal notification received for '%s' on run '%s' but data not in DB",
                signal_name,
                run_id,
            )
            raise SignalTimeoutError(signal_name, timeout.total_seconds())
        finally:
            async with self._lock:
                futures = self._listeners.get(channel, [])
                if future in futures:
                    futures.remove(future)
                if not futures:
                    # Last waiter for this channel — stop listening
                    self._listeners.pop(channel, None)
                    try:
                        await conn.remove_listener(channel, self._on_notify)
                    except Exception:
                        logger.debug("Failed to remove listener for channel %s", channel)

    async def notify(self, run_id: str, signal_name: str) -> None:
        """Send NOTIFY on the signal channel."""
        channel = _channel_name(run_id, signal_name)
        pool = self._backend._pool
        if pool is None:
            return

        async with pool.acquire() as conn:
            await conn.execute("SELECT pg_notify($1, $2)", channel, "")

    def _on_notify(
        self,
        connection: Any,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """Callback when NOTIFY is received — resolve ALL waiting futures for this channel."""
        futures = self._listeners.get(channel, [])
        for future in futures:
            if not future.done():
                future.set_result(True)

    async def close(self) -> None:
        """Release the shared connection back to the pool."""
        if self._shared_conn is not None:
            pool = self._backend._pool
            if pool is not None:
                try:
                    await pool.release(self._shared_conn)
                except Exception:
                    logger.debug("Failed to release shared signal connection")
            self._shared_conn = None
            self._listeners.clear()


def _channel_name(run_id: str, signal_name: str) -> str:
    """Build a deterministic channel name for LISTEN/NOTIFY."""
    # Sanitize for PG identifier: replace non-alnum with '_'
    safe_run = "".join(c if c.isalnum() else "_" for c in run_id)
    safe_sig = "".join(c if c.isalnum() else "_" for c in signal_name)
    return f"gravtory_sig_{safe_run}_{safe_sig}"
