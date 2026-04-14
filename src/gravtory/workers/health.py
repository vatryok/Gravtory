# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Worker health — heartbeat loop and liveness checks.

Provides a background heartbeat task that periodically updates the worker's
``last_heartbeat`` timestamp via the backend, and a liveness probe that
can be used by external monitoring systems.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from gravtory.core.types import WorkerInfo, WorkerStatus

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.workers.health")


class HealthMonitor:
    """Background heartbeat and liveness monitor for a single worker.

    Args:
        backend: Database backend for persisting heartbeat timestamps.
        worker_id: Unique identifier for this worker.
        heartbeat_interval: Seconds between heartbeat updates.
        stale_threshold: Duration after which a worker is considered stale.
    """

    def __init__(
        self,
        backend: Backend,
        worker_id: str,
        *,
        heartbeat_interval: float = 10.0,
        stale_threshold: timedelta = timedelta(minutes=5),
    ) -> None:
        self._backend = backend
        self._worker_id = worker_id
        self._heartbeat_interval = heartbeat_interval
        self._stale_threshold = stale_threshold
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._current_task: str | None = None
        self._last_heartbeat: datetime | None = None

    @property
    def is_running(self) -> bool:
        """Whether the heartbeat loop is currently active."""
        return self._task is not None and not self._task.done()

    @property
    def last_heartbeat(self) -> datetime | None:
        """Timestamp of the most recent successful heartbeat."""
        return self._last_heartbeat

    async def start(self) -> None:
        """Start the background heartbeat loop."""
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Health monitor started for worker '%s'", self._worker_id)

    async def stop(self) -> None:
        """Stop the heartbeat loop and deregister the worker."""
        self._shutdown_event.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        try:
            await self._backend.deregister_worker(self._worker_id)
        except Exception:
            logger.exception("Failed to deregister worker '%s'", self._worker_id)
        logger.info("Health monitor stopped for worker '%s'", self._worker_id)

    def set_current_task(self, task_id: str | None) -> None:
        """Update the current task being processed by this worker."""
        self._current_task = task_id

    async def register(self) -> None:
        """Register this worker with the backend."""
        now = datetime.now(tz=timezone.utc)
        info = WorkerInfo(
            worker_id=self._worker_id,
            node_id=self._worker_id,
            status=WorkerStatus.ACTIVE,
            last_heartbeat=now,
            current_task=self._current_task,
            started_at=now,
        )
        await self._backend.register_worker(info)
        self._last_heartbeat = now
        logger.debug("Worker '%s' registered", self._worker_id)

    async def heartbeat(self) -> None:
        """Send a single heartbeat update."""
        await self._backend.worker_heartbeat(
            self._worker_id,
            current_task=self._current_task,
        )
        self._last_heartbeat = datetime.now(tz=timezone.utc)
        logger.debug("Heartbeat sent for worker '%s'", self._worker_id)

    async def check_liveness(self) -> bool:
        """Check if this worker is still considered alive.

        Returns:
            True if the last heartbeat is within the stale threshold.
        """
        if self._last_heartbeat is None:
            return False
        now = datetime.now(tz=timezone.utc)
        return (now - self._last_heartbeat) < self._stale_threshold

    async def _heartbeat_loop(self) -> None:
        """Background loop that periodically sends heartbeats."""
        try:
            await self.register()
        except Exception:
            logger.exception("Failed to register worker '%s'", self._worker_id)

        while not self._shutdown_event.is_set():
            try:
                await self.heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Heartbeat failed for worker '%s'",
                    self._worker_id,
                )
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._heartbeat_interval,
                )
