# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Leader election — distributed lock-based leader election for the scheduler.

Uses the backend's distributed lock primitives to ensure only one scheduler
instance runs at a time across the cluster. The leader periodically renews
its lock; if it fails to renew, another node can take over.

The :class:`Scheduler` in ``scheduling.engine`` uses this module internally.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.scheduling.leader")


class LeaderElector:
    """Distributed leader election using backend locks.

    Only one :class:`LeaderElector` with the same *lock_name* can be the
    leader at any given time.  The leader must periodically call
    :meth:`renew` (or rely on the background loop started by :meth:`start`)
    to keep the lock alive.

    Args:
        backend: Database backend providing distributed lock methods.
        lock_name: Name of the distributed lock (e.g. ``"gravtory_scheduler"``).
        node_id: Unique identifier for this node.
        ttl: Lock time-to-live in seconds.  If the leader fails to renew
            within this window, the lock expires and another node can acquire it.
        renew_interval: Seconds between automatic lock renewal attempts.
    """

    def __init__(
        self,
        backend: Backend,
        lock_name: str,
        node_id: str,
        *,
        ttl: float = 30.0,
        renew_interval: float = 10.0,
    ) -> None:
        self._backend = backend
        self._lock_name = lock_name
        self._node_id = node_id
        self._ttl = ttl
        self._renew_interval = renew_interval
        self._is_leader = False
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def is_leader(self) -> bool:
        """Whether this node currently holds the leader lock."""
        return self._is_leader

    async def try_acquire(self) -> bool:
        """Attempt to acquire the leader lock.

        Returns:
            True if this node is now the leader.
        """
        acquired = await self._backend.acquire_lock(
            self._lock_name,
            self._node_id,
            int(self._ttl),
        )
        self._is_leader = bool(acquired)
        if self._is_leader:
            logger.info("Node '%s' acquired leader lock '%s'", self._node_id, self._lock_name)
        return self._is_leader

    async def renew(self) -> bool:
        """Renew the leader lock.

        Returns:
            True if the lock was successfully renewed.
        """
        if not self._is_leader:
            return False
        renewed = await self._backend.refresh_lock(
            self._lock_name,
            self._node_id,
            int(self._ttl),
        )
        if not renewed:
            logger.warning(
                "Node '%s' lost leader lock '%s'",
                self._node_id,
                self._lock_name,
            )
            self._is_leader = False
        return self._is_leader

    async def release(self) -> None:
        """Release the leader lock."""
        if self._is_leader:
            await self._backend.release_lock(self._lock_name, self._node_id)
            self._is_leader = False
            logger.info("Node '%s' released leader lock '%s'", self._node_id, self._lock_name)

    async def start(self) -> None:
        """Start a background loop that acquires and renews the leader lock."""
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._election_loop())
        logger.info(
            "Leader elector started for lock '%s' on node '%s'",
            self._lock_name,
            self._node_id,
        )

    async def stop(self) -> None:
        """Stop the election loop and release the lock."""
        self._shutdown_event.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self.release()
        logger.info(
            "Leader elector stopped for lock '%s' on node '%s'",
            self._lock_name,
            self._node_id,
        )

    async def _election_loop(self) -> None:
        """Background loop: acquire → renew → retry on loss."""
        while not self._shutdown_event.is_set():
            try:
                if not self._is_leader:
                    await self.try_acquire()
                else:
                    renewed = await self.renew()
                    if not renewed:
                        # Renewal failed — release lock immediately so another
                        # node can acquire without waiting for TTL expiry.
                        with contextlib.suppress(Exception):
                            await self._backend.release_lock(self._lock_name, self._node_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Leader election error on node '%s'", self._node_id)
                self._is_leader = False
                # Best-effort release on unexpected errors
                with contextlib.suppress(Exception):
                    await self._backend.release_lock(self._lock_name, self._node_id)

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._renew_interval,
                )
