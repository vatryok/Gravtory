# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Distributed coordination — stale worker detection and task reclamation."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory.distributed")


async def detect_and_reclaim_stale_tasks(
    backend: Backend,
    stale_threshold: timedelta = timedelta(minutes=5),
) -> int:
    """Find tasks claimed by workers whose heartbeat has gone stale.

    Algorithm:
      1. List all registered workers.
      2. Identify workers whose ``last_heartbeat`` is older than *stale_threshold*.
      3. For each stale worker, find all pending steps with
         ``status='running'`` and ``worker_id=stale_worker``.
      4. Reset those steps to ``status='pending'``, ``worker_id=None``.
      5. Deregister the stale worker.

    Returns:
        Number of tasks reclaimed.
    """
    now = datetime.now(tz=timezone.utc)
    cutoff = now - stale_threshold
    workers = await backend.list_workers()

    reclaimed = 0

    for worker in workers:
        if worker.last_heartbeat is None or worker.last_heartbeat < cutoff:
            logger.warning(
                "Stale worker detected: '%s' (last heartbeat: %s)",
                worker.worker_id,
                worker.last_heartbeat,
            )

            # Reclaim tasks assigned to this worker
            reclaimed += await _reclaim_worker_tasks(backend, worker.worker_id)

            # Deregister stale worker
            await backend.deregister_worker(worker.worker_id)
            logger.info("Deregistered stale worker '%s'", worker.worker_id)

    if reclaimed:
        logger.info("Reclaimed %d tasks from stale workers", reclaimed)

    return reclaimed


async def _reclaim_worker_tasks(backend: Backend, worker_id: str) -> int:
    """Reset all running tasks for a given worker back to pending.

    Delegates to the backend's reclaim_worker_tasks() method which is
    implemented by all backends.
    """
    return await backend.reclaim_worker_tasks(worker_id)
