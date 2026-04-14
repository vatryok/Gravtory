# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Multi-process worker pool — fork N workers, supervisor restarts crashed ones."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing
import os
import signal
from typing import Any

logger = logging.getLogger("gravtory.pool")


class WorkerPool:
    """Fork N worker processes, each running a :class:`LocalWorker`.

    A supervisor in the main process monitors process health and
    restarts any that crash unexpectedly.

    Key design decisions:
      - Each child process creates its **own** DB connection pool.
      - Shutdown uses a shared :class:`multiprocessing.Event`.
      - Supervisor detects crashed workers via ``Process.is_alive()``.
      - Worker IDs include *node_id* for distributed identification.
    """

    def __init__(
        self,
        count: int,
        backend_url: str,
        *,
        node_id: str = "",
        max_concurrent_per_worker: int = 10,
        poll_interval: float = 0.1,
        max_idle_backoff: float = 5.0,
        table_prefix: str = "gravtory_",
        registry_setup_fn: Any | None = None,
    ) -> None:
        self._count = count
        self._backend_url = backend_url
        self._node_id = node_id or f"node-{os.getpid()}"
        self._max_concurrent = max_concurrent_per_worker
        self._poll_interval = poll_interval
        self._max_idle_backoff = max_idle_backoff
        self._table_prefix = table_prefix
        self._registry_setup_fn = registry_setup_fn
        # Use 'spawn' context to avoid inheriting parent's DB connections
        # and file descriptors (asyncpg connections are not fork-safe).
        self._mp_ctx = multiprocessing.get_context("spawn")
        self._processes: dict[str, multiprocessing.process.BaseProcess] = {}
        self._shutdown_event = self._mp_ctx.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._started = False
        self._restart_counts: dict[str, int] = {}
        self._max_restarts = 5
        if registry_setup_fn is None:
            logger.warning(
                "WorkerPool created without registry_setup_fn — forked workers "
                "will have an empty WorkflowRegistry and cannot execute workflows. "
                "Pass a registry_setup_fn callback that registers workflows."
            )

    @property
    def is_running(self) -> bool:
        return self._started and not self._shutdown_event.is_set()

    @property
    def worker_count(self) -> int:
        return len(self._processes)

    @property
    def alive_count(self) -> int:
        return sum(1 for p in self._processes.values() if p.is_alive())

    def _worker_id(self, index: int) -> str:
        return f"{self._node_id}-worker-{index}"

    async def start(self) -> None:
        """Start N worker processes and the supervisor loop."""
        if self._started:
            return

        self._shutdown_event.clear()

        for i in range(self._count):
            self._start_worker_process(i)

        self._supervisor_task = asyncio.create_task(self._supervisor_loop())
        self._started = True
        logger.info(
            "WorkerPool started: %d workers on node '%s'",
            self._count,
            self._node_id,
        )

    async def stop(self, *, drain: bool = True, drain_timeout: float = 30.0) -> None:
        """Stop all worker processes.

        Args:
            drain: Wait for workers to finish active tasks before stopping.
            drain_timeout: Max seconds to wait for drain before force-killing.
        """
        self._shutdown_event.set()

        if self._supervisor_task is not None and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._supervisor_task

        if drain:
            # Wait up to drain_timeout for processes to exit gracefully
            loop = asyncio.get_running_loop()
            deadline = loop.time() + drain_timeout
            for _wid, proc in list(self._processes.items()):
                remaining = max(0, deadline - loop.time())
                await asyncio.get_running_loop().run_in_executor(None, proc.join, remaining)

        # SIGTERM any still alive
        for wid, proc in list(self._processes.items()):
            if proc.is_alive():
                logger.warning("Sending SIGTERM to worker '%s' (pid %d)", wid, proc.pid or 0)
                with contextlib.suppress(OSError):
                    proc.terminate()

        # Wait 10s more then SIGKILL
        loop = asyncio.get_running_loop()
        for wid, proc in list(self._processes.items()):
            await loop.run_in_executor(None, proc.join, 10.0)
            if proc.is_alive():
                logger.error("Force-killing worker '%s' (pid %d)", wid, proc.pid or 0)
                with contextlib.suppress(OSError):
                    proc.kill()
                await loop.run_in_executor(None, proc.join, 5.0)

        self._processes.clear()
        self._started = False
        logger.info("WorkerPool stopped on node '%s'", self._node_id)

    def _start_worker_process(self, index: int) -> None:
        """Fork a single worker process."""
        wid = self._worker_id(index)
        proc = self._mp_ctx.Process(
            target=_worker_process_entry,
            args=(
                wid,
                self._backend_url,
                self._table_prefix,
                self._shutdown_event,
                self._max_concurrent,
                self._poll_interval,
                self._max_idle_backoff,
                self._registry_setup_fn,
            ),
            name=f"gravtory-{wid}",
            daemon=True,
        )
        proc.start()
        self._processes[wid] = proc
        logger.info("Started worker process '%s' (pid %d)", wid, proc.pid or 0)

    async def _supervisor_loop(self) -> None:
        """Monitor worker processes and restart crashed ones with backoff."""
        while not self._shutdown_event.is_set():
            for i in range(self._count):
                wid = self._worker_id(i)
                proc = self._processes.get(wid)
                if proc is not None and not proc.is_alive():
                    exit_code = proc.exitcode
                    # Clean up the dead process
                    proc.join(timeout=1)

                    restarts = self._restart_counts.get(wid, 0)
                    if restarts >= self._max_restarts:
                        logger.error(
                            "Worker '%s' exceeded max restarts (%d) — "
                            "not restarting. Manual intervention required.",
                            wid,
                            self._max_restarts,
                        )
                        continue

                    # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    backoff = min(2**restarts, 16)
                    logger.warning(
                        "Worker '%s' exited (code=%s), restarting in %ds (restart %d/%d)",
                        wid,
                        exit_code,
                        backoff,
                        restarts + 1,
                        self._max_restarts,
                    )
                    await asyncio.sleep(backoff)
                    if self._shutdown_event.is_set():
                        break
                    self._restart_counts[wid] = restarts + 1
                    self._start_worker_process(i)

            try:
                await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        None, self._shutdown_event.wait, 5.0
                    ),
                    timeout=6.0,
                )
                break  # shutdown requested
            except asyncio.TimeoutError:
                continue


def _worker_process_entry(
    worker_id: str,
    backend_url: str,
    table_prefix: str,
    shutdown_event: Any,
    max_concurrent: int,
    poll_interval: float,
    max_idle_backoff: float,
    registry_setup_fn: Any | None = None,
) -> None:
    """Entry point for a forked worker process.

    Each child creates its own backend, registry, and engine from the
    connection string to avoid pickling non-serializable objects.
    The optional *registry_setup_fn* is called with the fresh registry
    so the parent can populate it with workflow definitions.
    """
    from gravtory.backends import create_backend
    from gravtory.core.execution import ExecutionEngine
    from gravtory.core.registry import WorkflowRegistry
    from gravtory.workers.local import LocalWorker

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Each child process creates its own DB connection
    backend = create_backend(backend_url, table_prefix=table_prefix)
    registry = WorkflowRegistry()
    if registry_setup_fn is not None:
        registry_setup_fn(registry)
    engine = ExecutionEngine(registry, backend)

    worker = LocalWorker(
        worker_id=worker_id,
        backend=backend,
        registry=registry,
        execution_engine=engine,
        poll_interval=poll_interval,
        max_idle_backoff=max_idle_backoff,
        max_concurrent=max_concurrent,
    )

    def _handle_sigterm(signum: int, frame: Any) -> None:
        shutdown_event.set()
        worker._shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    async def _run() -> None:
        await backend.initialize()
        await worker.start()
        # Poll the multiprocessing shutdown event
        while not shutdown_event.is_set():
            await asyncio.sleep(0.5)
        await worker.stop(drain=True)
        await backend.close()

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        loop.run_until_complete(worker.stop(drain=False))
    finally:
        loop.close()
