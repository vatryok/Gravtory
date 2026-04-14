# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Local async worker — single-process task claiming and execution loop."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from gravtory.core.dag import DAG
from gravtory.core.types import (
    DLQEntry,
    PendingStep,
    StepOutput,
    StepResult,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.execution import ExecutionEngine
    from gravtory.core.registry import WorkflowRegistry

logger = logging.getLogger("gravtory.worker")


class LocalWorker:
    """Single-process async worker that polls the DB for pending steps.

    Features:
      - Adaptive backoff when idle (poll slower when no work)
      - Bounded concurrency via asyncio.Semaphore
      - Graceful shutdown with drain support
      - Periodic heartbeat updates
    """

    def __init__(
        self,
        worker_id: str,
        backend: Backend,
        registry: WorkflowRegistry,
        execution_engine: ExecutionEngine,
        *,
        poll_interval: float = 0.1,
        max_idle_backoff: float = 5.0,
        max_concurrent: int = 10,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self._worker_id = worker_id
        self._backend = backend
        self._registry = registry
        self._engine = execution_engine
        self._poll_interval = poll_interval
        self._max_idle_backoff = max_idle_backoff
        self._max_concurrent = max_concurrent
        self._heartbeat_interval = heartbeat_interval
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._shutdown_event = asyncio.Event()
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._main_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._consecutive_heartbeat_failures = 0
        self._max_heartbeat_failures = 5

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def is_running(self) -> bool:
        return self._main_task is not None and not self._main_task.done()

    @property
    def active_task_count(self) -> int:
        return len(self._active_tasks)

    async def start(self) -> None:
        """Register worker and start main + heartbeat loops."""
        await self._backend.register_worker(
            WorkerInfo(
                worker_id=self._worker_id,
                status=WorkerStatus.ACTIVE,
            )
        )
        self._shutdown_event.clear()
        self._main_task = asyncio.create_task(self._main_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Worker '%s' started", self._worker_id)

    async def stop(self, *, drain: bool = True, drain_timeout: float = 30.0) -> None:
        """Stop the worker.

        Args:
            drain: If True, wait for active tasks to complete before stopping.
            drain_timeout: Maximum seconds to wait for drain before force-cancelling.
        """
        self._shutdown_event.set()

        if drain and self._active_tasks:
            logger.info(
                "Worker '%s' draining %d active tasks (timeout=%.1fs)",
                self._worker_id,
                len(self._active_tasks),
                drain_timeout,
            )
            _done, pending = await asyncio.wait(
                self._active_tasks,
                timeout=drain_timeout,
            )
            if pending:
                logger.warning(
                    "Worker '%s' drain timeout — cancelling %d remaining tasks",
                    self._worker_id,
                    len(pending),
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        elif self._active_tasks:
            for task in self._active_tasks:
                task.cancel()
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

        if self._main_task is not None and not self._main_task.done():
            self._main_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._main_task

        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task

        await self._backend.deregister_worker(self._worker_id)
        logger.info("Worker '%s' stopped", self._worker_id)

    async def _main_loop(self) -> None:
        """Poll for pending steps with adaptive backoff."""
        idle_count = 0

        while not self._shutdown_event.is_set():
            task = await self._backend.claim_step(self._worker_id)

            if task is None:
                idle_count += 1
                delay = min(
                    self._poll_interval * (1.5**idle_count),
                    self._max_idle_backoff,
                )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay)
                    break  # shutdown requested
                except asyncio.TimeoutError:
                    continue
            else:
                idle_count = 0
                await self._semaphore.acquire()
                coro = self._execute_task(task)
                asyncio_task = asyncio.create_task(coro)
                self._active_tasks.add(asyncio_task)
                asyncio_task.add_done_callback(self._active_tasks.discard)

    async def _execute_task(self, task: PendingStep) -> None:
        """Execute a single claimed task."""
        try:
            if task.id is None:
                logger.error("Claimed task has no ID, skipping")
                return
            task_id: int = task.id
            run = await self._backend.get_workflow_run(task.workflow_run_id)
            if run is None:
                logger.error("Workflow run '%s' not found", task.workflow_run_id)
                await self._backend.fail_step(task_id, error_message="Workflow run not found")
                return

            definition = self._registry.get(run.workflow_name, run.workflow_version)
            step_def = definition.steps.get(task.step_order)
            if step_def is None:
                logger.error(
                    "Step %d not found in workflow '%s'",
                    task.step_order,
                    run.workflow_name,
                )
                await self._backend.fail_step(task_id, error_message="Step definition not found")
                return

            # Load completed steps, restoring checkpoint data to Python objects
            step_outputs = await self._backend.get_step_outputs(task.workflow_run_id)
            checkpoint = self._engine._checkpoint
            completed: dict[int, Any] = {}
            for so in step_outputs:
                if so.status in (StepStatus.COMPLETED, StepStatus.SKIPPED):
                    output = so.output_data
                    if (
                        output is not None
                        and isinstance(output, (bytes, memoryview))
                        and checkpoint is not None
                    ):
                        output = checkpoint.restore(bytes(output))
                    completed[so.step_order] = StepResult(
                        output=output,
                        status=so.status,
                        was_replayed=True,
                    )

            # Decode workflow-level input data
            input_data: dict[str, Any] = {}
            if run.input_data:
                raw = run.input_data
                if isinstance(raw, (bytes, memoryview)):
                    input_data = json.loads(bytes(raw).decode("utf-8"))
                elif isinstance(raw, str):
                    input_data = json.loads(raw)

            # Execute the step
            result = await self._engine.execute_single_step(
                run_id=task.workflow_run_id,
                step_def=step_def,
                completed_steps=completed,
                input_data=input_data,
            )

            # Mark pending step completed
            await self._backend.complete_step(
                task_id,
                StepOutput(
                    workflow_run_id=task.workflow_run_id,
                    step_order=task.step_order,
                    step_name=step_def.name,
                    output_data=result.output,
                    status=StepStatus.COMPLETED,
                    duration_ms=result.duration_ms,
                    retry_count=result.retry_count,
                ),
            )

            # Enqueue next steps
            completed[step_def.order] = result
            dag = DAG(definition.steps)
            next_steps = dag.get_next_steps(step_def.order, completed)
            for ns in next_steps:
                await self._backend.enqueue_step(
                    PendingStep(
                        workflow_run_id=task.workflow_run_id,
                        step_order=ns.order,
                        priority=task.priority,
                        max_retries=ns.retries,
                    )
                )

            # Check if workflow is complete
            if dag.all_steps_done(completed):
                await self._backend.update_workflow_status(
                    task.workflow_run_id, WorkflowStatus.COMPLETED
                )

        except Exception as exc:
            if task.id is None:
                logger.error("Task has no ID during error handling")
                return
            task_id = task.id
            tb = traceback.format_exc()
            logger.error(
                "Worker '%s' step %d failed: %s",
                self._worker_id,
                task.step_order,
                exc,
            )

            if task.retry_count < task.max_retries:
                # Reschedule for retry
                retry_at = datetime.now(tz=timezone.utc)
                await self._backend.fail_step(
                    task_id,
                    error_message=str(exc),
                    retry_at=retry_at,
                )
            else:
                await self._backend.fail_step(task_id, error_message=str(exc))
                await self._backend.update_workflow_status(
                    task.workflow_run_id,
                    WorkflowStatus.FAILED,
                    error_message=str(exc),
                    error_traceback=tb,
                )
                await self._backend.add_to_dlq(
                    DLQEntry(
                        workflow_run_id=task.workflow_run_id,
                        step_order=task.step_order,
                        error_message=str(exc),
                        error_traceback=tb,
                        retry_count=task.retry_count,
                    )
                )
        finally:
            self._semaphore.release()

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat updates."""
        while not self._shutdown_event.is_set():
            try:
                await self._backend.worker_heartbeat(self._worker_id)
                self._consecutive_heartbeat_failures = 0
            except Exception:
                self._consecutive_heartbeat_failures += 1
                logger.warning(
                    "Heartbeat failed for worker '%s' (%d/%d consecutive failures)",
                    self._worker_id,
                    self._consecutive_heartbeat_failures,
                    self._max_heartbeat_failures,
                    exc_info=True,
                )
                if self._consecutive_heartbeat_failures >= self._max_heartbeat_failures:
                    logger.error(
                        "Worker '%s' exceeded max heartbeat failures (%d) — "
                        "initiating self-shutdown to prevent duplicate execution",
                        self._worker_id,
                        self._max_heartbeat_failures,
                    )
                    self._shutdown_event.set()
                    return
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._heartbeat_interval,
                )
                break
            except asyncio.TimeoutError:
                continue
