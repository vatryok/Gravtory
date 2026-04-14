# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Abstract Backend interface.

Every concrete backend (PostgreSQL, SQLite, MySQL, MongoDB, Redis)
must subclass Backend and implement all abstract methods.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from gravtory.core.types import WorkflowRun, WorkflowStatus

logger = logging.getLogger("gravtory.backends")

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from gravtory.core.types import (
        Compensation,
        DLQEntry,
        PendingStep,
        Schedule,
        Signal,
        SignalWait,
        StepOutput,
        WorkerInfo,
    )


class Backend(ABC):
    """Abstract backend interface for all Gravtory storage operations.

    Methods are grouped by domain.  Each concrete backend provides
    an implementation that maps to database-specific SQL, document
    operations, or Lua scripts depending on the engine.
    """

    # ── Lifecycle ────────────────────────────────────────────────

    @abstractmethod
    async def initialize(self) -> None:
        """Create tables / indexes / collections if they do not exist."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release connections and clean up resources."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if the backend is reachable and healthy."""
        ...

    # ── Workflow runs ────────────────────────────────────────────

    @abstractmethod
    async def create_workflow_run(self, run: WorkflowRun) -> None:
        """Persist a new workflow run."""
        ...

    @abstractmethod
    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        """Fetch a workflow run by its ID."""
        ...

    @abstractmethod
    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        """Update the status (and optional error/output) of a run."""
        ...

    async def validated_update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        """Update workflow status with state machine validation.

        Raises ``GravtoryError`` if the transition is illegal.
        """
        from gravtory.core.types import validate_transition

        run = await self.get_workflow_run(run_id)
        if run is not None and not validate_transition(run.status, status):
            from gravtory.core.errors import GravtoryError

            raise GravtoryError(
                f"Invalid workflow status transition: {run.status.value} → {status.value} "
                f"for run '{run_id}'"
            )
        await self.update_workflow_status(
            run_id,
            status,
            error_message=error_message,
            error_traceback=error_traceback,
            output_data=output_data,
        )

    async def claim_workflow_run(
        self,
        run_id: str,
        expected_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        """Atomically transition a workflow run from expected_status to new_status.

        Returns True if the transition succeeded (the run was in expected_status
        and is now in new_status). Returns False if the run was NOT in
        expected_status (already claimed by another worker).

        The default implementation is a **non-atomic** fallback that is subject
        to race conditions under concurrent workers.  Concrete backends MUST
        override this with a conditional ``UPDATE ... WHERE status = ?`` pattern
        (or equivalent) to guarantee atomicity.
        """
        logger.warning(
            "Using non-atomic default claim_workflow_run for run '%s'. "
            "Concrete backends should override this method for safe concurrency.",
            run_id,
        )
        run = await self.get_workflow_run(run_id)
        if run is None or run.status != expected_status:
            return False
        await self.update_workflow_status(run_id, new_status)
        return True

    @abstractmethod
    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        """List runs with optional filters."""
        ...

    @abstractmethod
    async def count_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
    ) -> int:
        """Count workflow runs matching filters."""
        ...

    async def delete_workflow_run(self, run_id: str) -> None:
        """Delete a workflow run and its associated step data.

        Used by the retention policy to purge old completed/failed runs.
        Default implementation is a no-op; backends should override.
        """
        logger.debug("delete_workflow_run: %s (no-op default)", run_id)

    async def list_child_runs(self, parent_run_id: str) -> Sequence[WorkflowRun]:
        """Return all workflow runs whose parent_run_id matches.

        Default implementation scans all active runs. Backends with SQL
        should override for efficiency.
        """
        result: list[WorkflowRun] = []
        for status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING):
            runs = await self.list_workflow_runs(status=status, limit=10000)
            result.extend(r for r in runs if r.parent_run_id == parent_run_id)
        return result

    @abstractmethod
    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        """Get all runs with status RUNNING or PENDING (for recovery)."""
        ...

    # ── Step outputs (checkpoints) ───────────────────────────────

    @abstractmethod
    async def save_step_output(self, output: StepOutput) -> None:
        """Atomically persist a step output (checkpoint)."""
        ...

    @abstractmethod
    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        """Retrieve all step outputs for a given run, ordered by step_order."""
        ...

    @abstractmethod
    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        """Retrieve a single step output."""
        ...

    @abstractmethod
    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        """Update the output_data of an existing step output.

        Required for key rotation, checkpoint correction, and data migration.
        Raises BackendError if the step output does not exist.
        """
        ...

    # ── Parallel step results ────────────────────────────────────

    @abstractmethod
    async def checkpoint_parallel_item(
        self,
        run_id: str,
        step_order: int,
        item_index: int,
        output_data: bytes,
    ) -> None:
        """Checkpoint a single parallel item result."""
        ...

    @abstractmethod
    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        """Load all completed parallel item results for a step.

        Returns dict keyed by item_index.
        """
        ...

    # ── Pending steps (task queue) ───────────────────────────────

    @abstractmethod
    async def enqueue_step(self, step: PendingStep) -> None:
        """Insert a pending step into the queue."""
        ...

    @abstractmethod
    async def claim_step(self, worker_id: str) -> PendingStep | None:
        """Atomically claim the next available step for a worker.

        Must use database-native locking (e.g. SELECT FOR UPDATE SKIP LOCKED)
        to prevent double-claiming.
        """
        ...

    @abstractmethod
    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        """Mark a pending step as completed and persist its output atomically."""
        ...

    @abstractmethod
    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: datetime | None = None,
    ) -> None:
        """Mark a pending step as failed, optionally scheduling a retry."""
        ...

    # ── Signals ──────────────────────────────────────────────────

    @abstractmethod
    async def send_signal(self, signal: Signal) -> None:
        """Store a signal for a workflow run."""
        ...

    @abstractmethod
    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        """Retrieve and mark a signal as consumed (atomic)."""
        ...

    @abstractmethod
    async def register_signal_wait(self, wait: SignalWait) -> None:
        """Register that a step is waiting for a signal."""
        ...

    # ── Compensation (sagas) ─────────────────────────────────────

    @abstractmethod
    async def save_compensation(self, comp: Compensation) -> None:
        """Persist a compensation record."""
        ...

    @abstractmethod
    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        """Get all compensations for a run, ordered by step_order DESC."""
        ...

    @abstractmethod
    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        """Update the status of a compensation."""
        ...

    # ── Scheduling ───────────────────────────────────────────────

    @abstractmethod
    async def save_schedule(self, schedule: Schedule) -> None:
        """Create or update a schedule."""
        ...

    @abstractmethod
    async def get_due_schedules(self) -> Sequence[Schedule]:
        """Return all schedules whose next_run_at is in the past."""
        ...

    @abstractmethod
    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: datetime, next_run_at: datetime | None
    ) -> None:
        """Update the last and next run times for a schedule."""
        ...

    @abstractmethod
    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        """Return all enabled schedules regardless of next_run_at."""
        ...

    @abstractmethod
    async def list_all_schedules(self) -> Sequence[Schedule]:
        """Return all schedules (enabled and disabled)."""
        ...

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        """Return a single schedule by ID, or None if not found.

        Default implementation filters from list_all_schedules().
        Backends may override for O(1) lookup.
        """
        for s in await self.list_all_schedules():
            if s.id == schedule_id:
                return s
        return None

    # ── Distributed locks ────────────────────────────────────────

    @abstractmethod
    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        """Try to acquire a named lock. Return True if acquired."""
        ...

    @abstractmethod
    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        """Release a named lock. Return True if released."""
        ...

    @abstractmethod
    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        """Extend a lock's TTL. Return True if refreshed."""
        ...

    # ── Dead letter queue ────────────────────────────────────────

    @abstractmethod
    async def add_to_dlq(self, entry: DLQEntry) -> None:
        """Move a failed step to the DLQ."""
        ...

    @abstractmethod
    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        """List DLQ entries."""
        ...

    @abstractmethod
    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        """Get a single DLQ entry by ID. O(1) lookup."""
        ...

    @abstractmethod
    async def count_dlq(self, *, namespace: str = "default") -> int:
        """Count DLQ entries for a namespace. More efficient than list_dlq for size checks."""
        ...

    @abstractmethod
    async def remove_from_dlq(self, entry_id: int) -> None:
        """Remove a DLQ entry (after manual retry or discard)."""
        ...

    @abstractmethod
    async def purge_dlq(self, *, namespace: str = "default") -> int:
        """Delete all DLQ entries. Returns count deleted."""
        ...

    # ── Workers ──────────────────────────────────────────────────

    @abstractmethod
    async def register_worker(self, worker: WorkerInfo) -> None:
        """Register a worker with the backend."""
        ...

    @abstractmethod
    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        """Update worker heartbeat timestamp and optional current task."""
        ...

    @abstractmethod
    async def deregister_worker(self, worker_id: str) -> None:
        """Remove a worker registration."""
        ...

    @abstractmethod
    async def list_workers(self) -> Sequence[WorkerInfo]:
        """List all registered workers."""
        ...

    @abstractmethod
    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        """Get workers whose heartbeat is older than threshold."""
        ...

    # ── Task reclamation ───────────────────────────────────────────

    @abstractmethod
    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        """Reset all running tasks for a given worker back to pending.

        Called when a stale worker is detected. Returns the number
        of tasks reclaimed.
        """
        ...

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        """Persist circuit breaker state as a JSON string, keyed by *name*.

        Default implementation is a no-op (in-memory breakers don't need storage).
        Backends that support it should override this method.
        """

    async def load_circuit_state(self, name: str) -> str | None:
        """Load circuit breaker state JSON for *name*. Return None if not found.

        Default implementation returns None (no persisted state).
        Backends that support it should override this method.
        """
        return None

    # ── Dynamic workflow persistence ─────────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        """Persist a dynamic workflow definition as JSON.

        Default implementation is a no-op. Override in backends that support it.
        """
        logger.debug("save_workflow_definition: %s v%d", name, version)

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        """Load all persisted dynamic workflow definitions.

        Returns a list of ``(name, version, definition_json)`` tuples.
        Default implementation returns an empty list.
        """
        return []

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        """Remove a persisted dynamic workflow definition.

        Default implementation is a no-op.
        """
        logger.debug("delete_workflow_definition: %s v%d", name, version)

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        """Persist multiple workflow definitions in one call.

        *definitions* is a list of ``(name, version, definition_json)`` tuples.
        Returns the number of definitions saved.

        Default implementation calls :meth:`save_workflow_definition` sequentially.
        Backends with transaction support should override for atomicity.
        """
        for name, version, definition_json in definitions:
            await self.save_workflow_definition(name, version, definition_json)
        logger.info("save_workflow_definitions_batch: saved %d definitions", len(definitions))
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        """Delete multiple workflow definitions in one call.

        *keys* is a list of ``(name, version)`` tuples.
        Returns the number of definitions targeted for deletion.

        Default implementation calls :meth:`delete_workflow_definition` sequentially.
        """
        for name, version in keys:
            await self.delete_workflow_definition(name, version)
        logger.info("delete_workflow_definitions_batch: deleted %d definitions", len(keys))
        return len(keys)

    # ── Concurrency control ───────────────────────────────────────

    @abstractmethod
    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        """Check if starting a new run is within concurrency limits.

        Returns True if the current count of active runs (running/pending)
        for the given workflow name + namespace is below *max_concurrent*.
        """
        ...
