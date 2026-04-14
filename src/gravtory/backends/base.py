"""Abstract Backend interface.

Every concrete backend (PostgreSQL, SQLite, MySQL, MongoDB, Redis)
must subclass Backend and implement all abstract methods.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from gravtory.core.types import (
        Compensation,
        DLQEntry,
        PendingStep,
        Schedule,
        Signal,
        SignalWait,
        StepOutput,
        WorkerInfo,
        WorkflowRun,
        WorkflowStatus,
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
        retry_at: Any | None = None,
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
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        """Update the last and next run times for a schedule."""
        ...

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
    async def remove_from_dlq(self, entry_id: int) -> None:
        """Remove a DLQ entry (after manual retry or discard)."""
        ...

    # ── Workers ──────────────────────────────────────────────────

    @abstractmethod
    async def register_worker(self, worker: WorkerInfo) -> None:
        """Register a worker with the backend."""
        ...

    @abstractmethod
    async def heartbeat_worker(self, worker_id: str) -> None:
        """Update the heartbeat timestamp for a worker."""
        ...

    @abstractmethod
    async def deregister_worker(self, worker_id: str) -> None:
        """Remove a worker registration."""
        ...

    @abstractmethod
    async def list_workers(self) -> Sequence[WorkerInfo]:
        """List all registered workers."""
        ...
