# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""In-memory backend for testing. Data is lost on process exit."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from gravtory.backends.base import Backend
from gravtory.core.types import (
    Compensation,
    DLQEntry,
    Lock,
    PendingStep,
    Schedule,
    Signal,
    SignalWait,
    StepOutput,
    StepStatus,
    WorkerInfo,
    WorkflowRun,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class InMemoryBackend(Backend):
    """In-memory backend for testing. NOT for production use.

    When ``checkpoint_engine`` is provided, step output data is serialized
    through the checkpoint pipeline (serialize → compress → encrypt),
    matching the behavior of SQL backends. Without it, raw Python objects
    are stored directly (faster but bypasses the serialization path).
    """

    def __init__(self, checkpoint_engine: Any | None = None) -> None:
        self._checkpoint_engine = checkpoint_engine
        self._runs: dict[str, WorkflowRun] = {}
        self._step_outputs: dict[tuple[str, int], StepOutput] = {}
        self._output_values: dict[tuple[str, int], Any] = {}  # actual Python objects
        self._pending_steps: dict[int, PendingStep] = {}
        self._signals: list[Signal] = []
        self._signal_waits: list[SignalWait] = []
        self._compensations: list[Compensation] = []
        self._schedules: dict[str, Schedule] = {}
        self._locks: dict[str, Lock] = {}
        self._dlq: list[DLQEntry] = []
        self._workers: dict[str, WorkerInfo] = {}
        self._parallel_results: dict[tuple[str, int, int], bytes] = {}
        self._circuit_states: dict[str, str] = {}
        self._workflow_defs: dict[tuple[str, int], str] = {}
        self._next_id: int = 1
        self._connected: bool = False

    def _auto_id(self) -> int:
        val = self._next_id
        self._next_id += 1
        return val

    # ── Lifecycle ────────────────────────────────────────────────

    async def initialize(self) -> None:
        self._connected = True

    async def close(self) -> None:
        self._connected = False

    async def health_check(self) -> bool:
        return self._connected

    # ── Workflow runs ────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        if run.id in self._runs:
            return  # idempotent
        now = _now()
        run.created_at = run.created_at or now
        run.updated_at = now
        self._runs[run.id] = run

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        return self._runs.get(run_id)

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        run = self._runs.get(run_id)
        if run is None:
            return
        run.status = status
        run.updated_at = _now()
        if error_message is not None:
            run.error_message = error_message
        if error_traceback is not None:
            run.error_traceback = error_traceback
        if output_data is not None:
            run.output_data = output_data
        if status == WorkflowStatus.COMPLETED:
            run.completed_at = _now()

    async def claim_workflow_run(
        self,
        run_id: str,
        expected_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        run = self._runs.get(run_id)
        if run is None or run.status != expected_status:
            return False
        run.status = new_status
        run.updated_at = _now()
        return True

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        results = list(self._runs.values())
        results = [r for r in results if r.namespace == namespace]
        if status is not None:
            results = [r for r in results if r.status == status]
        if workflow_name is not None:
            results = [r for r in results if r.workflow_name == workflow_name]
        return results[offset : offset + limit]

    async def count_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
    ) -> int:
        results = [r for r in self._runs.values() if r.namespace == namespace]
        if status is not None:
            results = [r for r in results if r.status == status]
        if workflow_name is not None:
            results = [r for r in results if r.workflow_name == workflow_name]
        return len(results)

    async def delete_workflow_run(self, run_id: str) -> None:
        self._runs.pop(run_id, None)
        keys_to_remove = [k for k in self._step_outputs if k[0] == run_id]
        for k in keys_to_remove:
            del self._step_outputs[k]

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        return [
            r
            for r in self._runs.values()
            if r.status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING)
        ]

    # ── Step outputs (checkpoints) ───────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        key = (output.workflow_run_id, output.step_order)
        if key in self._step_outputs:
            return  # idempotent — don't overwrite existing checkpoint
        output.id = output.id or self._auto_id()
        output.created_at = output.created_at or _now()
        # When checkpoint_engine is set, serialize output to bytes (matching SQL backends)
        if (
            self._checkpoint_engine is not None
            and output.output_data is not None
            and not isinstance(output.output_data, (bytes, memoryview))
        ):
            output.output_data = self._checkpoint_engine.process(output.output_data)
        self._step_outputs[key] = output
        # Update run's current_step
        run = self._runs.get(output.workflow_run_id)
        if run is not None:
            run.current_step = output.step_order
            run.updated_at = _now()

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        outputs = [v for (rid, _), v in self._step_outputs.items() if rid == run_id]
        return sorted(outputs, key=lambda o: o.step_order)

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        return self._step_outputs.get((run_id, step_order))

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        key = (run_id, step_order)
        existing = self._step_outputs.get(key)
        if existing is None:
            from gravtory.core.errors import BackendError

            raise BackendError(
                f"Step output not found for run_id={run_id!r}, step_order={step_order}"
            )
        existing.output_data = output_data

    # ── Parallel step results ────────────────────────────────────

    async def checkpoint_parallel_item(
        self,
        run_id: str,
        step_order: int,
        item_index: int,
        output_data: bytes,
    ) -> None:
        self._parallel_results[(run_id, step_order, item_index)] = output_data

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        return {
            idx: data
            for (rid, so, idx), data in self._parallel_results.items()
            if rid == run_id and so == step_order
        }

    # ── Pending steps (task queue) ───────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        step.id = step.id or self._auto_id()
        step.created_at = step.created_at or _now()
        self._pending_steps[step.id] = step

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        # Find highest priority pending step
        candidates = [s for s in self._pending_steps.values() if s.status == StepStatus.PENDING]
        if not candidates:
            return None
        candidates.sort(key=lambda s: (-s.priority, s.created_at or _now()))
        claimed = candidates[0]
        claimed.status = StepStatus.RUNNING
        claimed.worker_id = worker_id
        claimed.started_at = _now()
        return claimed

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        pending = self._pending_steps.get(step_id)
        if pending is not None:
            pending.status = StepStatus.COMPLETED
            pending.completed_at = _now()
        await self.save_step_output(output)

    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: Any | None = None,
    ) -> None:
        pending = self._pending_steps.get(step_id)
        if pending is not None:
            pending.status = StepStatus.FAILED
            if retry_at is not None:
                pending.status = StepStatus.PENDING
                pending.next_retry_at = retry_at
                pending.retry_count += 1

    # ── Signals ──────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        signal.id = signal.id or self._auto_id()
        signal.created_at = signal.created_at or _now()
        self._signals.append(signal)

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        for sig in self._signals:
            if (
                sig.workflow_run_id == run_id
                and sig.signal_name == signal_name
                and not sig.consumed
            ):
                sig.consumed = True
                return sig
        return None

    async def register_signal_wait(self, wait: SignalWait) -> None:
        wait.id = wait.id or self._auto_id()
        wait.created_at = wait.created_at or _now()
        self._signal_waits.append(wait)

    # ── Compensation (sagas) ─────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        comp.id = comp.id or self._auto_id()
        comp.created_at = comp.created_at or _now()
        self._compensations.append(comp)

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        comps = [c for c in self._compensations if c.workflow_run_id == run_id]
        return sorted(comps, key=lambda c: c.step_order, reverse=True)

    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        for comp in self._compensations:
            if comp.id == compensation_id:
                comp.status = StepStatus(status)
                if error_message is not None:
                    comp.error_message = error_message
                break

    # ── Scheduling ───────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        schedule.created_at = schedule.created_at or _now()
        self._schedules[schedule.id] = schedule

    async def get_due_schedules(self) -> Sequence[Schedule]:
        now = _now()
        return [
            s
            for s in self._schedules.values()
            if s.enabled and s.next_run_at is not None and s.next_run_at <= now
        ]

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        sched = self._schedules.get(schedule_id)
        if sched is not None:
            sched.last_run_at = last_run_at
            sched.next_run_at = next_run_at

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        return [s for s in self._schedules.values() if s.enabled]

    async def list_all_schedules(self) -> Sequence[Schedule]:
        return list(self._schedules.values())

    # ── Distributed locks ────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        now = _now()
        existing = self._locks.get(lock_name)
        if existing is not None and existing.expires_at is not None and existing.expires_at > now:
            return existing.holder_id == holder_id
        from datetime import timedelta

        self._locks[lock_name] = Lock(
            lock_name=lock_name,
            holder_id=holder_id,
            acquired_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        return True

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        existing = self._locks.get(lock_name)
        if existing is not None and existing.holder_id == holder_id:
            del self._locks[lock_name]
            return True
        return False

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        from datetime import timedelta

        existing = self._locks.get(lock_name)
        if existing is not None and existing.holder_id == holder_id:
            existing.expires_at = _now() + timedelta(seconds=ttl_seconds)
            return True
        return False

    # ── Dead letter queue ────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        entry.id = entry.id or self._auto_id()
        entry.created_at = entry.created_at or _now()
        self._dlq.append(entry)

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        # Filter by namespace via the associated workflow run
        filtered: list[DLQEntry] = []
        for entry in self._dlq:
            run = self._runs.get(entry.workflow_run_id)
            if run is None or run.namespace == namespace:
                filtered.append(entry)
        return filtered[:limit]

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        for entry in self._dlq:
            if entry.id == entry_id:
                return entry
        return None

    async def count_dlq(self, *, namespace: str = "default") -> int:
        return len(await self.list_dlq(namespace=namespace, limit=1_000_000))

    async def remove_from_dlq(self, entry_id: int) -> None:
        self._dlq = [e for e in self._dlq if e.id != entry_id]

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        # Only purge entries belonging to the given namespace
        to_keep: list[DLQEntry] = []
        count = 0
        for entry in self._dlq:
            run = self._runs.get(entry.workflow_run_id)
            if run is not None and run.namespace != namespace:
                to_keep.append(entry)
            else:
                count += 1
        self._dlq = to_keep
        return count

    # ── Workers ──────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        worker.started_at = worker.started_at or _now()
        worker.last_heartbeat = _now()
        self._workers[worker.worker_id] = worker

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.last_heartbeat = _now()
            if current_task is not None:
                worker.current_task = current_task

    async def deregister_worker(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)

    async def list_workers(self) -> Sequence[WorkerInfo]:
        return list(self._workers.values())

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        from datetime import timedelta

        now = _now()
        cutoff = now - timedelta(seconds=stale_threshold_seconds)
        return [
            w
            for w in self._workers.values()
            if w.last_heartbeat is None or w.last_heartbeat < cutoff
        ]

    # ── Task reclamation ───────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        count = 0
        for step in self._pending_steps.values():
            if step.worker_id == worker_id and step.status == StepStatus.RUNNING:
                step.status = StepStatus.PENDING
                step.worker_id = None
                step.started_at = None
                count += 1
        return count

    # ── Concurrency control ───────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        active = sum(
            1
            for r in self._runs.values()
            if r.workflow_name == workflow_name
            and r.namespace == namespace
            and r.status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING)
        )
        return active < max_concurrent

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        self._workflow_defs[(name, version)] = definition_json

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        return sorted(
            [(n, v, j) for (n, v), j in self._workflow_defs.items()],
            key=lambda t: (t[0], t[1]),
        )

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        self._workflow_defs.pop((name, version), None)

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        for name, version, definition_json in definitions:
            self._workflow_defs[(name, version)] = definition_json
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        for name, version in keys:
            self._workflow_defs.pop((name, version), None)
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        self._circuit_states[name] = state_json

    async def load_circuit_state(self, name: str) -> str | None:
        return self._circuit_states.get(name)
