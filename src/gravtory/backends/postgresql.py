# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""PostgreSQL backend using asyncpg — production-grade with connection pooling and SKIP LOCKED."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import asyncpg

from gravtory.backends.base import Backend
from gravtory.backends.schema import CURRENT_SCHEMA_VERSION, POSTGRES_TOKENS, schema_sql
from gravtory.core.types import (
    Compensation,
    DLQEntry,
    PendingStep,
    Schedule,
    ScheduleType,
    Signal,
    SignalWait,
    StepOutput,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
    WorkflowRun,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class PostgreSQLBackend(Backend):
    """PostgreSQL backend using asyncpg with connection pooling."""

    def __init__(
        self,
        dsn: str,
        *,
        min_pool_size: int = 2,
        max_pool_size: int = 10,
        statement_cache_size: int = 100,
        command_timeout: int = 30,
        table_prefix: str = "gravtory_",
    ) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None  # type: ignore[type-arg,unused-ignore]
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._statement_cache_size = statement_cache_size
        self._command_timeout = command_timeout
        self._prefix = table_prefix

    # ── Lifecycle ─────────────────────────────────────────────────

    async def _connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=self._min_pool_size,
            max_size=self._max_pool_size,
            max_inactive_connection_lifetime=300,
            command_timeout=self._command_timeout,
            statement_cache_size=self._statement_cache_size,
        )

    async def initialize(self) -> None:
        if self._pool is None:
            await self._connect()
        pool = self._require_pool()
        async with pool.acquire() as conn, conn.transaction():
            stmts = schema_sql(POSTGRES_TOKENS, self._prefix)
            for stmt in stmts:
                await conn.execute(stmt)
            # Insert schema version if not present
            row = await conn.fetchrow(
                f"SELECT version FROM {self._prefix}schema_version ORDER BY version DESC LIMIT 1"
            )
            if row is None:
                await conn.execute(
                    f"INSERT INTO {self._prefix}schema_version (version) VALUES ($1)",
                    CURRENT_SCHEMA_VERSION,
                )
        # Run pending migrations for existing databases
        from gravtory.backends.migration import SchemaMigrator

        migrator = SchemaMigrator(self)
        await migrator.check_and_migrate()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def health_check(self) -> bool:
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @property
    def _p(self) -> str:
        return self._prefix

    def _require_pool(self) -> asyncpg.Pool:  # type: ignore[type-arg,unused-ignore]
        """Return the connection pool or raise if not initialized."""
        if self._pool is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError(
                "PostgreSQL",
                "Connection pool not initialized. Call initialize() first.",
            )
        return self._pool

    # ── Workflow runs ─────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        pool = self._require_pool()
        status_val = run.status.value if isinstance(run.status, WorkflowStatus) else run.status
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}workflow_runs
                    (id, workflow_name, workflow_version, namespace, status,
                     current_step, input_data, error_message, error_traceback,
                     parent_run_id, deadline_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (id) DO NOTHING""",
                run.id,
                run.workflow_name,
                run.workflow_version,
                run.namespace,
                status_val,
                run.current_step,
                run.input_data,
                run.error_message,
                run.error_traceback,
                run.parent_run_id,
                run.deadline_at,
            )

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT * FROM {self._p}workflow_runs WHERE id = $1", run_id)
        if row is None:
            return None
        return self._row_to_workflow_run(row)

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        pool = self._require_pool()
        status_val = status.value if isinstance(status, WorkflowStatus) else status
        terminal = (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.COMPENSATION_FAILED,
        )
        completed_at = datetime.now(tz=timezone.utc) if status in terminal else None
        async with pool.acquire() as conn:
            await conn.execute(
                f"""UPDATE {self._p}workflow_runs
                    SET status=$2, updated_at=NOW(),
                        error_message=COALESCE($3, error_message),
                        error_traceback=COALESCE($4, error_traceback),
                        output_data=COALESCE($5, output_data),
                        completed_at=COALESCE($6, completed_at)
                    WHERE id=$1""",
                run_id,
                status_val,
                error_message,
                error_traceback,
                output_data,
                completed_at,
            )

    async def claim_workflow_run(
        self,
        run_id: str,
        expected_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        pool = self._require_pool()
        expected_val = (
            expected_status.value
            if isinstance(expected_status, WorkflowStatus)
            else expected_status
        )
        new_val = new_status.value if isinstance(new_status, WorkflowStatus) else new_status
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""UPDATE {self._p}workflow_runs
                    SET status=$2, updated_at=NOW()
                    WHERE id=$1 AND status=$3
                    RETURNING id""",
                run_id,
                new_val,
                expected_val,
            )
        return row is not None

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        pool = self._require_pool()
        conditions = ["namespace = $1"]
        params: list[Any] = [namespace]
        idx = 2
        if status is not None:
            conditions.append(f"status = ${idx}")
            params.append(status.value if isinstance(status, WorkflowStatus) else status)
            idx += 1
        if workflow_name is not None:
            conditions.append(f"workflow_name = ${idx}")
            params.append(workflow_name)
            idx += 1
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        sql = (
            f"SELECT * FROM {self._p}workflow_runs WHERE {where} "
            f"ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx + 1}"
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [self._row_to_workflow_run(r) for r in rows]

    async def list_child_runs(self, parent_run_id: str) -> Sequence[WorkflowRun]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}workflow_runs WHERE parent_run_id = $1 "
                f"ORDER BY created_at DESC",
                parent_run_id,
            )
        return [self._row_to_workflow_run(r) for r in rows]

    # ── Step outputs ──────────────────────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        pool = self._require_pool()
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None
        status_val = output.status.value if isinstance(output.status, StepStatus) else output.status
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"""INSERT INTO {self._p}step_outputs
                        (workflow_run_id, step_order, step_name, output_data,
                         output_type, duration_ms, retry_count, status, error_message)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                        ON CONFLICT (workflow_run_id, step_order) DO UPDATE SET
                            output_data=EXCLUDED.output_data,
                            output_type=EXCLUDED.output_type,
                            duration_ms=EXCLUDED.duration_ms,
                            retry_count=EXCLUDED.retry_count,
                            status=EXCLUDED.status,
                            error_message=EXCLUDED.error_message""",
                output.workflow_run_id,
                output.step_order,
                output.step_name,
                out_data,
                output.output_type,
                output.duration_ms,
                output.retry_count,
                status_val,
                output.error_message,
            )
            await conn.execute(
                f"UPDATE {self._p}workflow_runs SET current_step=$2, updated_at=NOW() WHERE id=$1",
                output.workflow_run_id,
                output.step_order,
            )

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=$1 ORDER BY step_order",
                run_id,
            )
        return [self._row_to_step_output(r) for r in rows]

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=$1 AND step_order=$2",
                run_id,
                step_order,
            )
        if row is None:
            return None
        return self._row_to_step_output(row)

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._p}step_outputs SET output_data=$1 WHERE workflow_run_id=$2 AND step_order=$3",
                output_data,
                run_id,
                step_order,
            )

    # ── Pending steps ─────────────────────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}pending_steps
                    (workflow_run_id, step_order, priority, status, max_retries)
                    VALUES ($1,$2,$3,'pending',$4)""",
                step.workflow_run_id,
                step.step_order,
                step.priority,
                step.max_retries,
            )

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""WITH claimed AS (
                        SELECT id FROM {self._p}pending_steps
                        WHERE status = 'pending' AND scheduled_at <= NOW()
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self._p}pending_steps ps
                    SET status='running', worker_id=$1, started_at=NOW()
                    FROM claimed
                    WHERE ps.id = claimed.id
                    RETURNING ps.*""",
                worker_id,
            )
        if row is None:
            return None
        return self._row_to_pending_step(row)

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._p}pending_steps SET status='completed', completed_at=NOW() WHERE id=$1",
                step_id,
            )
        await self.save_step_output(output)

    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: Any | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if retry_at is not None:
                await conn.execute(
                    f"""UPDATE {self._p}pending_steps
                        SET status='pending', retry_count=retry_count+1,
                            next_retry_at=$2, scheduled_at=$2
                        WHERE id=$1""",
                    step_id,
                    retry_at,
                )
            else:
                await conn.execute(
                    f"UPDATE {self._p}pending_steps SET status='failed' WHERE id=$1",
                    step_id,
                )

    # ── Signals ───────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}signals
                    (workflow_run_id, signal_name, signal_data, consumed)
                    VALUES ($1,$2,$3,FALSE)""",
                signal.workflow_run_id,
                signal.signal_name,
                signal.signal_data,
            )

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""UPDATE {self._p}signals SET consumed=TRUE
                    WHERE id = (
                        SELECT id FROM {self._p}signals
                        WHERE workflow_run_id=$1 AND signal_name=$2 AND consumed=FALSE
                        ORDER BY created_at ASC LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *""",
                run_id,
                signal_name,
            )
        if row is None:
            return None
        return self._row_to_signal(row)

    async def register_signal_wait(self, wait: SignalWait) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}signal_waits
                    (workflow_run_id, signal_name, timeout_at)
                    VALUES ($1,$2,$3)""",
                wait.workflow_run_id,
                wait.signal_name,
                wait.timeout_at,
            )

    # ── Compensation ──────────────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}compensations
                    (workflow_run_id, step_order, handler_name, step_output, status)
                    VALUES ($1,$2,$3,$4,$5)""",
                comp.workflow_run_id,
                comp.step_order,
                comp.handler_name,
                comp.step_output,
                comp.status,
            )

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}compensations WHERE workflow_run_id=$1 ORDER BY step_order DESC",
                run_id,
            )
        return [self._row_to_compensation(r) for r in rows]

    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""UPDATE {self._p}compensations
                    SET status=$2, error_message=COALESCE($3,error_message)
                    WHERE id=$1""",
                compensation_id,
                status,
                error_message,
            )

    # ── Scheduling ────────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}schedules
                    (id, workflow_name, schedule_type, schedule_config, namespace,
                     enabled, next_run_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT(id) DO UPDATE SET
                        schedule_config=EXCLUDED.schedule_config,
                        enabled=EXCLUDED.enabled,
                        next_run_at=EXCLUDED.next_run_at""",
                schedule.id,
                schedule.workflow_name,
                schedule.schedule_type,
                schedule.schedule_config,
                schedule.namespace,
                schedule.enabled,
                schedule.next_run_at,
            )

    async def get_due_schedules(self) -> Sequence[Schedule]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {self._p}schedules
                    WHERE enabled=TRUE AND next_run_at IS NOT NULL AND next_run_at <= NOW()"""
            )
        return [self._row_to_schedule(r) for r in rows]

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._p}schedules SET last_run_at=$2, next_run_at=$3 WHERE id=$1",
                schedule_id,
                last_run_at,
                next_run_at,
            )

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}schedules WHERE enabled=true",
            )
        return [self._row_to_schedule(r) for r in rows]

    async def list_all_schedules(self) -> Sequence[Schedule]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self._p}schedules")
        return [self._row_to_schedule(r) for r in rows]

    # ── Locks ─────────────────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                f"""INSERT INTO {self._p}locks (lock_name, holder_id, expires_at)
                    VALUES ($1, $2, NOW() + make_interval(secs => $3))
                    ON CONFLICT (lock_name) DO UPDATE
                    SET holder_id=$2,
                        acquired_at=NOW(),
                        expires_at=NOW() + make_interval(secs => $3)
                    WHERE {self._p}locks.expires_at < NOW()
                       OR {self._p}locks.holder_id = $2""",
                lock_name,
                holder_id,
                float(ttl_seconds),
            )
        # asyncpg returns command tag like "INSERT 0 1" (success) or "INSERT 0 0" (conflict blocked)
        return result is not None and result.endswith(" 1")

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                f"DELETE FROM {self._p}locks WHERE lock_name=$1 AND holder_id=$2",
                lock_name,
                holder_id,
            )
        return bool(result.endswith("1"))

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                f"""UPDATE {self._p}locks
                    SET expires_at=NOW() + make_interval(secs => $3)
                    WHERE lock_name=$1 AND holder_id=$2""",
                lock_name,
                holder_id,
                float(ttl_seconds),
            )
        return bool(result.endswith("1"))

    # ── DLQ ───────────────────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}dlq
                    (workflow_run_id, step_order, error_message, error_traceback, retry_count)
                    VALUES ($1,$2,$3,$4,$5)""",
                entry.workflow_run_id,
                entry.step_order,
                entry.error_message,
                entry.error_traceback,
                entry.retry_count,
            )

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}dlq ORDER BY created_at DESC LIMIT $1",
                limit,
            )
        return [self._row_to_dlq_entry(r) for r in rows]

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT * FROM {self._p}dlq WHERE id=$1", entry_id)
        return self._row_to_dlq_entry(row) if row else None

    async def count_dlq(self, *, namespace: str = "default") -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT COUNT(*) AS cnt FROM {self._p}dlq")
        return int(row["cnt"]) if row else 0

    async def remove_from_dlq(self, entry_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {self._p}dlq WHERE id=$1", entry_id)

    # ── Workers ───────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}workers (worker_id, node_id, status)
                    VALUES ($1,$2,$3)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        node_id=EXCLUDED.node_id,
                        last_heartbeat=NOW()""",
                worker.worker_id,
                worker.node_id,
                worker.status,
            )

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if current_task is not None:
                await conn.execute(
                    f"UPDATE {self._p}workers SET last_heartbeat=NOW(), current_task=$2 WHERE worker_id=$1",
                    worker_id,
                    current_task,
                )
            else:
                await conn.execute(
                    f"UPDATE {self._p}workers SET last_heartbeat=NOW() WHERE worker_id=$1",
                    worker_id,
                )

    async def deregister_worker(self, worker_id: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {self._p}workers WHERE worker_id=$1", worker_id)

    async def list_workers(self) -> Sequence[WorkerInfo]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self._p}workers")
        return [self._row_to_worker(r) for r in rows]

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {self._p}workers
                    WHERE last_heartbeat < NOW() - INTERVAL '{stale_threshold_seconds} seconds'
                       OR last_heartbeat IS NULL""",
            )
        return [self._row_to_worker(r) for r in rows]

    # ── Workflow run extras ────────────────────────────────────────

    async def count_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
    ) -> int:
        pool = self._require_pool()
        query = f"SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs WHERE namespace=$1"
        params: list[object] = [namespace]
        idx = 2
        if status is not None:
            query += f" AND status=${idx}"
            params.append(status.value)
            idx += 1
        if workflow_name is not None:
            query += f" AND workflow_name=${idx}"
            params.append(workflow_name)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
        return int(row["cnt"]) if row else 0

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._p}workflow_runs WHERE status IN ('running','pending')",
            )
        return [self._row_to_workflow_run(r) for r in rows]

    # ── Parallel step results ──────────────────────────────────────

    async def checkpoint_parallel_item(
        self,
        run_id: str,
        step_order: int,
        item_index: int,
        output_data: bytes,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._p}parallel_results (workflow_run_id, step_order, item_index, output_data)
                    VALUES ($1,$2,$3,$4)
                    ON CONFLICT (workflow_run_id, step_order, item_index) DO UPDATE SET output_data=$4""",
                run_id,
                step_order,
                item_index,
                output_data,
            )

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT item_index, output_data FROM {self._p}parallel_results WHERE workflow_run_id=$1 AND step_order=$2",
                run_id,
                step_order,
            )
        return {int(r["item_index"]): r["output_data"] for r in rows}

    # ── DLQ extras ─────────────────────────────────────────────────

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(f"DELETE FROM {self._p}dlq")
        return int(result.split()[-1]) if result else 0

    # ── Task reclamation ─────────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                f"""UPDATE {self._p}pending_steps
                    SET status='pending', worker_id=NULL, started_at=NULL
                    WHERE worker_id=$1 AND status='running'""",
                worker_id,
            )
        return int(result.split()[-1]) if result else 0

    # ── Concurrency control ─────────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs
                    WHERE workflow_name=$1 AND namespace=$2
                      AND status IN ('running','pending')""",
                workflow_name,
                namespace,
            )
        count = row["cnt"] if row else 0
        return int(count) < max_concurrent

    # ── Row mappers ───────────────────────────────────────────────

    @staticmethod
    def _row_to_workflow_run(row: asyncpg.Record) -> WorkflowRun:
        return WorkflowRun(
            id=row["id"],
            workflow_name=row["workflow_name"],
            workflow_version=row["workflow_version"],
            namespace=row["namespace"],
            status=WorkflowStatus(row["status"]),
            current_step=row["current_step"],
            input_data=row["input_data"],
            error_message=row["error_message"],
            error_traceback=row["error_traceback"],
            parent_run_id=row["parent_run_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            deadline_at=row["deadline_at"],
        )

    @staticmethod
    def _row_to_step_output(row: asyncpg.Record) -> StepOutput:
        return StepOutput(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            step_name=row["step_name"],
            output_data=row["output_data"],
            output_type=row["output_type"],
            duration_ms=row["duration_ms"],
            retry_count=row["retry_count"],
            status=StepStatus(row["status"]),
            error_message=row["error_message"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_pending_step(row: asyncpg.Record) -> PendingStep:
        status_raw = row["status"]
        try:
            step_status = StepStatus(status_raw)
        except ValueError:
            step_status = StepStatus.PENDING
        return PendingStep(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            priority=row["priority"],
            status=step_status,
            worker_id=row["worker_id"],
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
        )

    @staticmethod
    def _row_to_signal(row: asyncpg.Record) -> Signal:
        return Signal(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            signal_name=row["signal_name"],
            signal_data=row["signal_data"],
            consumed=row["consumed"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_compensation(row: asyncpg.Record) -> Compensation:
        status_raw = row["status"]
        try:
            comp_status = StepStatus(status_raw)
        except ValueError:
            comp_status = StepStatus.PENDING
        return Compensation(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            handler_name=row["handler_name"],
            step_output=row["step_output"],
            status=comp_status,
        )

    @staticmethod
    def _row_to_schedule(row: asyncpg.Record) -> Schedule:
        stype_raw = row["schedule_type"]
        try:
            stype = ScheduleType(stype_raw)
        except ValueError:
            stype = ScheduleType.CRON
        return Schedule(
            id=row["id"],
            workflow_name=row["workflow_name"],
            schedule_type=stype,
            schedule_config=row["schedule_config"],
            namespace=row["namespace"],
            enabled=row["enabled"],
            last_run_at=row["last_run_at"],
            next_run_at=row["next_run_at"],
        )

    @staticmethod
    def _row_to_dlq_entry(row: asyncpg.Record) -> DLQEntry:
        return DLQEntry(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            error_message=row["error_message"],
            error_traceback=row["error_traceback"],
            retry_count=row["retry_count"],
        )

    @staticmethod
    def _row_to_worker(row: asyncpg.Record) -> WorkerInfo:
        wstatus_raw = row["status"]
        try:
            wstatus = WorkerStatus(wstatus_raw)
        except ValueError:
            wstatus = WorkerStatus.ACTIVE
        return WorkerInfo(
            worker_id=row["worker_id"],
            node_id=row["node_id"],
            status=wstatus,
        )

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._prefix}workflow_definitions (name, version, definition_json)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (name, version) DO UPDATE SET definition_json=EXCLUDED.definition_json""",
                name,
                version,
                definition_json,
            )

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT name, version, definition_json FROM {self._prefix}workflow_definitions ORDER BY name, version"
            )
        return [(row["name"], row["version"], row["definition_json"]) for row in rows]

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {self._prefix}workflow_definitions WHERE name=$1 AND version=$2",
                name,
                version,
            )

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        if not definitions:
            return 0
        pool = self._require_pool()
        async with pool.acquire() as conn, conn.transaction():
            await conn.executemany(
                f"""INSERT INTO {self._prefix}workflow_definitions (name, version, definition_json)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (name, version) DO UPDATE SET definition_json=EXCLUDED.definition_json""",
                [(n, v, d) for n, v, d in definitions],
            )
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        if not keys:
            return 0
        pool = self._require_pool()
        async with pool.acquire() as conn, conn.transaction():
            await conn.executemany(
                f"DELETE FROM {self._prefix}workflow_definitions WHERE name=$1 AND version=$2",
                [(n, v) for n, v in keys],
            )
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._prefix}circuit_breakers (name, state_json)
                    VALUES ($1, $2)
                    ON CONFLICT (name) DO UPDATE SET state_json=EXCLUDED.state_json,
                        updated_at=NOW()""",
                name,
                state_json,
            )

    async def load_circuit_state(self, name: str) -> str | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT state_json FROM {self._prefix}circuit_breakers WHERE name=$1",
                name,
            )
        if row is not None:
            return str(row["state_json"])
        return None
