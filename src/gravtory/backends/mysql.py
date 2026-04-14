# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""MySQL backend using aiomysql — full implementation with connection pooling and SKIP LOCKED."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

try:
    import aiomysql
except ImportError as _exc:
    raise ImportError(
        "MySQL backend requires aiomysql. Install with: pip install gravtory[mysql]"
    ) from _exc

from gravtory.backends.base import Backend
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


def _parse_mysql_dsn(dsn: str) -> dict[str, Any]:
    """Parse a mysql:// DSN into aiomysql.create_pool kwargs."""
    parsed = urlparse(dsn)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": parsed.username or "root",
        "password": parsed.password or "",
        "db": (parsed.path or "/gravtory").lstrip("/"),
    }


class MySQLBackend(Backend):
    """MySQL backend using aiomysql with connection pooling."""

    def __init__(
        self,
        dsn: str,
        *,
        min_pool_size: int = 2,
        max_pool_size: int = 10,
        table_prefix: str = "gravtory_",
    ) -> None:
        self._dsn = dsn
        self._pool: aiomysql.Pool | None = None
        self._min_size = min_pool_size
        self._max_size = max_pool_size
        self._prefix = table_prefix

    # ── Lifecycle ─────────────────────────────────────────────────

    async def _connect(self) -> None:
        params = _parse_mysql_dsn(self._dsn)
        self._pool = await aiomysql.create_pool(
            host=params["host"],
            port=params["port"],
            user=params["user"],
            password=params["password"],
            db=params["db"],
            minsize=self._min_size,
            maxsize=self._max_size,
            autocommit=True,
            charset="utf8mb4",
        )

    async def initialize(self) -> None:
        if self._pool is None:
            await self._connect()
        pool = self._ensure_connected()
        p = self._prefix
        stmts = _mysql_schema(p)
        async with pool.acquire() as conn:
            cur: aiomysql.Cursor = await conn.cursor()
            for stmt in stmts:
                await cur.execute(stmt)
            await cur.close()

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def health_check(self) -> bool:
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute("SELECT 1")
                await cur.close()
            return True
        except Exception:
            return False

    def _ensure_connected(self) -> aiomysql.Pool:
        """Return the active pool or raise BackendConnectionError."""
        if self._pool is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("MySQL", "Not connected. Call initialize() first.")
        return self._pool

    @property
    def _p(self) -> str:
        return self._prefix

    @property
    def _pl(self) -> aiomysql.Pool:
        """Narrowed accessor for the pool (raises if not connected)."""
        return self._ensure_connected()

    # ── helpers ────────────────────────────────────────────────────

    async def _execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        """Execute a statement and return rowcount."""
        async with self._pl.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, args)
            rc: int = cur.rowcount
            await cur.close()
            return rc

    async def _fetchone(self, sql: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        async with self._pl.acquire() as conn:
            cur = await conn.cursor(aiomysql.DictCursor)
            await cur.execute(sql, args)
            row: dict[str, Any] | None = await cur.fetchone()
            await cur.close()
            return row

    async def _fetchall(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self._pl.acquire() as conn:
            cur = await conn.cursor(aiomysql.DictCursor)
            await cur.execute(sql, args)
            rows: list[dict[str, Any]] = list(await cur.fetchall())
            await cur.close()
            return rows

    # ── Workflow runs ─────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        status_val = run.status.value if isinstance(run.status, WorkflowStatus) else run.status
        await self._execute(
            f"""INSERT IGNORE INTO {self._p}workflow_runs
                (id, workflow_name, workflow_version, namespace, status,
                 current_step, input_data, error_message, error_traceback,
                 parent_run_id, deadline_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
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
            ),
        )

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        row = await self._fetchone(f"SELECT * FROM {self._p}workflow_runs WHERE id = %s", (run_id,))
        return self._row_to_workflow_run(row) if row else None

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        status_val = status.value if isinstance(status, WorkflowStatus) else status
        terminal = (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.COMPENSATION_FAILED,
        )
        completed_at = datetime.now(tz=timezone.utc) if status in terminal else None
        await self._execute(
            f"""UPDATE {self._p}workflow_runs
                SET status=%s, updated_at=CURRENT_TIMESTAMP(6),
                    error_message=COALESCE(%s, error_message),
                    error_traceback=COALESCE(%s, error_traceback),
                    output_data=COALESCE(%s, output_data),
                    completed_at=COALESCE(%s, completed_at)
                WHERE id=%s""",
            (status_val, error_message, error_traceback, output_data, completed_at, run_id),
        )

    async def claim_workflow_run(
        self,
        run_id: str,
        expected_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        expected_val = (
            expected_status.value
            if isinstance(expected_status, WorkflowStatus)
            else expected_status
        )
        new_val = new_status.value if isinstance(new_status, WorkflowStatus) else new_status
        rc = await self._execute(
            f"""UPDATE {self._p}workflow_runs
                SET status=%s, updated_at=CURRENT_TIMESTAMP(6)
                WHERE id=%s AND status=%s""",
            (new_val, run_id, expected_val),
        )
        return rc > 0

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        conditions = ["namespace = %s"]
        params: list[Any] = [namespace]
        if status is not None:
            conditions.append("status = %s")
            params.append(status.value if isinstance(status, WorkflowStatus) else status)
        if workflow_name is not None:
            conditions.append("workflow_name = %s")
            params.append(workflow_name)
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        rows = await self._fetchall(
            f"SELECT * FROM {self._p}workflow_runs WHERE {where} "
            f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
            tuple(params),
        )
        return [self._row_to_workflow_run(r) for r in rows]

    # ── Step outputs ──────────────────────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None
        status_val = output.status.value if isinstance(output.status, StepStatus) else output.status
        async with self._pl.acquire() as conn:
            cur = await conn.cursor()
            await conn.begin()
            await cur.execute(
                f"""INSERT INTO {self._p}step_outputs
                    (workflow_run_id, step_order, step_name, output_data,
                     output_type, duration_ms, retry_count, status, error_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        output_data=VALUES(output_data),
                        output_type=VALUES(output_type),
                        duration_ms=VALUES(duration_ms),
                        retry_count=VALUES(retry_count),
                        status=VALUES(status),
                        error_message=VALUES(error_message)""",
                (
                    output.workflow_run_id,
                    output.step_order,
                    output.step_name,
                    out_data,
                    output.output_type,
                    output.duration_ms,
                    output.retry_count,
                    status_val,
                    output.error_message,
                ),
            )
            await cur.execute(
                f"UPDATE {self._p}workflow_runs SET current_step=%s, updated_at=CURRENT_TIMESTAMP(6) WHERE id=%s",
                (output.step_order, output.workflow_run_id),
            )
            await conn.commit()
            await cur.close()

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        rows = await self._fetchall(
            f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=%s ORDER BY step_order",
            (run_id,),
        )
        return [self._row_to_step_output(r) for r in rows]

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        row = await self._fetchone(
            f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=%s AND step_order=%s",
            (run_id, step_order),
        )
        return self._row_to_step_output(row) if row else None

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        await self._execute(
            f"UPDATE {self._p}step_outputs SET output_data=%s WHERE workflow_run_id=%s AND step_order=%s",
            (output_data, run_id, step_order),
        )

    # ── Pending steps ─────────────────────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}pending_steps
                (workflow_run_id, step_order, priority, status, max_retries)
                VALUES (%s,%s,%s,'pending',%s)""",
            (step.workflow_run_id, step.step_order, step.priority, step.max_retries),
        )

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        async with self._pl.acquire() as conn:
            cur = await conn.cursor(aiomysql.DictCursor)
            await conn.begin()
            # MySQL requires subquery wrapping for UPDATE with LIMIT + FOR UPDATE SKIP LOCKED
            await cur.execute(
                f"""SELECT id FROM {self._p}pending_steps
                    WHERE status = 'pending' AND scheduled_at <= CURRENT_TIMESTAMP(6)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED""",
            )
            target = await cur.fetchone()
            if target is None:
                await conn.commit()
                await cur.close()
                return None
            target_id = target["id"]
            await cur.execute(
                f"""UPDATE {self._p}pending_steps
                    SET status='running', worker_id=%s, started_at=CURRENT_TIMESTAMP(6)
                    WHERE id=%s""",
                (worker_id, target_id),
            )
            await cur.execute(f"SELECT * FROM {self._p}pending_steps WHERE id=%s", (target_id,))
            row = await cur.fetchone()
            await conn.commit()
            await cur.close()
        return self._row_to_pending_step(row) if row else None

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        await self._execute(
            f"UPDATE {self._p}pending_steps SET status='completed', completed_at=CURRENT_TIMESTAMP(6) WHERE id=%s",
            (step_id,),
        )
        await self.save_step_output(output)

    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: Any | None = None,
    ) -> None:
        if retry_at is not None:
            await self._execute(
                f"""UPDATE {self._p}pending_steps
                    SET status='pending', retry_count=retry_count+1,
                        next_retry_at=%s, scheduled_at=%s
                    WHERE id=%s""",
                (retry_at, retry_at, step_id),
            )
        else:
            await self._execute(
                f"UPDATE {self._p}pending_steps SET status='failed' WHERE id=%s",
                (step_id,),
            )

    # ── Signals ───────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}signals
                (workflow_run_id, signal_name, signal_data, consumed)
                VALUES (%s,%s,%s,0)""",
            (signal.workflow_run_id, signal.signal_name, signal.signal_data),
        )

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        async with self._pl.acquire() as conn:
            cur = await conn.cursor(aiomysql.DictCursor)
            await conn.begin()
            await cur.execute(
                f"""SELECT id FROM {self._p}signals
                    WHERE workflow_run_id=%s AND signal_name=%s AND consumed=0
                    ORDER BY created_at ASC LIMIT 1
                    FOR UPDATE SKIP LOCKED""",
                (run_id, signal_name),
            )
            target = await cur.fetchone()
            if target is None:
                await conn.commit()
                await cur.close()
                return None
            target_id = target["id"]
            await cur.execute(f"UPDATE {self._p}signals SET consumed=1 WHERE id=%s", (target_id,))
            await cur.execute(f"SELECT * FROM {self._p}signals WHERE id=%s", (target_id,))
            row = await cur.fetchone()
            await conn.commit()
            await cur.close()
        return self._row_to_signal(row) if row else None

    async def register_signal_wait(self, wait: SignalWait) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}signal_waits
                (workflow_run_id, signal_name, timeout_at) VALUES (%s,%s,%s)""",
            (wait.workflow_run_id, wait.signal_name, wait.timeout_at),
        )

    # ── Compensation ──────────────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}compensations
                (workflow_run_id, step_order, handler_name, step_output, status)
                VALUES (%s,%s,%s,%s,%s)""",
            (
                comp.workflow_run_id,
                comp.step_order,
                comp.handler_name,
                comp.step_output,
                comp.status,
            ),
        )

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        rows = await self._fetchall(
            f"SELECT * FROM {self._p}compensations WHERE workflow_run_id=%s ORDER BY step_order DESC",
            (run_id,),
        )
        return [self._row_to_compensation(r) for r in rows]

    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        await self._execute(
            f"""UPDATE {self._p}compensations
                SET status=%s, error_message=COALESCE(%s,error_message)
                WHERE id=%s""",
            (status, error_message, compensation_id),
        )

    # ── Scheduling ────────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}schedules
                (id, workflow_name, schedule_type, schedule_config, namespace,
                 enabled, next_run_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    schedule_config=VALUES(schedule_config),
                    enabled=VALUES(enabled),
                    next_run_at=VALUES(next_run_at)""",
            (
                schedule.id,
                schedule.workflow_name,
                schedule.schedule_type,
                schedule.schedule_config,
                schedule.namespace,
                schedule.enabled,
                schedule.next_run_at,
            ),
        )

    async def get_due_schedules(self) -> Sequence[Schedule]:
        rows = await self._fetchall(
            f"""SELECT * FROM {self._p}schedules
                WHERE enabled=1 AND next_run_at IS NOT NULL
                  AND next_run_at <= CURRENT_TIMESTAMP(6)"""
        )
        return [self._row_to_schedule(r) for r in rows]

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        await self._execute(
            f"UPDATE {self._p}schedules SET last_run_at=%s, next_run_at=%s WHERE id=%s",
            (last_run_at, next_run_at, schedule_id),
        )

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        rows = await self._fetchall(f"SELECT * FROM {self._p}schedules WHERE enabled=1")
        return [self._row_to_schedule(r) for r in rows]

    async def list_all_schedules(self) -> Sequence[Schedule]:
        rows = await self._fetchall(f"SELECT * FROM {self._p}schedules")
        return [self._row_to_schedule(r) for r in rows]

    # ── Locks ─────────────────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        await self._execute(
            f"""INSERT INTO {self._p}locks (lock_name, holder_id, expires_at)
                VALUES (%s, %s, DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %s SECOND))
                ON DUPLICATE KEY UPDATE
                  holder_id = IF(expires_at < CURRENT_TIMESTAMP(6) OR holder_id = VALUES(holder_id),
                                 VALUES(holder_id), holder_id),
                  acquired_at = IF(expires_at < CURRENT_TIMESTAMP(6) OR holder_id = VALUES(holder_id),
                                   CURRENT_TIMESTAMP(6), acquired_at),
                  expires_at = IF(expires_at < CURRENT_TIMESTAMP(6) OR holder_id = VALUES(holder_id),
                                  VALUES(expires_at), expires_at)""",
            (lock_name, holder_id, ttl_seconds),
        )
        # After upsert, verify holder
        row = await self._fetchone(
            f"SELECT holder_id FROM {self._p}locks WHERE lock_name=%s", (lock_name,)
        )
        return row is not None and row["holder_id"] == holder_id

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        rc = await self._execute(
            f"DELETE FROM {self._p}locks WHERE lock_name=%s AND holder_id=%s",
            (lock_name, holder_id),
        )
        return rc > 0

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        rc = await self._execute(
            f"""UPDATE {self._p}locks
                SET expires_at=DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %s SECOND)
                WHERE lock_name=%s AND holder_id=%s""",
            (ttl_seconds, lock_name, holder_id),
        )
        return rc > 0

    # ── DLQ ───────────────────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}dlq
                (workflow_run_id, step_order, error_message, error_traceback, retry_count)
                VALUES (%s,%s,%s,%s,%s)""",
            (
                entry.workflow_run_id,
                entry.step_order,
                entry.error_message,
                entry.error_traceback,
                entry.retry_count,
            ),
        )

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        rows = await self._fetchall(
            f"SELECT * FROM {self._p}dlq ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return [self._row_to_dlq_entry(r) for r in rows]

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        row = await self._fetchone(f"SELECT * FROM {self._p}dlq WHERE id=%s", (entry_id,))
        return self._row_to_dlq_entry(row) if row else None

    async def count_dlq(self, *, namespace: str = "default") -> int:
        row = await self._fetchone(f"SELECT COUNT(*) AS cnt FROM {self._p}dlq")
        return int(row["cnt"]) if row else 0

    async def remove_from_dlq(self, entry_id: int) -> None:
        await self._execute(f"DELETE FROM {self._p}dlq WHERE id=%s", (entry_id,))

    # ── Workers ───────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}workers (worker_id, node_id, status)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    node_id=VALUES(node_id),
                    last_heartbeat=CURRENT_TIMESTAMP(6)""",
            (worker.worker_id, worker.node_id, worker.status),
        )

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        if current_task is not None:
            await self._execute(
                f"UPDATE {self._p}workers SET last_heartbeat=CURRENT_TIMESTAMP(6), current_task=%s WHERE worker_id=%s",
                (current_task, worker_id),
            )
        else:
            await self._execute(
                f"UPDATE {self._p}workers SET last_heartbeat=CURRENT_TIMESTAMP(6) WHERE worker_id=%s",
                (worker_id,),
            )

    async def deregister_worker(self, worker_id: str) -> None:
        await self._execute(f"DELETE FROM {self._p}workers WHERE worker_id=%s", (worker_id,))

    async def list_workers(self) -> Sequence[WorkerInfo]:
        rows = await self._fetchall(f"SELECT * FROM {self._p}workers")
        return [self._row_to_worker(r) for r in rows]

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        rows = await self._fetchall(
            f"""SELECT * FROM {self._p}workers
                WHERE last_heartbeat < DATE_SUB(NOW(6), INTERVAL %s SECOND)
                   OR last_heartbeat IS NULL""",
            (stale_threshold_seconds,),
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
        query = f"SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs WHERE namespace=%s"
        params: list[object] = [namespace]
        if status is not None:
            query += " AND status=%s"
            params.append(status.value)
        if workflow_name is not None:
            query += " AND workflow_name=%s"
            params.append(workflow_name)
        row = await self._fetchone(query, tuple(params))
        return int(row["cnt"]) if row else 0

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        rows = await self._fetchall(
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
        await self._execute(
            f"""INSERT INTO {self._p}parallel_results (workflow_run_id, step_order, item_index, output_data)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE output_data=VALUES(output_data)""",
            (run_id, step_order, item_index, output_data),
        )

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        rows = await self._fetchall(
            f"SELECT item_index, output_data FROM {self._p}parallel_results WHERE workflow_run_id=%s AND step_order=%s",
            (run_id, step_order),
        )
        return {int(r["item_index"]): r["output_data"] for r in rows}

    # ── DLQ extras ─────────────────────────────────────────────────

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        row = await self._fetchone(f"SELECT COUNT(*) AS cnt FROM {self._p}dlq")
        count = int(row["cnt"]) if row else 0
        await self._execute(f"DELETE FROM {self._p}dlq")
        return count

    # ── Task reclamation ───────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        return await self._execute(
            f"""UPDATE {self._p}pending_steps
                SET status='pending', worker_id=NULL, started_at=NULL
                WHERE worker_id=%s AND status='running'""",
            (worker_id,),
        )

    # ── Concurrency control ───────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        row = await self._fetchone(
            f"""SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs
                WHERE workflow_name=%s AND namespace=%s
                  AND status IN ('running','pending')""",
            (workflow_name, namespace),
        )
        count = row["cnt"] if row else 0
        return int(count) < max_concurrent

    # ── Row mappers ───────────────────────────────────────────────

    @staticmethod
    def _row_to_workflow_run(row: dict[str, Any]) -> WorkflowRun:
        return WorkflowRun(
            id=row["id"],
            workflow_name=row["workflow_name"],
            workflow_version=row["workflow_version"],
            namespace=row["namespace"],
            status=WorkflowStatus(row["status"]),
            current_step=row["current_step"],
            input_data=row.get("input_data"),
            error_message=row.get("error_message"),
            error_traceback=row.get("error_traceback"),
            parent_run_id=row.get("parent_run_id"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            completed_at=row.get("completed_at"),
            deadline_at=row.get("deadline_at"),
        )

    @staticmethod
    def _row_to_step_output(row: dict[str, Any]) -> StepOutput:
        return StepOutput(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            step_name=row["step_name"],
            output_data=row.get("output_data"),
            output_type=row.get("output_type"),
            duration_ms=row.get("duration_ms"),
            retry_count=row.get("retry_count", 0),
            status=StepStatus(row["status"]),
            error_message=row.get("error_message"),
            created_at=row.get("created_at"),
        )

    @staticmethod
    def _row_to_pending_step(row: dict[str, Any]) -> PendingStep:
        try:
            step_status = StepStatus(row["status"])
        except ValueError:
            step_status = StepStatus.PENDING
        return PendingStep(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            priority=row["priority"],
            status=step_status,
            worker_id=row.get("worker_id"),
            retry_count=row.get("retry_count", 0),
            max_retries=row.get("max_retries", 0),
        )

    @staticmethod
    def _row_to_signal(row: dict[str, Any]) -> Signal:
        return Signal(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            signal_name=row["signal_name"],
            signal_data=row.get("signal_data"),
            consumed=bool(row.get("consumed", False)),
            created_at=row.get("created_at"),
        )

    @staticmethod
    def _row_to_compensation(row: dict[str, Any]) -> Compensation:
        try:
            comp_status = StepStatus(row["status"])
        except ValueError:
            comp_status = StepStatus.PENDING
        return Compensation(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            handler_name=row["handler_name"],
            step_output=row.get("step_output"),
            status=comp_status,
        )

    @staticmethod
    def _row_to_schedule(row: dict[str, Any]) -> Schedule:
        try:
            stype = ScheduleType(row["schedule_type"])
        except ValueError:
            stype = ScheduleType.CRON
        return Schedule(
            id=row["id"],
            workflow_name=row["workflow_name"],
            schedule_type=stype,
            schedule_config=row["schedule_config"],
            namespace=row.get("namespace", "default"),
            enabled=bool(row.get("enabled", True)),
            last_run_at=row.get("last_run_at"),
            next_run_at=row.get("next_run_at"),
        )

    @staticmethod
    def _row_to_dlq_entry(row: dict[str, Any]) -> DLQEntry:
        return DLQEntry(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row.get("step_order", 0),
            error_message=row.get("error_message"),
            error_traceback=row.get("error_traceback"),
            retry_count=row.get("retry_count", 0),
        )

    @staticmethod
    def _row_to_worker(row: dict[str, Any]) -> WorkerInfo:
        try:
            wstatus = WorkerStatus(row["status"])
        except ValueError:
            wstatus = WorkerStatus.ACTIVE
        return WorkerInfo(
            worker_id=row["worker_id"],
            node_id=row["node_id"],
            status=wstatus,
        )

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}workflow_definitions (name, version, definition_json)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE definition_json=VALUES(definition_json)""",
            (name, version, definition_json),
        )

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        rows = await self._fetchall(
            f"SELECT name, version, definition_json FROM {self._p}workflow_definitions ORDER BY name, version"
        )
        return [(row["name"], int(row["version"]), row["definition_json"]) for row in rows]

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        await self._execute(
            f"DELETE FROM {self._p}workflow_definitions WHERE name=%s AND version=%s",
            (name, version),
        )

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        if not definitions:
            return 0
        async with self._pl.acquire() as conn:
            cur = await conn.cursor()
            await cur.executemany(
                f"""INSERT INTO {self._p}workflow_definitions (name, version, definition_json)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE definition_json=VALUES(definition_json)""",
                [(n, v, d) for n, v, d in definitions],
            )
            await cur.close()
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        if not keys:
            return 0
        async with self._pl.acquire() as conn:
            cur = await conn.cursor()
            await cur.executemany(
                f"DELETE FROM {self._p}workflow_definitions WHERE name=%s AND version=%s",
                [(n, v) for n, v in keys],
            )
            await cur.close()
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        await self._execute(
            f"""INSERT INTO {self._p}circuit_breakers (name, state_json)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE state_json=VALUES(state_json),
                    updated_at=NOW(6)""",
            (name, state_json),
        )

    async def load_circuit_state(self, name: str) -> str | None:
        row = await self._fetchone(
            f"SELECT state_json FROM {self._p}circuit_breakers WHERE name=%s",
            (name,),
        )
        if row is not None:
            return str(row["state_json"])
        return None


# ---------------------------------------------------------------------------
# MySQL DDL
# ---------------------------------------------------------------------------


def _mysql_schema(prefix: str) -> list[str]:
    """Return MySQL CREATE TABLE / CREATE INDEX statements."""
    p = prefix
    stmts: list[str] = []

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workflow_runs (
    id                  VARCHAR(255) PRIMARY KEY,
    workflow_name       VARCHAR(255) NOT NULL,
    workflow_version    INT NOT NULL DEFAULT 1,
    namespace           VARCHAR(255) NOT NULL DEFAULT 'default',
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',
    current_step        INT,
    input_data          LONGBLOB,
    output_data         LONGBLOB,
    error_message       TEXT,
    error_traceback     TEXT,
    parent_run_id       VARCHAR(255),
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    completed_at        DATETIME(6),
    deadline_at         DATETIME(6),
    INDEX idx_{p}wr_status (status),
    INDEX idx_{p}wr_name (workflow_name),
    INDEX idx_{p}wr_namespace (namespace),
    INDEX idx_{p}wr_created (created_at),
    INDEX idx_{p}wr_parent (parent_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}step_outputs (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    step_order          INT NOT NULL,
    step_name           VARCHAR(255) NOT NULL,
    output_data         LONGBLOB,
    output_type         VARCHAR(255),
    duration_ms         INT,
    retry_count         INT NOT NULL DEFAULT 0,
    status              VARCHAR(50) NOT NULL DEFAULT 'completed',
    error_message       TEXT,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_{p}so_run_step (workflow_run_id, step_order),
    INDEX idx_{p}so_run (workflow_run_id),
    FOREIGN KEY (workflow_run_id) REFERENCES {p}workflow_runs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}parallel_results (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    step_order          INT NOT NULL,
    item_index          INT NOT NULL,
    output_data         LONGBLOB NOT NULL,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_{p}pr_run_step_item (workflow_run_id, step_order, item_index),
    INDEX idx_{p}pr_run_step (workflow_run_id, step_order),
    FOREIGN KEY (workflow_run_id) REFERENCES {p}workflow_runs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}pending_steps (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    step_order          INT NOT NULL,
    priority            INT NOT NULL DEFAULT 0,
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',
    worker_id           VARCHAR(255),
    scheduled_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    started_at          DATETIME(6),
    completed_at        DATETIME(6),
    retry_count         INT NOT NULL DEFAULT 0,
    max_retries         INT NOT NULL DEFAULT 0,
    next_retry_at       DATETIME(6),
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_{p}ps_status_sched (status, scheduled_at),
    INDEX idx_{p}ps_priority (priority DESC, created_at ASC),
    INDEX idx_{p}ps_worker (worker_id),
    FOREIGN KEY (workflow_run_id) REFERENCES {p}workflow_runs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}signals (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    signal_name         VARCHAR(255) NOT NULL,
    signal_data         LONGBLOB,
    consumed            TINYINT(1) NOT NULL DEFAULT 0,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_{p}sig_run_name (workflow_run_id, signal_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}signal_waits (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    signal_name         VARCHAR(255) NOT NULL,
    timeout_at          DATETIME(6),
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_{p}sw_run (workflow_run_id, signal_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}compensations (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    step_order          INT NOT NULL,
    handler_name        VARCHAR(255) NOT NULL,
    step_output         LONGBLOB,
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',
    error_message       TEXT,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_{p}comp_run (workflow_run_id),
    FOREIGN KEY (workflow_run_id) REFERENCES {p}workflow_runs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}schedules (
    id                  VARCHAR(255) PRIMARY KEY,
    workflow_name       VARCHAR(255) NOT NULL,
    schedule_type       VARCHAR(50) NOT NULL,
    schedule_config     VARCHAR(255) NOT NULL,
    namespace           VARCHAR(255) NOT NULL DEFAULT 'default',
    enabled             TINYINT(1) NOT NULL DEFAULT 1,
    last_run_at         DATETIME(6),
    next_run_at         DATETIME(6),
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_{p}sched_next (enabled, next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}locks (
    lock_name           VARCHAR(255) PRIMARY KEY,
    holder_id           VARCHAR(255) NOT NULL,
    acquired_at         DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at          DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}dlq (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    workflow_run_id     VARCHAR(255) NOT NULL,
    step_order          INT NOT NULL DEFAULT 0,
    error_message       TEXT,
    error_traceback     TEXT,
    step_input          LONGBLOB,
    retry_count         INT NOT NULL DEFAULT 0,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workers (
    worker_id           VARCHAR(255) PRIMARY KEY,
    node_id             VARCHAR(255) NOT NULL,
    status              VARCHAR(50) NOT NULL DEFAULT 'active',
    last_heartbeat      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    current_task        VARCHAR(255),
    started_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}schema_version (
    version             INT NOT NULL,
    applied_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workflow_definitions (
    name                VARCHAR(255) NOT NULL,
    version             INT NOT NULL,
    definition_json     LONGTEXT NOT NULL,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (name, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}circuit_breakers (
    name                VARCHAR(255) PRIMARY KEY,
    state_json          LONGTEXT NOT NULL,
    updated_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    return stmts
