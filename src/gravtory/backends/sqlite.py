# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""SQLite backend using aiosqlite — single-file persistence for development and testing.

.. warning::

    The SQLite backend is intended for **development and testing only**.
    It uses database-level locking (not row-level) which creates a
    serialization bottleneck under concurrent worker access.  For
    production deployments, use the PostgreSQL or MySQL backend with
    ``SELECT ... FOR UPDATE SKIP LOCKED``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import aiosqlite

from gravtory.backends.base import Backend
from gravtory.backends.schema import CURRENT_SCHEMA_VERSION, SQLITE_TOKENS, schema_sql
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
    import sqlite3
    from collections.abc import Sequence


def _parse_path(dsn: str) -> str:
    """Extract file path from sqlite:/// connection string."""
    if dsn.startswith("sqlite:///"):
        return dsn[len("sqlite:///") :]
    if dsn.startswith("sqlite://"):
        remainder = dsn[len("sqlite://") :]
        if remainder == ":memory:":
            return ":memory:"
        return remainder
    return dsn


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _parse_dt(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    # Handle various ISO formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


class SQLiteBackend(Backend):
    """SQLite backend using aiosqlite. WAL mode for concurrency."""

    def __init__(
        self,
        dsn: str = "sqlite:///gravtory.db",
        *,
        journal_mode: str = "WAL",
        busy_timeout: int = 5000,
        table_prefix: str = "gravtory_",
    ) -> None:
        self._path = _parse_path(dsn)
        self._journal_mode = journal_mode
        self._busy_timeout = busy_timeout
        self._prefix = table_prefix
        self._db: aiosqlite.Connection | None = None

    # ── Lifecycle ─────────────────────────────────────────────────

    async def _connect(self) -> None:
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute(f"PRAGMA journal_mode={self._journal_mode}")
        await self._db.execute(f"PRAGMA busy_timeout={self._busy_timeout}")
        await self._db.execute("PRAGMA foreign_keys=ON")

    async def initialize(self) -> None:
        import warnings

        warnings.warn(
            "SQLiteBackend is intended for development/testing only. "
            "For production, use PostgreSQLBackend or MySQLBackend.",
            stacklevel=2,
        )
        if self._db is None:
            await self._connect()
        db = self._ensure_connected()
        stmts = schema_sql(SQLITE_TOKENS, self._prefix)
        for stmt in stmts:
            await db.execute(stmt)
        # Insert schema version if not present
        row = await db.execute_fetchall(
            f"SELECT version FROM {self._prefix}schema_version ORDER BY version DESC LIMIT 1"
        )
        if not row:
            await db.execute(
                f"INSERT INTO {self._prefix}schema_version (version) VALUES (?)",
                (CURRENT_SCHEMA_VERSION,),
            )
        await db.commit()
        # Run pending migrations for existing databases
        from gravtory.backends.migration import SchemaMigrator

        migrator = SchemaMigrator(self)
        await migrator.check_and_migrate()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def health_check(self) -> bool:
        if self._db is None:
            return False
        try:
            await self._db.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _ensure_connected(self) -> aiosqlite.Connection:
        """Return the active DB connection or raise BackendConnectionError."""
        if self._db is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("SQLite", "Not connected. Call initialize() first.")
        return self._db

    @property
    def _conn(self) -> aiosqlite.Connection:
        """Narrowed accessor for the DB connection (raises if not connected)."""
        return self._ensure_connected()

    @property
    def _p(self) -> str:
        return self._prefix

    # ── Workflow runs ─────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        await self._conn.execute(
            f"""INSERT OR IGNORE INTO {self._p}workflow_runs
                (id, workflow_name, workflow_version, namespace, status,
                 current_step, input_data, error_message, error_traceback,
                 parent_run_id, deadline_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run.id,
                run.workflow_name,
                run.workflow_version,
                run.namespace,
                run.status.value if isinstance(run.status, WorkflowStatus) else run.status,
                run.current_step,
                run.input_data,
                run.error_message,
                run.error_traceback,
                run.parent_run_id,
                run.deadline_at.isoformat() if run.deadline_at else None,
            ),
        )
        await self._conn.commit()

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        rows = list(
            await self._conn.execute_fetchall(
                f"SELECT * FROM {self._p}workflow_runs WHERE id = ?", (run_id,)
            )
        )
        if not rows:
            return None
        return self._row_to_workflow_run(rows[0])

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        completed_at = (
            _now_iso()
            if status
            in (
                WorkflowStatus.COMPLETED,
                WorkflowStatus.FAILED,
                WorkflowStatus.COMPENSATED,
                WorkflowStatus.COMPENSATION_FAILED,
            )
            else None
        )
        await self._conn.execute(
            f"""UPDATE {self._p}workflow_runs
                SET status=?, updated_at=datetime('now'), error_message=COALESCE(?,error_message),
                    error_traceback=COALESCE(?,error_traceback),
                    output_data=COALESCE(?,output_data),
                    completed_at=COALESCE(?,completed_at)
                WHERE id=?""",
            (
                status.value if isinstance(status, WorkflowStatus) else status,
                error_message,
                error_traceback,
                output_data,
                completed_at,
                run_id,
            ),
        )
        await self._conn.commit()

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
        cursor = await self._conn.execute(
            f"""UPDATE {self._p}workflow_runs
                SET status=?, updated_at=datetime('now')
                WHERE id=? AND status=?""",
            (new_val, run_id, expected_val),
        )
        await self._conn.commit()
        return cursor.rowcount > 0

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        sql = f"SELECT * FROM {self._p}workflow_runs WHERE namespace = ?"
        params: list[Any] = [namespace]
        if status is not None:
            sql += " AND status = ?"
            params.append(status.value if isinstance(status, WorkflowStatus) else status)
        if workflow_name is not None:
            sql += " AND workflow_name = ?"
            params.append(workflow_name)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = await self._conn.execute_fetchall(sql, params)
        return [self._row_to_workflow_run(r) for r in rows]

    # ── Step outputs ──────────────────────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None  # Only persist bytes; real serialization in Section 04
        # Atomic checkpoint: step output + run current_step in single transaction
        await self._conn.execute("BEGIN")
        try:
            await self._conn.execute(
                f"""INSERT INTO {self._p}step_outputs
                    (workflow_run_id, step_order, step_name, output_data,
                     output_type, duration_ms, retry_count, status, error_message)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(workflow_run_id, step_order) DO UPDATE SET
                        output_data=excluded.output_data,
                        output_type=excluded.output_type,
                        duration_ms=excluded.duration_ms,
                        retry_count=excluded.retry_count,
                        status=excluded.status,
                        error_message=excluded.error_message""",
                (
                    output.workflow_run_id,
                    output.step_order,
                    output.step_name,
                    out_data,
                    output.output_type,
                    output.duration_ms,
                    output.retry_count,
                    output.status.value if isinstance(output.status, StepStatus) else output.status,
                    output.error_message,
                ),
            )
            # Update run's current_step
            await self._conn.execute(
                f"UPDATE {self._p}workflow_runs SET current_step=?, updated_at=datetime('now') WHERE id=?",
                (output.step_order, output.workflow_run_id),
            )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        rows = await self._conn.execute_fetchall(
            f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=? ORDER BY step_order",
            (run_id,),
        )
        return [self._row_to_step_output(r) for r in rows]

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        rows = list(
            await self._conn.execute_fetchall(
                f"SELECT * FROM {self._p}step_outputs WHERE workflow_run_id=? AND step_order=?",
                (run_id, step_order),
            )
        )
        if not rows:
            return None
        return self._row_to_step_output(rows[0])

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        await self._conn.execute(
            f"UPDATE {self._p}step_outputs SET output_data=? WHERE workflow_run_id=? AND step_order=?",
            (output_data, run_id, step_order),
        )
        await self._conn.commit()

    # ── Pending steps ─────────────────────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}pending_steps
                (workflow_run_id, step_order, priority, status, scheduled_at, max_retries)
                VALUES (?,?,?,?,datetime('now'),?)""",
            (
                step.workflow_run_id,
                step.step_order,
                step.priority,
                "pending",
                step.max_retries,
            ),
        )
        await self._conn.commit()

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        # SQLite: BEGIN IMMEDIATE allows concurrent readers (WAL mode)
        # while still providing write serialization for safe claiming.
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = list(
                await self._conn.execute_fetchall(
                    f"""SELECT * FROM {self._p}pending_steps
                        WHERE status = 'pending' AND scheduled_at <= datetime('now')
                        ORDER BY priority DESC, created_at ASC LIMIT 1"""
                )
            )
            if not rows:
                await self._conn.commit()
                return None
            row = rows[0]
            await self._conn.execute(
                f"""UPDATE {self._p}pending_steps
                    SET status='running', worker_id=?, started_at=datetime('now')
                    WHERE id=?""",
                (worker_id, row["id"]),
            )
            await self._conn.commit()
            ps = self._row_to_pending_step(row)
            ps.status = StepStatus.RUNNING
            ps.worker_id = worker_id
            return ps
        except Exception:
            await self._conn.rollback()
            raise

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None
        # Atomic: mark pending step complete + save output + update run in one txn
        await self._conn.execute("BEGIN")
        try:
            await self._conn.execute(
                f"UPDATE {self._p}pending_steps SET status='completed', completed_at=datetime('now') WHERE id=?",
                (step_id,),
            )
            await self._conn.execute(
                f"""INSERT INTO {self._p}step_outputs
                    (workflow_run_id, step_order, step_name, output_data,
                     output_type, duration_ms, retry_count, status, error_message)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(workflow_run_id, step_order) DO UPDATE SET
                        output_data=excluded.output_data,
                        output_type=excluded.output_type,
                        duration_ms=excluded.duration_ms,
                        retry_count=excluded.retry_count,
                        status=excluded.status,
                        error_message=excluded.error_message""",
                (
                    output.workflow_run_id,
                    output.step_order,
                    output.step_name,
                    out_data,
                    output.output_type,
                    output.duration_ms,
                    output.retry_count,
                    output.status.value if isinstance(output.status, StepStatus) else output.status,
                    output.error_message,
                ),
            )
            await self._conn.execute(
                f"UPDATE {self._p}workflow_runs SET current_step=?, updated_at=datetime('now') WHERE id=?",
                (output.step_order, output.workflow_run_id),
            )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: Any | None = None,
    ) -> None:
        if retry_at is not None:
            await self._conn.execute(
                f"""UPDATE {self._p}pending_steps
                    SET status='pending', retry_count=retry_count+1,
                        next_retry_at=?, scheduled_at=?
                    WHERE id=?""",
                (str(retry_at), str(retry_at), step_id),
            )
        else:
            await self._conn.execute(
                f"UPDATE {self._p}pending_steps SET status='failed' WHERE id=?",
                (step_id,),
            )
        await self._conn.commit()

    # ── Signals ───────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}signals
                (workflow_run_id, signal_name, signal_data, consumed)
                VALUES (?,?,?,0)""",
            (signal.workflow_run_id, signal.signal_name, signal.signal_data),
        )
        await self._conn.commit()

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        # Wrap in BEGIN IMMEDIATE for atomic read-then-update
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = list(
                await self._conn.execute_fetchall(
                    f"""SELECT * FROM {self._p}signals
                        WHERE workflow_run_id=? AND signal_name=? AND consumed=0
                        ORDER BY created_at ASC LIMIT 1""",
                    (run_id, signal_name),
                )
            )
            if not rows:
                await self._conn.commit()
                return None
            row = rows[0]
            await self._conn.execute(
                f"UPDATE {self._p}signals SET consumed=1 WHERE id=?", (row["id"],)
            )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise
        sig = self._row_to_signal(row)
        sig.consumed = True
        return sig

    async def register_signal_wait(self, wait: SignalWait) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}signal_waits
                (workflow_run_id, signal_name, timeout_at)
                VALUES (?,?,?)""",
            (
                wait.workflow_run_id,
                wait.signal_name,
                str(wait.timeout_at) if wait.timeout_at else None,
            ),
        )
        await self._conn.commit()

    # ── Compensation ──────────────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}compensations
                (workflow_run_id, step_order, handler_name, step_output, status)
                VALUES (?,?,?,?,?)""",
            (
                comp.workflow_run_id,
                comp.step_order,
                comp.handler_name,
                comp.step_output,
                comp.status,
            ),
        )
        await self._conn.commit()

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        rows = await self._conn.execute_fetchall(
            f"SELECT * FROM {self._p}compensations WHERE workflow_run_id=? ORDER BY step_order DESC",
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
        await self._conn.execute(
            f"UPDATE {self._p}compensations SET status=?, error_message=COALESCE(?,error_message) WHERE id=?",
            (status, error_message, compensation_id),
        )
        await self._conn.commit()

    # ── Scheduling ────────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}schedules
                (id, workflow_name, schedule_type, schedule_config, namespace,
                 enabled, next_run_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    schedule_config=excluded.schedule_config,
                    enabled=excluded.enabled,
                    next_run_at=excluded.next_run_at""",
            (
                schedule.id,
                schedule.workflow_name,
                schedule.schedule_type,
                schedule.schedule_config,
                schedule.namespace,
                1 if schedule.enabled else 0,
                str(schedule.next_run_at) if schedule.next_run_at else None,
            ),
        )
        await self._conn.commit()

    async def get_due_schedules(self) -> Sequence[Schedule]:
        rows = await self._conn.execute_fetchall(
            f"""SELECT * FROM {self._p}schedules
                WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at <= datetime('now')"""
        )
        return [self._row_to_schedule(r) for r in rows]

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        await self._conn.execute(
            f"UPDATE {self._p}schedules SET last_run_at=?, next_run_at=? WHERE id=?",
            (str(last_run_at), str(next_run_at), schedule_id),
        )
        await self._conn.commit()

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        rows = list(
            await self._conn.execute_fetchall(
                f"SELECT * FROM {self._p}schedules WHERE enabled=1",
            )
        )
        return [self._row_to_schedule(r) for r in rows]

    async def list_all_schedules(self) -> Sequence[Schedule]:
        rows = list(
            await self._conn.execute_fetchall(
                f"SELECT * FROM {self._p}schedules",
            )
        )
        return [self._row_to_schedule(r) for r in rows]

    # ── Locks ─────────────────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        if self._db is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("SQLite", "Not connected")
        now = _now_iso()
        expires_at = (datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        # Atomic INSERT ... ON CONFLICT to eliminate TOCTOU race.
        # The UPDATE only fires when the existing lock is expired OR held
        # by the same holder.
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            await self._conn.execute(
                f"""INSERT INTO {self._p}locks (lock_name, holder_id, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(lock_name) DO UPDATE
                        SET holder_id=excluded.holder_id,
                            acquired_at=excluded.acquired_at,
                            expires_at=excluded.expires_at
                        WHERE {self._p}locks.holder_id = excluded.holder_id
                           OR {self._p}locks.expires_at <= ?""",
                (lock_name, holder_id, now, expires_at, now),
            )
            # Verify we actually hold the lock
            rows = list(
                await self._conn.execute_fetchall(
                    f"SELECT holder_id FROM {self._p}locks WHERE lock_name=?",
                    (lock_name,),
                )
            )
            await self._conn.commit()
            return bool(rows) and rows[0]["holder_id"] == holder_id
        except Exception:
            await self._conn.rollback()
            raise

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        cur = await self._conn.execute(
            f"DELETE FROM {self._p}locks WHERE lock_name=? AND holder_id=?",
            (lock_name, holder_id),
        )
        await self._conn.commit()
        return bool(cur.rowcount is not None and cur.rowcount > 0)

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        if self._db is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("SQLite", "Not connected")
        expires_at = (datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        cur = await self._conn.execute(
            f"""UPDATE {self._p}locks
                SET expires_at=?
                WHERE lock_name=? AND holder_id=?""",
            (expires_at, lock_name, holder_id),
        )
        await self._conn.commit()
        return bool(cur.rowcount is not None and cur.rowcount > 0)

    # ── DLQ ───────────────────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}dlq
                (workflow_run_id, step_order, error_message, error_traceback, retry_count)
                VALUES (?,?,?,?,?)""",
            (
                entry.workflow_run_id,
                entry.step_order,
                entry.error_message,
                entry.error_traceback,
                entry.retry_count,
            ),
        )
        await self._conn.commit()

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        rows = await self._conn.execute_fetchall(
            f"SELECT * FROM {self._p}dlq ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [self._row_to_dlq_entry(r) for r in rows]

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        rows = list(
            await self._conn.execute_fetchall(f"SELECT * FROM {self._p}dlq WHERE id=?", (entry_id,))
        )
        return self._row_to_dlq_entry(rows[0]) if rows else None

    async def count_dlq(self, *, namespace: str = "default") -> int:
        rows = list(await self._conn.execute_fetchall(f"SELECT COUNT(*) AS cnt FROM {self._p}dlq"))
        return int(rows[0]["cnt"]) if rows else 0

    async def remove_from_dlq(self, entry_id: int) -> None:
        await self._conn.execute(f"DELETE FROM {self._p}dlq WHERE id=?", (entry_id,))
        await self._conn.commit()

    # ── Workers ───────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}workers (worker_id, node_id, status)
                VALUES (?,?,?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    node_id=excluded.node_id,
                    last_heartbeat=datetime('now')""",
            (worker.worker_id, worker.node_id, worker.status),
        )
        await self._conn.commit()

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        if current_task is not None:
            await self._conn.execute(
                f"UPDATE {self._p}workers SET last_heartbeat=datetime('now'), current_task=? WHERE worker_id=?",
                (current_task, worker_id),
            )
        else:
            await self._conn.execute(
                f"UPDATE {self._p}workers SET last_heartbeat=datetime('now') WHERE worker_id=?",
                (worker_id,),
            )
        await self._conn.commit()

    async def deregister_worker(self, worker_id: str) -> None:
        await self._conn.execute(f"DELETE FROM {self._p}workers WHERE worker_id=?", (worker_id,))
        await self._conn.commit()

    async def list_workers(self) -> Sequence[WorkerInfo]:
        rows = await self._conn.execute_fetchall(f"SELECT * FROM {self._p}workers")
        return [self._row_to_worker(r) for r in rows]

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        if self._db is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("SQLite", "Not connected")
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(seconds=stale_threshold_seconds)
        ).isoformat()
        rows = await self._conn.execute_fetchall(
            f"""SELECT * FROM {self._p}workers
                WHERE last_heartbeat < ?
                   OR last_heartbeat IS NULL""",
            (cutoff,),
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
        query = f"SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs WHERE namespace=?"
        params: list[object] = [namespace]
        if status is not None:
            query += " AND status=?"
            params.append(status.value)
        if workflow_name is not None:
            query += " AND workflow_name=?"
            params.append(workflow_name)
        rows = await self._conn.execute_fetchall(query, tuple(params))
        result = list(rows)
        return int(result[0]["cnt"]) if result else 0

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        rows = await self._conn.execute_fetchall(
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
        await self._conn.execute(
            f"""INSERT INTO {self._p}parallel_results (workflow_run_id, step_order, item_index, output_data)
                VALUES (?,?,?,?)
                ON CONFLICT (workflow_run_id, step_order, item_index) DO UPDATE SET output_data=excluded.output_data""",
            (run_id, step_order, item_index, output_data),
        )
        await self._conn.commit()

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        rows = await self._conn.execute_fetchall(
            f"SELECT item_index, output_data FROM {self._p}parallel_results WHERE workflow_run_id=? AND step_order=?",
            (run_id, step_order),
        )
        return {int(r["item_index"]): r["output_data"] for r in rows}

    # ── DLQ extras ─────────────────────────────────────────────────

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        cursor = await self._conn.execute(f"SELECT COUNT(*) AS cnt FROM {self._p}dlq")
        row = await cursor.fetchone()
        count = int(row["cnt"]) if row else 0
        await self._conn.execute(f"DELETE FROM {self._p}dlq")
        await self._conn.commit()
        return count

    # ── Task reclamation ─────────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        cursor = await self._conn.execute(
            f"""UPDATE {self._p}pending_steps
                SET status='pending', worker_id=NULL, started_at=NULL
                WHERE worker_id=? AND status='running'""",
            (worker_id,),
        )
        await self._conn.commit()
        return cursor.rowcount

    # ── Concurrency control ─────────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        result = list(
            await self._conn.execute_fetchall(
                f"""SELECT COUNT(*) AS cnt FROM {self._p}workflow_runs
                    WHERE workflow_name=? AND namespace=? AND status IN ('running','pending')""",
                (workflow_name, namespace),
            )
        )
        count = result[0]["cnt"] if result else 0
        return int(count) < max_concurrent

    # ── Row mappers ───────────────────────────────────────────────

    @staticmethod
    def _row_to_workflow_run(row: sqlite3.Row | aiosqlite.Row) -> WorkflowRun:
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
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            completed_at=_parse_dt(row["completed_at"]),
            deadline_at=_parse_dt(row["deadline_at"]),
        )

    @staticmethod
    def _row_to_step_output(row: sqlite3.Row | aiosqlite.Row) -> StepOutput:
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
            created_at=_parse_dt(row["created_at"]),
        )

    @staticmethod
    def _row_to_pending_step(row: sqlite3.Row | aiosqlite.Row) -> PendingStep:
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
    def _row_to_signal(row: sqlite3.Row | aiosqlite.Row) -> Signal:
        return Signal(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            signal_name=row["signal_name"],
            signal_data=row["signal_data"],
            consumed=bool(row["consumed"]),
            created_at=_parse_dt(row["created_at"]),
        )

    @staticmethod
    def _row_to_compensation(row: sqlite3.Row | aiosqlite.Row) -> Compensation:
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
    def _row_to_schedule(row: sqlite3.Row | aiosqlite.Row) -> Schedule:
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
            enabled=bool(row["enabled"]),
            last_run_at=_parse_dt(row["last_run_at"]),
            next_run_at=_parse_dt(row["next_run_at"]),
        )

    @staticmethod
    def _row_to_dlq_entry(row: sqlite3.Row | aiosqlite.Row) -> DLQEntry:
        return DLQEntry(
            id=row["id"],
            workflow_run_id=row["workflow_run_id"],
            step_order=row["step_order"],
            error_message=row["error_message"],
            error_traceback=row["error_traceback"],
            retry_count=row["retry_count"],
        )

    # ── Child runs ────────────────────────────────────────────────

    async def list_child_runs(self, parent_run_id: str) -> list[WorkflowRun]:
        rows = await self._conn.execute_fetchall(
            f"""SELECT * FROM {self._p}workflow_runs
                WHERE parent_run_id=? AND status IN ('running','pending')""",
            (parent_run_id,),
        )
        return [self._row_to_workflow_run(r) for r in rows]

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}workflow_definitions (name, version, definition_json)
                VALUES (?, ?, ?)
                ON CONFLICT (name, version) DO UPDATE SET definition_json=excluded.definition_json""",
            (name, version, definition_json),
        )
        await self._conn.commit()

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        rows = await self._conn.execute_fetchall(
            f"SELECT name, version, definition_json FROM {self._p}workflow_definitions ORDER BY name, version"
        )
        return [(str(r["name"]), int(r["version"]), str(r["definition_json"])) for r in rows]

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        await self._conn.execute(
            f"DELETE FROM {self._p}workflow_definitions WHERE name=? AND version=?",
            (name, version),
        )
        await self._conn.commit()

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        if not definitions:
            return 0
        await self._conn.executemany(
            f"""INSERT INTO {self._p}workflow_definitions (name, version, definition_json)
                VALUES (?, ?, ?)
                ON CONFLICT (name, version) DO UPDATE SET definition_json=excluded.definition_json""",
            [(n, v, d) for n, v, d in definitions],
        )
        await self._conn.commit()
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        if not keys:
            return 0
        await self._conn.executemany(
            f"DELETE FROM {self._p}workflow_definitions WHERE name=? AND version=?",
            [(n, v) for n, v in keys],
        )
        await self._conn.commit()
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        await self._conn.execute(
            f"""INSERT INTO {self._p}circuit_breakers (name, state_json)
                VALUES (?, ?)
                ON CONFLICT (name) DO UPDATE SET state_json=excluded.state_json,
                    updated_at=datetime('now')""",
            (name, state_json),
        )
        await self._conn.commit()

    async def load_circuit_state(self, name: str) -> str | None:
        rows = await self._conn.execute_fetchall(
            f"SELECT state_json FROM {self._p}circuit_breakers WHERE name=?",
            (name,),
        )
        if rows:
            row = next(iter(rows))
            return str(row["state_json"])
        return None

    @staticmethod
    def _row_to_worker(row: sqlite3.Row | aiosqlite.Row) -> WorkerInfo:
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
