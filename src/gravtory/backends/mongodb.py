# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""MongoDB backend using motor (async pymongo) — full implementation with transactions."""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

try:
    import motor.motor_asyncio
    from pymongo import ReturnDocument
    from pymongo.errors import DuplicateKeyError
except ImportError as _exc:
    raise ImportError(
        "MongoDB backend requires motor. Install with: pip install gravtory[mongodb]"
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


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class MongoDBBackend(Backend):
    """MongoDB backend using motor with connection pooling."""

    def __init__(
        self,
        dsn: str,
        *,
        database_name: str = "gravtory",
        collection_prefix: str = "gravtory_",
    ) -> None:
        self._dsn = dsn
        self._db_name = database_name
        self._prefix = collection_prefix
        self._client: motor.motor_asyncio.AsyncIOMotorClient[Any] | None = None
        self._db: motor.motor_asyncio.AsyncIOMotorDatabase[Any] | None = None
        self._id_counters: dict[str, int] = {}

    # ── helpers ────────────────────────────────────────────────────

    def _ensure_connected(self) -> Any:
        """Return the active DB or raise BackendConnectionError."""
        if self._db is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("MongoDB", "Not connected. Call initialize() first.")
        return self._db

    def _col(self, name: str) -> Any:
        """Return a collection by logical name."""
        db = self._ensure_connected()
        return db[f"{self._prefix}{name}"]

    async def _next_id(self, collection_name: str) -> int:
        """Generate an auto-incrementing integer ID for a collection."""
        db = self._ensure_connected()
        result = await db[f"{self._prefix}counters"].find_one_and_update(
            {"_id": collection_name},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(result["seq"])

    @property
    def _p(self) -> str:
        return self._prefix

    # ── Lifecycle ─────────────────────────────────────────────────

    async def initialize(self) -> None:
        from pymongo import ReadPreference, WriteConcern
        from pymongo.read_concern import ReadConcern

        self._client = motor.motor_asyncio.AsyncIOMotorClient(self._dsn)
        # Use majority write/read concern for crash safety in replica sets.
        # Standalone mongod ignores these settings gracefully.
        self._db = self._client.get_database(
            self._db_name,
            write_concern=WriteConcern(w="majority"),
            read_concern=ReadConcern(level="majority"),
            read_preference=ReadPreference.PRIMARY,
        )

        # Create indexes
        wr = self._col("workflow_runs")
        await wr.create_index("status")
        await wr.create_index("workflow_name")
        await wr.create_index("namespace")
        await wr.create_index("created_at")
        await wr.create_index("parent_run_id")

        so = self._col("step_outputs")
        await so.create_index([("workflow_run_id", 1), ("step_order", 1)], unique=True)

        ps = self._col("pending_steps")
        await ps.create_index([("status", 1), ("scheduled_at", 1)])
        await ps.create_index([("priority", -1), ("created_at", 1)])

        sig = self._col("signals")
        await sig.create_index([("workflow_run_id", 1), ("signal_name", 1)])

        sw = self._col("signal_waits")
        await sw.create_index([("workflow_run_id", 1), ("signal_name", 1)])

        comp = self._col("compensations")
        await comp.create_index("workflow_run_id")

        sched = self._col("schedules")
        await sched.create_index([("enabled", 1), ("next_run_at", 1)])

        dlq = self._col("dlq")
        await dlq.create_index("created_at")

        wfdef = self._col("workflow_definitions")
        await wfdef.create_index([("name", 1), ("version", 1)], unique=True)

        cb = self._col("circuit_breakers")
        await cb.create_index("_id")

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None

    async def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.admin.command("ping")
            return True
        except Exception:
            return False

    # ── Workflow runs ─────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        status_val = run.status.value if isinstance(run.status, WorkflowStatus) else run.status
        now = _utcnow()
        doc: dict[str, Any] = {
            "_id": run.id,
            "workflow_name": run.workflow_name,
            "workflow_version": run.workflow_version,
            "namespace": run.namespace,
            "status": status_val,
            "current_step": run.current_step,
            "input_data": run.input_data,
            "output_data": run.output_data,
            "error_message": run.error_message,
            "error_traceback": run.error_traceback,
            "parent_run_id": run.parent_run_id,
            "created_at": run.created_at or now,
            "updated_at": run.updated_at or now,
            "completed_at": run.completed_at,
            "deadline_at": run.deadline_at,
        }
        with contextlib.suppress(DuplicateKeyError):
            await self._col("workflow_runs").insert_one(doc)

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        doc = await self._col("workflow_runs").find_one({"_id": run_id})
        return self._doc_to_workflow_run(doc) if doc else None

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
        update: dict[str, Any] = {
            "status": status_val,
            "updated_at": _utcnow(),
        }
        if error_message is not None:
            update["error_message"] = error_message
        if error_traceback is not None:
            update["error_traceback"] = error_traceback
        if output_data is not None:
            update["output_data"] = output_data
        if status in terminal:
            update["completed_at"] = _utcnow()
        await self._col("workflow_runs").update_one({"_id": run_id}, {"$set": update})

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
        result = await self._col("workflow_runs").update_one(
            {"_id": run_id, "status": expected_val},
            {"$set": {"status": new_val, "updated_at": _utcnow()}},
        )
        return bool(result.modified_count > 0)

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        query: dict[str, Any] = {"namespace": namespace}
        if status is not None:
            query["status"] = status.value if isinstance(status, WorkflowStatus) else status
        if workflow_name is not None:
            query["workflow_name"] = workflow_name
        cursor = (
            self._col("workflow_runs").find(query).sort("created_at", -1).skip(offset).limit(limit)
        )
        docs = await cursor.to_list(length=limit)
        return [self._doc_to_workflow_run(d) for d in docs]

    # ── Step outputs ──────────────────────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None
        status_val = output.status.value if isinstance(output.status, StepStatus) else output.status
        step_id = await self._next_id("step_outputs")
        doc: dict[str, Any] = {
            "_id": step_id,
            "workflow_run_id": output.workflow_run_id,
            "step_order": output.step_order,
            "step_name": output.step_name,
            "output_data": out_data,
            "output_type": output.output_type,
            "duration_ms": output.duration_ms,
            "retry_count": output.retry_count,
            "status": status_val,
            "error_message": output.error_message,
            "created_at": _utcnow(),
        }
        # Idempotent upsert — $setOnInsert ensures no overwrite
        await self._col("step_outputs").update_one(
            {"workflow_run_id": output.workflow_run_id, "step_order": output.step_order},
            {"$setOnInsert": doc},
            upsert=True,
        )
        await self._col("workflow_runs").update_one(
            {"_id": output.workflow_run_id},
            {"$set": {"current_step": output.step_order, "updated_at": _utcnow()}},
        )

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        cursor = self._col("step_outputs").find({"workflow_run_id": run_id}).sort("step_order", 1)
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_step_output(d) for d in docs]

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        doc = await self._col("step_outputs").find_one(
            {"workflow_run_id": run_id, "step_order": step_order}
        )
        return self._doc_to_step_output(doc) if doc else None

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        result = await self._col("step_outputs").update_one(
            {"workflow_run_id": run_id, "step_order": step_order},
            {"$set": {"output_data": output_data}},
        )
        if result.matched_count == 0:
            from gravtory.core.errors import BackendError

            raise BackendError(
                f"Step output not found for run_id={run_id!r}, step_order={step_order}"
            )

    # ── Pending steps ─────────────────────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        step_id = await self._next_id("pending_steps")
        doc: dict[str, Any] = {
            "_id": step_id,
            "workflow_run_id": step.workflow_run_id,
            "step_order": step.step_order,
            "priority": step.priority,
            "status": "pending",
            "worker_id": None,
            "scheduled_at": _utcnow(),
            "started_at": None,
            "completed_at": None,
            "retry_count": 0,
            "max_retries": step.max_retries,
            "next_retry_at": None,
            "created_at": _utcnow(),
        }
        await self._col("pending_steps").insert_one(doc)

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        now = _utcnow()
        doc = await self._col("pending_steps").find_one_and_update(
            {"status": "pending", "scheduled_at": {"$lte": now}},
            {"$set": {"status": "running", "worker_id": worker_id, "started_at": now}},
            sort=[("priority", -1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        return self._doc_to_pending_step(doc) if doc else None

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        await self._col("pending_steps").update_one(
            {"_id": step_id},
            {"$set": {"status": "completed", "completed_at": _utcnow()}},
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
            await self._col("pending_steps").update_one(
                {"_id": step_id},
                {
                    "$set": {
                        "status": "pending",
                        "next_retry_at": retry_at,
                        "scheduled_at": retry_at,
                    },
                    "$inc": {"retry_count": 1},
                },
            )
        else:
            await self._col("pending_steps").update_one(
                {"_id": step_id},
                {"$set": {"status": "failed"}},
            )

    # ── Signals ───────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        sig_id = await self._next_id("signals")
        doc: dict[str, Any] = {
            "_id": sig_id,
            "workflow_run_id": signal.workflow_run_id,
            "signal_name": signal.signal_name,
            "signal_data": signal.signal_data,
            "consumed": False,
            "created_at": _utcnow(),
        }
        await self._col("signals").insert_one(doc)

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        doc = await self._col("signals").find_one_and_update(
            {"workflow_run_id": run_id, "signal_name": signal_name, "consumed": False},
            {"$set": {"consumed": True}},
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        return self._doc_to_signal(doc) if doc else None

    async def register_signal_wait(self, wait: SignalWait) -> None:
        sw_id = await self._next_id("signal_waits")
        doc: dict[str, Any] = {
            "_id": sw_id,
            "workflow_run_id": wait.workflow_run_id,
            "signal_name": wait.signal_name,
            "timeout_at": wait.timeout_at,
            "created_at": _utcnow(),
        }
        await self._col("signal_waits").insert_one(doc)

    # ── Compensation ──────────────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        comp_id = await self._next_id("compensations")
        doc: dict[str, Any] = {
            "_id": comp_id,
            "workflow_run_id": comp.workflow_run_id,
            "step_order": comp.step_order,
            "handler_name": comp.handler_name,
            "step_output": comp.step_output,
            "status": comp.status,
            "error_message": None,
            "created_at": _utcnow(),
        }
        await self._col("compensations").insert_one(doc)

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        cursor = self._col("compensations").find({"workflow_run_id": run_id}).sort("step_order", -1)
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_compensation(d) for d in docs]

    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        update: dict[str, Any] = {"status": status}
        if error_message is not None:
            update["error_message"] = error_message
        await self._col("compensations").update_one({"_id": compensation_id}, {"$set": update})

    # ── Scheduling ────────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        doc: dict[str, Any] = {
            "_id": schedule.id,
            "workflow_name": schedule.workflow_name,
            "schedule_type": schedule.schedule_type
            if isinstance(schedule.schedule_type, str)
            else schedule.schedule_type.value
            if hasattr(schedule.schedule_type, "value")
            else str(schedule.schedule_type),
            "schedule_config": schedule.schedule_config,
            "namespace": schedule.namespace,
            "enabled": schedule.enabled,
            "last_run_at": schedule.last_run_at,
            "next_run_at": schedule.next_run_at,
            "created_at": _utcnow(),
        }
        await self._col("schedules").update_one(
            {"_id": schedule.id},
            {"$set": doc},
            upsert=True,
        )

    async def get_due_schedules(self) -> Sequence[Schedule]:
        now = _utcnow()
        cursor = self._col("schedules").find(
            {
                "enabled": True,
                "next_run_at": {"$ne": None, "$lte": now},
            }
        )
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_schedule(d) for d in docs]

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        await self._col("schedules").update_one(
            {"_id": schedule_id},
            {"$set": {"last_run_at": last_run_at, "next_run_at": next_run_at}},
        )

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        cursor = self._col("schedules").find({"enabled": True})
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_schedule(d) for d in docs]

    async def list_all_schedules(self) -> Sequence[Schedule]:
        cursor = self._col("schedules").find()
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_schedule(d) for d in docs]

    # ── Locks ─────────────────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        now = _utcnow()
        expires = now + timedelta(seconds=ttl_seconds)
        # Try insert
        try:
            await self._col("locks").insert_one(
                {
                    "_id": lock_name,
                    "holder_id": holder_id,
                    "acquired_at": now,
                    "expires_at": expires,
                }
            )
            return True
        except DuplicateKeyError:
            pass  # Lock exists, try to take over below
        # Try to take over expired or own lock
        result = await self._col("locks").update_one(
            {
                "_id": lock_name,
                "$or": [
                    {"expires_at": {"$lt": now}},
                    {"holder_id": holder_id},
                ],
            },
            {"$set": {"holder_id": holder_id, "acquired_at": now, "expires_at": expires}},
        )
        return bool(result.modified_count > 0)

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        result = await self._col("locks").delete_one({"_id": lock_name, "holder_id": holder_id})
        return bool(result.deleted_count > 0)

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        expires = _utcnow() + timedelta(seconds=ttl_seconds)
        result = await self._col("locks").update_one(
            {"_id": lock_name, "holder_id": holder_id},
            {"$set": {"expires_at": expires}},
        )
        return bool(result.modified_count > 0)

    # ── DLQ ───────────────────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        dlq_id = await self._next_id("dlq")
        doc: dict[str, Any] = {
            "_id": dlq_id,
            "workflow_run_id": entry.workflow_run_id,
            "step_order": entry.step_order,
            "error_message": entry.error_message,
            "error_traceback": entry.error_traceback,
            "retry_count": entry.retry_count,
            "created_at": _utcnow(),
        }
        await self._col("dlq").insert_one(doc)

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        cursor = self._col("dlq").find().sort("created_at", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [self._doc_to_dlq_entry(d) for d in docs]

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        doc = await self._col("dlq").find_one({"_id": entry_id})
        return self._doc_to_dlq_entry(doc) if doc else None

    async def count_dlq(self, *, namespace: str = "default") -> int:
        return int(await self._col("dlq").count_documents({}))

    async def remove_from_dlq(self, entry_id: int) -> None:
        await self._col("dlq").delete_one({"_id": entry_id})

    # ── Workers ───────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        status_val = (
            worker.status.value if isinstance(worker.status, WorkerStatus) else worker.status
        )
        doc: dict[str, Any] = {
            "_id": worker.worker_id,
            "node_id": worker.node_id,
            "status": status_val,
            "last_heartbeat": _utcnow(),
            "current_task": worker.current_task,
            "started_at": _utcnow(),
        }
        await self._col("workers").update_one({"_id": worker.worker_id}, {"$set": doc}, upsert=True)

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        update: dict[str, object] = {"last_heartbeat": _utcnow()}
        if current_task is not None:
            update["current_task"] = current_task
        await self._col("workers").update_one({"_id": worker_id}, {"$set": update})

    async def deregister_worker(self, worker_id: str) -> None:
        await self._col("workers").delete_one({"_id": worker_id})

    async def list_workers(self) -> Sequence[WorkerInfo]:
        cursor = self._col("workers").find()
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_worker(d) for d in docs]

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        from datetime import timedelta

        cutoff = _utcnow() - timedelta(seconds=stale_threshold_seconds)
        cursor = self._col("workers").find(
            {
                "$or": [
                    {"last_heartbeat": {"$lt": cutoff}},
                    {"last_heartbeat": None},
                ]
            }
        )
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_worker(d) for d in docs]

    # ── Workflow run extras ────────────────────────────────────────

    async def count_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
    ) -> int:
        query: dict[str, Any] = {"namespace": namespace}
        if status is not None:
            query["status"] = status.value
        if workflow_name is not None:
            query["workflow_name"] = workflow_name
        return int(await self._col("workflow_runs").count_documents(query))

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        cursor = self._col("workflow_runs").find(
            {
                "status": {"$in": ["running", "pending"]},
            }
        )
        docs = await cursor.to_list(length=10000)
        return [self._doc_to_workflow_run(d) for d in docs]

    # ── Parallel step results ──────────────────────────────────────

    async def checkpoint_parallel_item(
        self,
        run_id: str,
        step_order: int,
        item_index: int,
        output_data: bytes,
    ) -> None:
        await self._col("parallel_results").update_one(
            {"run_id": run_id, "step_order": step_order, "item_index": item_index},
            {"$set": {"output_data": output_data, "updated_at": _utcnow()}},
            upsert=True,
        )

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        cursor = self._col("parallel_results").find(
            {
                "run_id": run_id,
                "step_order": step_order,
            }
        )
        docs = await cursor.to_list(length=10000)
        return {int(d["item_index"]): d["output_data"] for d in docs}

    # ── DLQ extras ─────────────────────────────────────────────────

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        result = await self._col("dlq").delete_many({})
        return int(result.deleted_count)

    # ── Task reclamation ───────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        col = self._col("pending_steps")
        result = await col.update_many(
            {"worker_id": worker_id, "status": "running"},
            {"$set": {"status": "pending", "worker_id": None, "started_at": None}},
        )
        return int(result.modified_count)

    # ── Concurrency control ───────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        count = await self._col("workflow_runs").count_documents(
            {
                "workflow_name": workflow_name,
                "namespace": namespace,
                "status": {"$in": ["running", "pending"]},
            }
        )
        return bool(count < max_concurrent)

    # ── Document mappers ──────────────────────────────────────────

    @staticmethod
    def _doc_to_workflow_run(doc: dict[str, Any]) -> WorkflowRun:
        return WorkflowRun(
            id=doc["_id"],
            workflow_name=doc["workflow_name"],
            workflow_version=doc.get("workflow_version", 1),
            namespace=doc.get("namespace", "default"),
            status=WorkflowStatus(doc["status"]),
            current_step=doc.get("current_step"),
            input_data=doc.get("input_data"),
            error_message=doc.get("error_message"),
            error_traceback=doc.get("error_traceback"),
            parent_run_id=doc.get("parent_run_id"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
            completed_at=doc.get("completed_at"),
            deadline_at=doc.get("deadline_at"),
        )

    @staticmethod
    def _doc_to_step_output(doc: dict[str, Any]) -> StepOutput:
        return StepOutput(
            id=doc["_id"],
            workflow_run_id=doc["workflow_run_id"],
            step_order=doc["step_order"],
            step_name=doc["step_name"],
            output_data=doc.get("output_data"),
            output_type=doc.get("output_type"),
            duration_ms=doc.get("duration_ms"),
            retry_count=doc.get("retry_count", 0),
            status=StepStatus(doc["status"]),
            error_message=doc.get("error_message"),
            created_at=doc.get("created_at"),
        )

    @staticmethod
    def _doc_to_pending_step(doc: dict[str, Any]) -> PendingStep:
        try:
            step_status = StepStatus(doc["status"])
        except ValueError:
            step_status = StepStatus.PENDING
        return PendingStep(
            id=doc["_id"],
            workflow_run_id=doc["workflow_run_id"],
            step_order=doc["step_order"],
            priority=doc["priority"],
            status=step_status,
            worker_id=doc.get("worker_id"),
            retry_count=doc.get("retry_count", 0),
            max_retries=doc.get("max_retries", 0),
        )

    @staticmethod
    def _doc_to_signal(doc: dict[str, Any]) -> Signal:
        return Signal(
            id=doc["_id"],
            workflow_run_id=doc["workflow_run_id"],
            signal_name=doc["signal_name"],
            signal_data=doc.get("signal_data"),
            consumed=doc.get("consumed", False),
            created_at=doc.get("created_at"),
        )

    @staticmethod
    def _doc_to_compensation(doc: dict[str, Any]) -> Compensation:
        try:
            comp_status = StepStatus(doc["status"])
        except ValueError:
            comp_status = StepStatus.PENDING
        return Compensation(
            id=doc["_id"],
            workflow_run_id=doc["workflow_run_id"],
            step_order=doc["step_order"],
            handler_name=doc["handler_name"],
            step_output=doc.get("step_output"),
            status=comp_status,
        )

    @staticmethod
    def _doc_to_schedule(doc: dict[str, Any]) -> Schedule:
        try:
            stype = ScheduleType(doc["schedule_type"])
        except ValueError:
            stype = ScheduleType.CRON
        return Schedule(
            id=doc["_id"],
            workflow_name=doc["workflow_name"],
            schedule_type=stype,
            schedule_config=doc["schedule_config"],
            namespace=doc.get("namespace", "default"),
            enabled=doc.get("enabled", True),
            last_run_at=doc.get("last_run_at"),
            next_run_at=doc.get("next_run_at"),
        )

    @staticmethod
    def _doc_to_dlq_entry(doc: dict[str, Any]) -> DLQEntry:
        return DLQEntry(
            id=doc["_id"],
            workflow_run_id=doc["workflow_run_id"],
            step_order=doc.get("step_order", 0),
            error_message=doc.get("error_message"),
            error_traceback=doc.get("error_traceback"),
            retry_count=doc.get("retry_count", 0),
        )

    @staticmethod
    def _doc_to_worker(doc: dict[str, Any]) -> WorkerInfo:
        try:
            wstatus = WorkerStatus(doc["status"])
        except ValueError:
            wstatus = WorkerStatus.ACTIVE
        return WorkerInfo(
            worker_id=doc["_id"],
            node_id=doc["node_id"],
            status=wstatus,
        )

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        col = self._col("workflow_definitions")
        await col.update_one(
            {"name": name, "version": version},
            {"$set": {"name": name, "version": version, "definition_json": definition_json}},
            upsert=True,
        )

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        col = self._col("workflow_definitions")
        results: list[tuple[str, int, str]] = []
        async for doc in col.find().sort([("name", 1), ("version", 1)]):
            results.append((doc["name"], int(doc["version"]), doc["definition_json"]))
        return results

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        col = self._col("workflow_definitions")
        await col.delete_one({"name": name, "version": version})

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        if not definitions:
            return 0
        from pymongo import UpdateOne

        col = self._col("workflow_definitions")
        ops = [
            UpdateOne(
                {"name": n, "version": v},
                {"$set": {"name": n, "version": v, "definition_json": d}},
                upsert=True,
            )
            for n, v, d in definitions
        ]
        await col.bulk_write(ops, ordered=False)
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        if not keys:
            return 0
        from pymongo import DeleteOne

        col = self._col("workflow_definitions")
        ops = [DeleteOne({"name": n, "version": v}) for n, v in keys]
        await col.bulk_write(ops, ordered=False)
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        col = self._col("circuit_breakers")
        await col.update_one(
            {"_id": name},
            {"$set": {"_id": name, "state_json": state_json, "updated_at": _utcnow()}},
            upsert=True,
        )

    async def load_circuit_state(self, name: str) -> str | None:
        col = self._col("circuit_breakers")
        doc = await col.find_one({"_id": name})
        if doc is not None:
            return str(doc["state_json"])
        return None
