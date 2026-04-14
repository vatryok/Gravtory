# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Redis backend using redis-py — full implementation with Lua scripts for atomicity."""

# mypy: disable-error-code="misc"
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

try:
    import redis.asyncio as aioredis
except ImportError as _exc:
    raise ImportError(
        "Redis backend requires redis[hiredis]. Install with: pip install gravtory[redis]"
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _dt_to_str(dt: datetime | None) -> str:
    return dt.isoformat() if dt else ""


def _str_to_dt(s: str | bytes | None) -> datetime | None:
    if not s:
        return None
    val = s.decode() if isinstance(s, bytes) else s
    if not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except (ValueError, TypeError):
        return None


def _to_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode()
    return str(val)


def _to_int(val: Any) -> int:
    if val is None:
        return 0
    if isinstance(val, bytes):
        val = val.decode()
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _to_bool(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, bytes):
        val = val.decode()
    return str(val).lower() in ("1", "true", "yes")


def _bytes_or_none(val: Any) -> bytes | None:
    """Return bytes, or None if empty/missing."""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val if val else None
    s = str(val)
    return s.encode() if s else None


# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

_CHECKPOINT_SCRIPT = """
local run_key = KEYS[1]
local step_key = KEYS[2]

if redis.call('EXISTS', step_key) == 1 then
    return 0
end

redis.call('HSET', step_key,
    'workflow_run_id', ARGV[1],
    'step_order', ARGV[2],
    'step_name', ARGV[3],
    'output_data', ARGV[4],
    'output_type', ARGV[5],
    'duration_ms', ARGV[6],
    'retry_count', ARGV[7],
    'status', ARGV[8],
    'error_message', ARGV[9],
    'created_at', ARGV[10])

redis.call('HSET', run_key, 'current_step', ARGV[2], 'updated_at', ARGV[10])

return 1
"""

_CLAIM_SCRIPT = """
local tasks = redis.call('ZRANGE', KEYS[1], 0, 0)
if #tasks == 0 then
    return nil
end

local task_id = tasks[1]
redis.call('ZREM', KEYS[1], task_id)

local task_key = KEYS[2] .. task_id
redis.call('HSET', task_key, 'status', 'running',
           'worker_id', ARGV[1], 'started_at', ARGV[2])

return task_id
"""

_CONSUME_SIGNAL_SCRIPT = """
local sigs = redis.call('ZRANGE', KEYS[1], 0, 0)
if #sigs == 0 then
    return nil
end

local sig_id = sigs[1]
redis.call('ZREM', KEYS[1], sig_id)

local sig_key = KEYS[2] .. sig_id
redis.call('HSET', sig_key, 'consumed', '1')

return sig_id
"""

_RELEASE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then
    return 0
end
if current == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""

_REFRESH_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then
    return 0
end
if current == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
"""

_CREATE_RUN_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
for i = 1, #ARGV, 2 do
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1])
end
return 1
"""

_CLAIM_WORKFLOW_RUN_SCRIPT = """
local key = KEYS[1]
local idx_old = KEYS[2]
local idx_new = KEYS[3]
local expected = ARGV[1]
local new_status = ARGV[2]
local updated_at = ARGV[3]
local run_id = ARGV[4]

local current = redis.call('HGET', key, 'status')
if current ~= expected then
    return 0
end
redis.call('HSET', key, 'status', new_status, 'updated_at', updated_at)
redis.call('SREM', idx_old, run_id)
redis.call('SADD', idx_new, run_id)
return 1
"""


class RedisBackend(Backend):
    """Redis backend using redis-py with Lua scripts for atomicity."""

    def __init__(
        self,
        dsn: str,
        *,
        key_prefix: str = "gravtory:",
        wfdef_ttl: int | None = None,
        circuit_ttl: int | None = 86400,
    ) -> None:
        """Initialize the Redis backend.

        Args:
            dsn: Redis connection string (e.g. ``redis://localhost:6379/0``).
            key_prefix: Prefix for all Redis keys.
            wfdef_ttl: TTL in seconds for workflow definition keys. ``None``
                means definitions persist indefinitely (recommended).

                .. warning::

                    Setting a finite ``wfdef_ttl`` will cause workflow
                    definitions to **silently expire** after the TTL elapses.
                    Unlike SQL backends which persist definitions indefinitely,
                    Redis will discard expired keys.  Only use this when
                    definitions are re-persisted on a schedule shorter than
                    the TTL, or when ephemeral definitions are acceptable.

            circuit_ttl: TTL in seconds for circuit breaker state keys.
                Defaults to 86400 (24 hours).
        """
        self._dsn = dsn
        self._prefix = key_prefix
        self._wfdef_ttl = wfdef_ttl
        self._circuit_ttl = circuit_ttl
        self._client: aioredis.Redis | None = None
        self._checkpoint_sha: str = ""
        self._claim_sha: str = ""
        self._consume_signal_sha: str = ""
        self._release_lock_sha: str = ""
        self._refresh_lock_sha: str = ""
        self._create_run_sha: str = ""

    @property
    def _p(self) -> str:
        return self._prefix

    @property
    def _r(self) -> aioredis.Redis:
        """Narrowed accessor for the Redis client (raises if not connected)."""
        if self._client is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("Redis", "Not connected. Call initialize() first.")
        return self._client

    # ── Lifecycle ─────────────────────────────────────────────────

    async def initialize(self) -> None:
        self._client = aioredis.from_url(self._dsn, decode_responses=False)
        await self._client.ping()
        self._checkpoint_sha = await self._client.script_load(_CHECKPOINT_SCRIPT)
        self._claim_sha = await self._client.script_load(_CLAIM_SCRIPT)
        self._consume_signal_sha = await self._client.script_load(_CONSUME_SIGNAL_SCRIPT)
        self._release_lock_sha = await self._client.script_load(_RELEASE_LOCK_SCRIPT)
        self._refresh_lock_sha = await self._client.script_load(_REFRESH_LOCK_SCRIPT)
        self._create_run_sha = await self._client.script_load(_CREATE_RUN_SCRIPT)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            return False

    def _ensure_connected(self) -> aioredis.Redis:
        """Return the active Redis client or raise BackendConnectionError."""
        if self._client is None:
            from gravtory.core.errors import BackendConnectionError

            raise BackendConnectionError("Redis", "Not connected. Call initialize() first.")
        return self._client

    # ── ID generation ─────────────────────────────────────────────

    async def _next_id(self, name: str) -> int:
        self._ensure_connected()
        return int(await self._r.incr(f"{self._p}counter:{name}"))

    # ── Workflow runs ─────────────────────────────────────────────

    async def create_workflow_run(self, run: WorkflowRun) -> None:
        self._ensure_connected()
        key = f"{self._p}run:{run.id}"
        status_val = run.status.value if isinstance(run.status, WorkflowStatus) else run.status
        now = _utcnow()
        # Flatten mapping into alternating key/value ARGV for Lua script
        fields: list[str | bytes] = [
            "id",
            run.id,
            "workflow_name",
            run.workflow_name,
            "workflow_version",
            str(run.workflow_version),
            "namespace",
            run.namespace,
            "status",
            status_val,
            "current_step",
            str(run.current_step or ""),
            "input_data",
            run.input_data or b"",
            "output_data",
            run.output_data or b"",
            "error_message",
            run.error_message or "",
            "error_traceback",
            run.error_traceback or "",
            "parent_run_id",
            run.parent_run_id or "",
            "created_at",
            _dt_to_str(run.created_at or now),
            "updated_at",
            _dt_to_str(run.updated_at or now),
            "completed_at",
            _dt_to_str(run.completed_at),
            "deadline_at",
            _dt_to_str(run.deadline_at),
        ]
        created = await self._r.evalsha(
            self._create_run_sha,
            1,
            key,
            *fields,
        )
        if created == 0:
            return  # Already exists — idempotent
        # Populate index sets (non-atomic but indexes are secondary)
        pipe = self._r.pipeline()
        pipe.sadd(f"{self._p}idx:status:{status_val}", run.id)
        pipe.sadd(f"{self._p}idx:workflow:{run.workflow_name}", run.id)
        pipe.sadd(f"{self._p}idx:ns:{run.namespace}", run.id)
        pipe.sadd(f"{self._p}idx:allruns", run.id)
        pipe.zadd(
            f"{self._p}idx:runs_by_time",
            {run.id: (run.created_at or now).timestamp()},
        )
        await pipe.execute()

    async def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        self._ensure_connected()
        data = await self._r.hgetall(f"{self._p}run:{run_id}")
        if not data:
            return None
        return self._hash_to_workflow_run(run_id, data)

    async def update_workflow_status(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        error_message: str | None = None,
        error_traceback: str | None = None,
        output_data: bytes | None = None,
    ) -> None:
        self._ensure_connected()
        key = f"{self._p}run:{run_id}"
        status_val = status.value if isinstance(status, WorkflowStatus) else status
        old_status_raw = await self._r.hget(key, "status")
        old_status = old_status_raw.decode() if old_status_raw else None

        terminal = (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.COMPENSATION_FAILED,
        )
        pipe = self._r.pipeline()
        pipe.hset(key, "status", status_val)
        pipe.hset(key, "updated_at", _dt_to_str(_utcnow()))
        if error_message is not None:
            pipe.hset(key, "error_message", error_message)
        if error_traceback is not None:
            pipe.hset(key, "error_traceback", error_traceback)
        if output_data is not None:
            pipe.hset(key, mapping={"output_data": output_data})
        if status in terminal:
            pipe.hset(key, "completed_at", _dt_to_str(_utcnow()))
        if old_status and old_status != status_val:
            pipe.srem(f"{self._p}idx:status:{old_status}", run_id)
        pipe.sadd(f"{self._p}idx:status:{status_val}", run_id)
        await pipe.execute()

    async def claim_workflow_run(
        self,
        run_id: str,
        expected_status: WorkflowStatus,
        new_status: WorkflowStatus,
    ) -> bool:
        self._ensure_connected()
        expected_val = (
            expected_status.value
            if isinstance(expected_status, WorkflowStatus)
            else expected_status
        )
        new_val = new_status.value if isinstance(new_status, WorkflowStatus) else new_status
        result = await self._r.eval(
            _CLAIM_WORKFLOW_RUN_SCRIPT,
            3,
            f"{self._p}run:{run_id}",
            f"{self._p}idx:status:{expected_val}",
            f"{self._p}idx:status:{new_val}",
            expected_val,
            new_val,
            _dt_to_str(_utcnow()),
            run_id,
        )
        return int(result) == 1

    async def list_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[WorkflowRun]:
        self._ensure_connected()
        sets_to_intersect: list[str] = [f"{self._p}idx:ns:{namespace}"]
        if status is not None:
            sv = status.value if isinstance(status, WorkflowStatus) else status
            sets_to_intersect.append(f"{self._p}idx:status:{sv}")
        if workflow_name is not None:
            sets_to_intersect.append(f"{self._p}idx:workflow:{workflow_name}")

        if len(sets_to_intersect) == 1:
            candidate_ids = await self._r.smembers(sets_to_intersect[0])
        else:
            candidate_ids = await self._r.sinter(sets_to_intersect)

        # Sort by created_at timestamp from the sorted set, not by string ID
        candidate_set = {r.decode() if isinstance(r, bytes) else r for r in candidate_ids}
        # Get all IDs with scores (timestamps) from the time-ordered index
        all_scored = await self._r.zrevrange(f"{self._p}idx:runs_by_time", 0, -1)
        # Filter to only those in our candidate set, preserving time order
        ordered_ids = [
            rid.decode() if isinstance(rid, bytes) else rid
            for rid in all_scored
            if (rid.decode() if isinstance(rid, bytes) else rid) in candidate_set
        ]
        ordered_ids = ordered_ids[offset : offset + limit]

        runs: list[WorkflowRun] = []
        for rid in ordered_ids:
            run = await self.get_workflow_run(rid)
            if run is not None:
                runs.append(run)
        return runs

    # ── Step outputs ──────────────────────────────────────────────

    async def save_step_output(self, output: StepOutput) -> None:
        self._ensure_connected()
        out_data = output.output_data
        if out_data is not None and not isinstance(out_data, (bytes, memoryview)):
            out_data = None
        status_val = output.status.value if isinstance(output.status, StepStatus) else output.status
        now_str = _dt_to_str(_utcnow())

        run_key = f"{self._p}run:{output.workflow_run_id}"
        step_key = f"{self._p}step:{output.workflow_run_id}:{output.step_order}"

        await self._r.evalsha(
            self._checkpoint_sha,
            2,
            run_key,
            step_key,
            output.workflow_run_id,
            str(output.step_order),
            output.step_name,
            out_data or b"",
            output.output_type or "",
            str(output.duration_ms or 0),
            str(output.retry_count),
            status_val,
            output.error_message or "",
            now_str,
        )
        await self._r.sadd(
            f"{self._p}steps:{output.workflow_run_id}",
            str(output.step_order),
        )

    async def get_step_outputs(self, run_id: str) -> Sequence[StepOutput]:
        self._ensure_connected()
        members = await self._r.smembers(f"{self._p}steps:{run_id}")
        orders = sorted(_to_int(s) for s in members)
        results: list[StepOutput] = []
        for so in orders:
            output = await self.get_step_output(run_id, so)
            if output is not None:
                results.append(output)
        return results

    async def get_step_output(self, run_id: str, step_order: int) -> StepOutput | None:
        self._ensure_connected()
        data = await self._r.hgetall(f"{self._p}step:{run_id}:{step_order}")
        if not data:
            return None
        return self._hash_to_step_output(data)

    async def update_step_output(self, run_id: str, step_order: int, output_data: bytes) -> None:
        self._ensure_connected()
        key = f"{self._p}step:{run_id}:{step_order}"
        exists = await self._r.exists(key)
        if not exists:
            from gravtory.core.errors import BackendError

            raise BackendError(
                f"Step output not found for run_id={run_id!r}, step_order={step_order}"
            )
        await self._r.hset(key, "output_data", output_data)  # type: ignore[arg-type]

    # ── Pending steps ─────────────────────────────────────────────

    async def enqueue_step(self, step: PendingStep) -> None:
        self._ensure_connected()
        step_id = await self._next_id("pending_steps")
        key = f"{self._p}pending:{step_id}"
        now = _utcnow()
        mapping: dict[str, str] = {
            "id": str(step_id),
            "workflow_run_id": step.workflow_run_id,
            "step_order": str(step.step_order),
            "priority": str(step.priority),
            "status": "pending",
            "worker_id": "",
            "scheduled_at": _dt_to_str(now),
            "started_at": "",
            "completed_at": "",
            "retry_count": "0",
            "max_retries": str(step.max_retries),
            "next_retry_at": "",
            "created_at": _dt_to_str(now),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        score = -step.priority * 1_000_000 + now.timestamp()
        pipe.zadd(f"{self._p}pending_queue", {str(step_id): score})
        await pipe.execute()

    async def claim_step(self, worker_id: str) -> PendingStep | None:
        self._ensure_connected()
        now_str = _dt_to_str(_utcnow())
        result = await self._r.evalsha(
            self._claim_sha,
            2,
            f"{self._p}pending_queue",
            f"{self._p}pending:",
            worker_id,
            now_str,
        )
        if result is None:
            return None
        task_id = result.decode() if isinstance(result, bytes) else str(result)
        data = await self._r.hgetall(f"{self._p}pending:{task_id}")
        if not data:
            return None
        return self._hash_to_pending_step(data)

    async def complete_step(self, step_id: int, output: StepOutput) -> None:
        self._ensure_connected()
        key = f"{self._p}pending:{step_id}"
        pipe = self._r.pipeline()
        pipe.hset(key, "status", "completed")
        pipe.hset(key, "completed_at", _dt_to_str(_utcnow()))
        await pipe.execute()
        await self.save_step_output(output)

    async def fail_step(
        self,
        step_id: int,
        *,
        error_message: str,
        retry_at: Any | None = None,
    ) -> None:
        self._ensure_connected()
        key = f"{self._p}pending:{step_id}"
        if retry_at is not None:
            retry_str = _dt_to_str(retry_at) if hasattr(retry_at, "isoformat") else str(retry_at)
            # Use retry_at as the score so the task isn't claimed before its scheduled time
            retry_ts = (
                retry_at.timestamp() if hasattr(retry_at, "timestamp") else _utcnow().timestamp()
            )
            pipe = self._r.pipeline()
            pipe.hset(key, "status", "pending")
            pipe.hincrby(key, "retry_count", 1)
            pipe.hset(key, "next_retry_at", retry_str)
            pipe.hset(key, "scheduled_at", retry_str)
            score = -_to_int(await self._r.hget(key, "priority")) * 1_000_000 + retry_ts
            pipe.zadd(f"{self._p}pending_queue", {str(step_id): score})
            await pipe.execute()
        else:
            await self._r.hset(key, "status", "failed")

    # ── Signals ───────────────────────────────────────────────────

    async def send_signal(self, signal: Signal) -> None:
        self._ensure_connected()
        sig_id = await self._next_id("signals")
        key = f"{self._p}signal:{sig_id}"
        now = _utcnow()
        mapping: dict[str, str | bytes] = {
            "id": str(sig_id),
            "workflow_run_id": signal.workflow_run_id,
            "signal_name": signal.signal_name,
            "signal_data": signal.signal_data or b"",
            "consumed": "0",
            "created_at": _dt_to_str(now),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        idx_key = f"{self._p}sigidx:{signal.workflow_run_id}:{signal.signal_name}"
        pipe.zadd(idx_key, {str(sig_id): now.timestamp()})
        await pipe.execute()

    async def consume_signal(self, run_id: str, signal_name: str) -> Signal | None:
        self._ensure_connected()
        idx_key = f"{self._p}sigidx:{run_id}:{signal_name}"
        prefix = f"{self._p}signal:"
        result = await self._r.evalsha(
            self._consume_signal_sha,
            2,
            idx_key,
            prefix,
        )
        if result is None:
            return None
        sig_id = result.decode() if isinstance(result, bytes) else str(result)
        data = await self._r.hgetall(f"{self._p}signal:{sig_id}")
        if not data:
            return None
        return self._hash_to_signal(data)

    async def register_signal_wait(self, wait: SignalWait) -> None:
        self._ensure_connected()
        sw_id = await self._next_id("signal_waits")
        key = f"{self._p}sigwait:{sw_id}"
        mapping: dict[str, str] = {
            "id": str(sw_id),
            "workflow_run_id": wait.workflow_run_id,
            "signal_name": wait.signal_name,
            "timeout_at": _dt_to_str(wait.timeout_at),
            "created_at": _dt_to_str(_utcnow()),
        }
        await self._r.hset(key, mapping=mapping)

    # ── Compensation ──────────────────────────────────────────────

    async def save_compensation(self, comp: Compensation) -> None:
        self._ensure_connected()
        comp_id = await self._next_id("compensations")
        key = f"{self._p}comp:{comp_id}"
        status_str = (
            comp.status
            if isinstance(comp.status, str)
            else comp.status.value
            if hasattr(comp.status, "value")
            else str(comp.status)
        )
        mapping: dict[str, str | bytes] = {
            "id": str(comp_id),
            "workflow_run_id": comp.workflow_run_id,
            "step_order": str(comp.step_order),
            "handler_name": comp.handler_name,
            "step_output": comp.step_output or b"",
            "status": status_str,
            "error_message": "",
            "created_at": _dt_to_str(_utcnow()),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.sadd(f"{self._p}compidx:{comp.workflow_run_id}", str(comp_id))
        await pipe.execute()

    async def get_compensations(self, run_id: str) -> Sequence[Compensation]:
        self._ensure_connected()
        ids_raw = await self._r.smembers(f"{self._p}compidx:{run_id}")
        results: list[Compensation] = []
        for cid_raw in ids_raw:
            cid = cid_raw.decode() if isinstance(cid_raw, bytes) else str(cid_raw)
            data = await self._r.hgetall(f"{self._p}comp:{cid}")
            if data:
                results.append(self._hash_to_compensation(data))
        return sorted(results, key=lambda c: c.step_order, reverse=True)

    async def update_compensation_status(
        self,
        compensation_id: int,
        status: str,
        *,
        error_message: str | None = None,
    ) -> None:
        self._ensure_connected()
        key = f"{self._p}comp:{compensation_id}"
        pipe = self._r.pipeline()
        pipe.hset(key, "status", status)
        if error_message is not None:
            pipe.hset(key, "error_message", error_message)
        await pipe.execute()

    # ── Scheduling ────────────────────────────────────────────────

    async def save_schedule(self, schedule: Schedule) -> None:
        self._ensure_connected()
        key = f"{self._p}schedule:{schedule.id}"
        stype = (
            schedule.schedule_type
            if isinstance(schedule.schedule_type, str)
            else schedule.schedule_type.value
            if hasattr(schedule.schedule_type, "value")
            else str(schedule.schedule_type)
        )
        mapping: dict[str, str] = {
            "id": schedule.id,
            "workflow_name": schedule.workflow_name,
            "schedule_type": stype,
            "schedule_config": schedule.schedule_config,
            "namespace": schedule.namespace,
            "enabled": "1" if schedule.enabled else "0",
            "last_run_at": _dt_to_str(schedule.last_run_at),
            "next_run_at": _dt_to_str(schedule.next_run_at),
            "created_at": _dt_to_str(_utcnow()),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.sadd(f"{self._p}idx:allschedules", schedule.id)
        await pipe.execute()

    async def get_due_schedules(self) -> Sequence[Schedule]:
        self._ensure_connected()
        all_ids = await self._r.smembers(f"{self._p}idx:allschedules")
        now = _utcnow()
        results: list[Schedule] = []
        for sid_raw in all_ids:
            sid = sid_raw.decode() if isinstance(sid_raw, bytes) else str(sid_raw)
            data = await self._r.hgetall(f"{self._p}schedule:{sid}")
            if not data:
                continue
            sched = self._hash_to_schedule(data)
            if sched.enabled and sched.next_run_at is not None and sched.next_run_at <= now:
                results.append(sched)
        return results

    async def update_schedule_last_run(
        self, schedule_id: str, last_run_at: Any, next_run_at: Any
    ) -> None:
        self._ensure_connected()
        key = f"{self._p}schedule:{schedule_id}"
        lr = _dt_to_str(last_run_at) if hasattr(last_run_at, "isoformat") else str(last_run_at)
        nr = _dt_to_str(next_run_at) if hasattr(next_run_at, "isoformat") else str(next_run_at)
        pipe = self._r.pipeline()
        pipe.hset(key, "last_run_at", lr)
        pipe.hset(key, "next_run_at", nr)
        await pipe.execute()

    async def get_all_enabled_schedules(self) -> Sequence[Schedule]:
        self._ensure_connected()
        all_ids = await self._r.smembers(f"{self._p}idx:allschedules")
        results: list[Schedule] = []
        for sid_raw in all_ids:
            sid = sid_raw.decode() if isinstance(sid_raw, bytes) else str(sid_raw)
            data = await self._r.hgetall(f"{self._p}schedule:{sid}")
            if not data:
                continue
            sched = self._hash_to_schedule(data)
            if sched.enabled:
                results.append(sched)
        return results

    async def list_all_schedules(self) -> Sequence[Schedule]:
        self._ensure_connected()
        all_ids = await self._r.smembers(f"{self._p}idx:allschedules")
        results: list[Schedule] = []
        for sid_raw in all_ids:
            sid = sid_raw.decode() if isinstance(sid_raw, bytes) else str(sid_raw)
            data = await self._r.hgetall(f"{self._p}schedule:{sid}")
            if not data:
                continue
            results.append(self._hash_to_schedule(data))
        return results

    # ── Locks ─────────────────────────────────────────────────────

    async def acquire_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        self._ensure_connected()
        key = f"{self._p}lock:{lock_name}"
        result = await self._r.set(key, holder_id.encode(), nx=True, ex=ttl_seconds)
        if result is not None:
            return True
        # Check if we already hold it
        current = await self._r.get(key)
        if current is not None:
            current_str = current.decode() if isinstance(current, bytes) else str(current)
            if current_str == holder_id:
                await self._r.expire(key, ttl_seconds)
                return True
        return False

    async def release_lock(self, lock_name: str, holder_id: str) -> bool:
        self._ensure_connected()
        key = f"{self._p}lock:{lock_name}"
        result = await self._r.evalsha(
            self._release_lock_sha,
            1,
            key,
            holder_id,
        )
        return int(result) == 1

    async def refresh_lock(self, lock_name: str, holder_id: str, ttl_seconds: int) -> bool:
        self._ensure_connected()
        key = f"{self._p}lock:{lock_name}"
        result = await self._r.evalsha(
            self._refresh_lock_sha,
            1,
            key,
            holder_id,
            str(ttl_seconds),
        )
        return int(result) == 1

    # ── DLQ ───────────────────────────────────────────────────────

    async def add_to_dlq(self, entry: DLQEntry) -> None:
        self._ensure_connected()
        dlq_id = await self._next_id("dlq")
        key = f"{self._p}dlq:{dlq_id}"
        now = _utcnow()
        mapping: dict[str, str] = {
            "id": str(dlq_id),
            "workflow_run_id": entry.workflow_run_id,
            "step_order": str(entry.step_order),
            "error_message": entry.error_message or "",
            "error_traceback": entry.error_traceback or "",
            "retry_count": str(entry.retry_count),
            "created_at": _dt_to_str(now),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.zadd(f"{self._p}idx:dlq", {str(dlq_id): now.timestamp()})
        await pipe.execute()

    async def list_dlq(self, *, namespace: str = "default", limit: int = 100) -> Sequence[DLQEntry]:
        self._ensure_connected()
        ids_raw = await self._r.zrevrange(f"{self._p}idx:dlq", 0, limit - 1)
        results: list[DLQEntry] = []
        for did_raw in ids_raw:
            did = did_raw.decode() if isinstance(did_raw, bytes) else str(did_raw)
            data = await self._r.hgetall(f"{self._p}dlq:{did}")
            if data:
                results.append(self._hash_to_dlq_entry(data))
        return results

    async def get_dlq_entry(self, entry_id: int) -> DLQEntry | None:
        self._ensure_connected()
        data = await self._r.hgetall(f"{self._p}dlq:{entry_id}")
        if not data:
            return None
        return self._hash_to_dlq_entry(data)

    async def count_dlq(self, *, namespace: str = "default") -> int:
        self._ensure_connected()
        return int(await self._r.zcard(f"{self._p}idx:dlq"))

    async def remove_from_dlq(self, entry_id: int) -> None:
        self._ensure_connected()
        pipe = self._r.pipeline()
        pipe.delete(f"{self._p}dlq:{entry_id}")
        pipe.zrem(f"{self._p}idx:dlq", str(entry_id))
        await pipe.execute()

    # ── Workers ───────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo) -> None:
        self._ensure_connected()
        status_val = (
            worker.status.value if isinstance(worker.status, WorkerStatus) else worker.status
        )
        key = f"{self._p}worker:{worker.worker_id}"
        mapping: dict[str, str] = {
            "worker_id": worker.worker_id,
            "node_id": worker.node_id,
            "status": status_val,
            "last_heartbeat": _dt_to_str(_utcnow()),
            "current_task": worker.current_task or "",
            "started_at": _dt_to_str(_utcnow()),
        }
        pipe = self._r.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.sadd(f"{self._p}idx:workers", worker.worker_id)
        await pipe.execute()

    async def worker_heartbeat(
        self,
        worker_id: str,
        current_task: str | None = None,
    ) -> None:
        self._ensure_connected()
        mapping: dict[str, str] = {"last_heartbeat": _dt_to_str(_utcnow())}
        if current_task is not None:
            mapping["current_task"] = current_task
        await self._r.hset(f"{self._p}worker:{worker_id}", mapping=mapping)

    async def deregister_worker(self, worker_id: str) -> None:
        self._ensure_connected()
        pipe = self._r.pipeline()
        pipe.delete(f"{self._p}worker:{worker_id}")
        pipe.srem(f"{self._p}idx:workers", worker_id)
        await pipe.execute()

    async def list_workers(self) -> Sequence[WorkerInfo]:
        self._ensure_connected()
        ids_raw = await self._r.smembers(f"{self._p}idx:workers")
        results: list[WorkerInfo] = []
        for wid_raw in ids_raw:
            wid = wid_raw.decode() if isinstance(wid_raw, bytes) else str(wid_raw)
            data = await self._r.hgetall(f"{self._p}worker:{wid}")
            if data:
                results.append(self._hash_to_worker(data))
        return results

    async def get_stale_workers(
        self,
        stale_threshold_seconds: int,
    ) -> Sequence[WorkerInfo]:
        self._ensure_connected()
        from datetime import timedelta

        cutoff = _utcnow() - timedelta(seconds=stale_threshold_seconds)
        ids_raw = await self._r.smembers(f"{self._p}idx:workers")
        results: list[WorkerInfo] = []
        for wid_raw in ids_raw:
            wid = wid_raw.decode() if isinstance(wid_raw, bytes) else str(wid_raw)
            data = await self._r.hgetall(f"{self._p}worker:{wid}")
            if data:
                d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
                hb = _str_to_dt(d.get("last_heartbeat"))
                if hb is None or hb < cutoff:
                    results.append(self._hash_to_worker(data))
        return results

    # ── Workflow run extras ────────────────────────────────────────

    async def count_workflow_runs(
        self,
        *,
        namespace: str = "default",
        status: WorkflowStatus | None = None,
        workflow_name: str | None = None,
    ) -> int:
        self._ensure_connected()
        ns_ids = await self._r.smembers(f"{self._p}idx:ns:{namespace}")
        candidates = ns_ids
        if status is not None:
            status_ids = await self._r.smembers(f"{self._p}idx:status:{status.value}")
            candidates = candidates & status_ids
        if workflow_name is not None:
            wf_ids = await self._r.smembers(f"{self._p}idx:workflow:{workflow_name}")
            candidates = candidates & wf_ids
        return len(candidates)

    async def get_incomplete_runs(self) -> Sequence[WorkflowRun]:
        self._ensure_connected()
        running_ids = await self._r.smembers(f"{self._p}idx:status:running")
        pending_ids = await self._r.smembers(f"{self._p}idx:status:pending")
        all_ids = running_ids | pending_ids
        results: list[WorkflowRun] = []
        for rid_raw in all_ids:
            rid = rid_raw.decode() if isinstance(rid_raw, bytes) else str(rid_raw)
            data = await self._r.hgetall(f"{self._p}run:{rid}")
            if data:
                results.append(self._hash_to_workflow_run(rid, data))
        return results

    # ── Parallel step results ──────────────────────────────────────

    async def checkpoint_parallel_item(
        self,
        run_id: str,
        step_order: int,
        item_index: int,
        output_data: bytes,
    ) -> None:
        self._ensure_connected()
        key = f"{self._p}parallel:{run_id}:{step_order}:{item_index}"
        await self._r.set(key, output_data)
        await self._r.sadd(f"{self._p}idx:parallel:{run_id}:{step_order}", str(item_index))

    async def get_parallel_results(
        self,
        run_id: str,
        step_order: int,
    ) -> dict[int, bytes]:
        self._ensure_connected()
        idx_key = f"{self._p}idx:parallel:{run_id}:{step_order}"
        indices = await self._r.smembers(idx_key)
        results: dict[int, bytes] = {}
        for idx_raw in indices:
            idx = int(idx_raw.decode() if isinstance(idx_raw, bytes) else str(idx_raw))
            data = await self._r.get(f"{self._p}parallel:{run_id}:{step_order}:{idx}")
            if data is not None:
                results[idx] = data if isinstance(data, bytes) else data.encode()
        return results

    # ── DLQ extras ─────────────────────────────────────────────────

    async def purge_dlq(self, *, namespace: str = "default") -> int:
        self._ensure_connected()
        ids_raw = await self._r.smembers(f"{self._p}idx:dlq")
        count = len(ids_raw)
        if count > 0:
            pipe = self._r.pipeline()
            for did_raw in ids_raw:
                did = did_raw.decode() if isinstance(did_raw, bytes) else str(did_raw)
                pipe.delete(f"{self._p}dlq:{did}")
            pipe.delete(f"{self._p}idx:dlq")
            await pipe.execute()
        return count

    # ── Task reclamation ───────────────────────────────────────────

    async def reclaim_worker_tasks(self, worker_id: str) -> int:
        self._ensure_connected()
        keys = await self._r.keys(f"{self._p}pending:*")
        reclaimed = 0
        for key in keys:
            data = await self._r.hgetall(key)
            if not data:
                continue
            w_id = data.get(b"worker_id") or data.get("worker_id")
            status_val = data.get(b"status") or data.get("status")
            if isinstance(w_id, bytes):
                w_id = w_id.decode()
            if isinstance(status_val, bytes):
                status_val = status_val.decode()
            if w_id == worker_id and status_val == "running":
                await self._r.hset(
                    key,
                    mapping={
                        "status": "pending",
                        "worker_id": "",
                        "started_at": "",
                    },
                )
                reclaimed += 1
        return reclaimed

    # ── Concurrency control ───────────────────────────────────────

    async def check_concurrency_limit(
        self,
        workflow_name: str,
        namespace: str,
        max_concurrent: int,
    ) -> bool:
        self._ensure_connected()
        workflow_ids = await self._r.smembers(f"{self._p}idx:workflow:{workflow_name}")
        ns_ids = await self._r.smembers(f"{self._p}idx:ns:{namespace}")
        running_ids = await self._r.smembers(f"{self._p}idx:status:running")
        pending_ids = await self._r.smembers(f"{self._p}idx:status:pending")
        active = (running_ids | pending_ids) & workflow_ids & ns_ids
        return len(active) < max_concurrent

    # ── Hash mappers ──────────────────────────────────────────────

    @staticmethod
    def _hash_to_workflow_run(run_id: str, data: dict[bytes | str, bytes | str]) -> WorkflowRun:
        d: dict[str, str] = {(_to_str(k)): _to_str(v) for k, v in data.items()}
        return WorkflowRun(
            id=run_id,
            workflow_name=d.get("workflow_name", ""),
            workflow_version=_to_int(d.get("workflow_version", "1")),
            namespace=d.get("namespace", "default"),
            status=WorkflowStatus(d.get("status", "pending")),
            current_step=_to_int(d.get("current_step")) or None,
            input_data=_bytes_or_none(data.get(b"input_data", data.get("input_data"))),
            error_message=d.get("error_message") or None,
            error_traceback=d.get("error_traceback") or None,
            parent_run_id=d.get("parent_run_id") or None,
            created_at=_str_to_dt(d.get("created_at")),
            updated_at=_str_to_dt(d.get("updated_at")),
            completed_at=_str_to_dt(d.get("completed_at")),
            deadline_at=_str_to_dt(d.get("deadline_at")),
        )

    @staticmethod
    def _hash_to_step_output(data: dict[bytes | str, bytes | str]) -> StepOutput:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        return StepOutput(
            workflow_run_id=d.get("workflow_run_id", ""),
            step_order=_to_int(d.get("step_order")),
            step_name=d.get("step_name", ""),
            output_data=_bytes_or_none(data.get(b"output_data", data.get("output_data"))),
            output_type=d.get("output_type") or None,
            duration_ms=_to_int(d.get("duration_ms")),
            retry_count=_to_int(d.get("retry_count")),
            status=StepStatus(d.get("status", "completed")),
            error_message=d.get("error_message") or None,
            created_at=_str_to_dt(d.get("created_at")),
        )

    @staticmethod
    def _hash_to_pending_step(data: dict[bytes | str, bytes | str]) -> PendingStep:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        try:
            step_status = StepStatus(d.get("status", "pending"))
        except ValueError:
            step_status = StepStatus.PENDING
        return PendingStep(
            id=_to_int(d.get("id")),
            workflow_run_id=d.get("workflow_run_id", ""),
            step_order=_to_int(d.get("step_order")),
            priority=_to_int(d.get("priority")),
            status=step_status,
            worker_id=d.get("worker_id") or None,
            retry_count=_to_int(d.get("retry_count")),
            max_retries=_to_int(d.get("max_retries")),
        )

    @staticmethod
    def _hash_to_signal(data: dict[bytes | str, bytes | str]) -> Signal:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        return Signal(
            id=_to_int(d.get("id")),
            workflow_run_id=d.get("workflow_run_id", ""),
            signal_name=d.get("signal_name", ""),
            signal_data=_bytes_or_none(data.get(b"signal_data", data.get("signal_data"))),
            consumed=_to_bool(d.get("consumed")),
            created_at=_str_to_dt(d.get("created_at")),
        )

    @staticmethod
    def _hash_to_compensation(data: dict[bytes | str, bytes | str]) -> Compensation:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        try:
            comp_status = StepStatus(d.get("status", "pending"))
        except ValueError:
            comp_status = StepStatus.PENDING
        return Compensation(
            id=_to_int(d.get("id")),
            workflow_run_id=d.get("workflow_run_id", ""),
            step_order=_to_int(d.get("step_order")),
            handler_name=d.get("handler_name", ""),
            step_output=_bytes_or_none(data.get(b"step_output", data.get("step_output"))),
            status=comp_status,
        )

    @staticmethod
    def _hash_to_schedule(data: dict[bytes | str, bytes | str]) -> Schedule:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        try:
            stype = ScheduleType(d.get("schedule_type", "cron"))
        except ValueError:
            stype = ScheduleType.CRON
        return Schedule(
            id=d.get("id", ""),
            workflow_name=d.get("workflow_name", ""),
            schedule_type=stype,
            schedule_config=d.get("schedule_config", ""),
            namespace=d.get("namespace", "default"),
            enabled=_to_bool(d.get("enabled")),
            last_run_at=_str_to_dt(d.get("last_run_at")),
            next_run_at=_str_to_dt(d.get("next_run_at")),
        )

    @staticmethod
    def _hash_to_dlq_entry(data: dict[bytes | str, bytes | str]) -> DLQEntry:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        return DLQEntry(
            id=_to_int(d.get("id")),
            workflow_run_id=d.get("workflow_run_id", ""),
            step_order=_to_int(d.get("step_order")),
            error_message=d.get("error_message") or None,
            error_traceback=d.get("error_traceback") or None,
            retry_count=_to_int(d.get("retry_count")),
        )

    # ── Dynamic workflow persistence ──────────────────────────────

    async def save_workflow_definition(self, name: str, version: int, definition_json: str) -> None:
        self._ensure_connected()
        key = f"{self._p}wfdef:{name}:{version}"
        await self._r.hset(
            key,
            mapping={
                "name": name,
                "version": str(version),
                "definition_json": definition_json,
            },
        )
        if self._wfdef_ttl is not None:
            await self._r.expire(key, self._wfdef_ttl)
        await self._r.sadd(f"{self._p}idx:wfdef", f"{name}:{version}")

    async def load_workflow_definitions(self) -> list[tuple[str, int, str]]:
        self._ensure_connected()
        ids_raw = await self._r.smembers(f"{self._p}idx:wfdef")
        results: list[tuple[str, int, str]] = []
        for entry_raw in ids_raw:
            entry = entry_raw.decode() if isinstance(entry_raw, bytes) else str(entry_raw)
            data = await self._r.hgetall(f"{self._p}wfdef:{entry}")
            if data:
                d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
                results.append(
                    (d.get("name", ""), int(d.get("version", "1")), d.get("definition_json", ""))
                )
        return sorted(results, key=lambda t: (t[0], t[1]))

    async def delete_workflow_definition(self, name: str, version: int) -> None:
        self._ensure_connected()
        key = f"{self._p}wfdef:{name}:{version}"
        await self._r.delete(key)
        await self._r.srem(f"{self._p}idx:wfdef", f"{name}:{version}")

    async def save_workflow_definitions_batch(
        self,
        definitions: list[tuple[str, int, str]],
    ) -> int:
        if not definitions:
            return 0
        self._ensure_connected()
        async with self._r.pipeline(transaction=True) as pipe:
            for n, v, d in definitions:
                key = f"{self._p}wfdef:{n}:{v}"
                pipe.hset(key, mapping={"name": n, "version": str(v), "definition_json": d})
                if self._wfdef_ttl is not None:
                    pipe.expire(key, self._wfdef_ttl)
                pipe.sadd(f"{self._p}idx:wfdef", f"{n}:{v}")
            await pipe.execute()
        return len(definitions)

    async def delete_workflow_definitions_batch(
        self,
        keys: list[tuple[str, int]],
    ) -> int:
        if not keys:
            return 0
        self._ensure_connected()
        async with self._r.pipeline(transaction=True) as pipe:
            for n, v in keys:
                key = f"{self._p}wfdef:{n}:{v}"
                pipe.delete(key)
                pipe.srem(f"{self._p}idx:wfdef", f"{n}:{v}")
            await pipe.execute()
        return len(keys)

    # ── Circuit breaker state ──────────────────────────────────────

    async def save_circuit_state(self, name: str, state_json: str) -> None:
        self._ensure_connected()
        key = f"{self._p}circuit:{name}"
        if self._circuit_ttl is not None:
            await self._r.set(key, state_json, ex=self._circuit_ttl)
        else:
            await self._r.set(key, state_json)

    async def load_circuit_state(self, name: str) -> str | None:
        self._ensure_connected()
        key = f"{self._p}circuit:{name}"
        val = await self._r.get(key)
        if val is None:
            return None
        return val.decode() if isinstance(val, bytes) else str(val)

    # ── Hash mappers ──────────────────────────────────────────────

    @staticmethod
    def _hash_to_worker(data: dict[bytes | str, bytes | str]) -> WorkerInfo:
        d: dict[str, str] = {_to_str(k): _to_str(v) for k, v in data.items()}
        try:
            wstatus = WorkerStatus(d.get("status", "active"))
        except ValueError:
            wstatus = WorkerStatus.ACTIVE
        return WorkerInfo(
            worker_id=d.get("worker_id", ""),
            node_id=d.get("node_id", ""),
            status=wstatus,
        )
