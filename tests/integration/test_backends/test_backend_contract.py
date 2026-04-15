"""Backend contract tests — verify all backends implement identical behavior.

These tests ensure that every Backend implementation (InMemory, SQLite,
PostgreSQL, MySQL, MongoDB, Redis) produces the same observable results
for the same sequence of operations.  Any new Backend must pass all of
these tests to be considered conformant.

The ``backend`` fixture is parameterized in conftest.py and automatically
runs each test against every available backend.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.core.types import (
    DLQEntry,
    PendingStep,
    Signal,
    StepOutput,
    StepStatus,
    WorkflowRun,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ── Contract: Workflow lifecycle ────────────────────────────────────


class TestWorkflowLifecycleContract:
    """Every backend MUST support the full workflow CRUD lifecycle."""

    async def test_create_get_round_trip(self, backend: Backend) -> None:
        run = WorkflowRun(id="contract-1", workflow_name="WF")
        await backend.create_workflow_run(run)
        got = await backend.get_workflow_run("contract-1")
        assert got is not None
        assert got.id == "contract-1"
        assert got.workflow_name == "WF"
        assert got.status == WorkflowStatus.PENDING

    async def test_create_is_idempotent(self, backend: Backend) -> None:
        run = WorkflowRun(id="contract-idem", workflow_name="WF")
        await backend.create_workflow_run(run)
        await backend.create_workflow_run(run)  # no error
        got = await backend.get_workflow_run("contract-idem")
        assert got is not None

    async def test_status_transition(self, backend: Backend) -> None:
        run = WorkflowRun(id="contract-trans", workflow_name="WF")
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("contract-trans", WorkflowStatus.RUNNING)
        got = await backend.get_workflow_run("contract-trans")
        assert got is not None
        assert got.status == WorkflowStatus.RUNNING

    async def test_list_by_status(self, backend: Backend) -> None:
        for i in range(3):
            await backend.create_workflow_run(WorkflowRun(id=f"list-{i}", workflow_name="WF"))
        await backend.update_workflow_status("list-1", WorkflowStatus.RUNNING)
        pending = await backend.list_workflow_runs(status=WorkflowStatus.PENDING)
        ids = {r.id for r in pending}
        assert "list-0" in ids
        assert "list-2" in ids
        assert "list-1" not in ids


# ── Contract: Step output checkpointing ────────────────────────────


class TestStepOutputContract:
    """Every backend MUST persist and retrieve step outputs identically."""

    async def test_save_and_get(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="so-1", workflow_name="WF"))
        output = StepOutput(
            workflow_run_id="so-1",
            step_order=1,
            step_name="step_a",
            output_data=b'{"result": 42}',
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(output)
        got = await backend.get_step_output("so-1", 1)
        assert got is not None
        assert got.output_data == b'{"result": 42}'
        assert got.step_name == "step_a"

    async def test_save_is_idempotent(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="so-idem", workflow_name="WF"))
        output = StepOutput(
            workflow_run_id="so-idem",
            step_order=1,
            step_name="step_a",
            output_data=b"first",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(output)
        # Second save with different data should NOT overwrite
        output2 = StepOutput(
            workflow_run_id="so-idem",
            step_order=1,
            step_name="step_a",
            output_data=b"second",
            status=StepStatus.COMPLETED,
        )
        await backend.save_step_output(output2)
        got = await backend.get_step_output("so-idem", 1)
        assert got is not None
        assert got.output_data == b"first"

    async def test_get_all_outputs_ordered(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="so-ord", workflow_name="WF"))
        for order in [3, 1, 2]:
            await backend.save_step_output(
                StepOutput(
                    workflow_run_id="so-ord",
                    step_order=order,
                    step_name=f"step_{order}",
                    output_data=f"out-{order}".encode(),
                    status=StepStatus.COMPLETED,
                )
            )
        outputs = await backend.get_step_outputs("so-ord")
        assert [o.step_order for o in outputs] == [1, 2, 3]


# ── Contract: Task queue (pending steps) ───────────────────────────


class TestTaskQueueContract:
    """Every backend MUST support enqueue, claim, complete, fail."""

    async def test_enqueue_and_claim(self, backend: Backend) -> None:
        step = PendingStep(
            workflow_run_id="tq-1",
            step_order=1,
        )
        await backend.enqueue_step(step)
        claimed = await backend.claim_step("worker-1")
        assert claimed is not None
        assert claimed.workflow_run_id == "tq-1"
        assert claimed.status == StepStatus.RUNNING

    async def test_claim_returns_none_when_empty(self, backend: Backend) -> None:
        claimed = await backend.claim_step("worker-empty")
        assert claimed is None

    async def test_complete_step(self, backend: Backend) -> None:
        await backend.create_workflow_run(WorkflowRun(id="tq-comp", workflow_name="WF"))
        step = PendingStep(workflow_run_id="tq-comp", step_order=1)
        await backend.enqueue_step(step)
        claimed = await backend.claim_step("worker-2")
        assert claimed is not None
        output = StepOutput(
            workflow_run_id="tq-comp",
            step_order=1,
            step_name="step_a",
            output_data=b"done",
            status=StepStatus.COMPLETED,
        )
        await backend.complete_step(claimed.id, output)


# ── Contract: Distributed locking ──────────────────────────────────


class TestLockingContract:
    """Every backend MUST support acquire/release/refresh locks."""

    async def test_acquire_and_release(self, backend: Backend) -> None:
        acquired = await backend.acquire_lock("test-lock", "node-1", ttl_seconds=30)
        assert acquired is True
        await backend.release_lock("test-lock", "node-1")

    async def test_acquire_blocks_other_nodes(self, backend: Backend) -> None:
        await backend.acquire_lock("excl-lock", "node-1", ttl_seconds=30)
        acquired = await backend.acquire_lock("excl-lock", "node-2", ttl_seconds=30)
        assert acquired is False
        await backend.release_lock("excl-lock", "node-1")

    async def test_refresh_extends_lock(self, backend: Backend) -> None:
        await backend.acquire_lock("ref-lock", "node-1", ttl_seconds=30)
        await backend.refresh_lock("ref-lock", "node-1", ttl_seconds=60)
        # Lock still held by node-1
        acquired = await backend.acquire_lock("ref-lock", "node-2", ttl_seconds=30)
        assert acquired is False
        await backend.release_lock("ref-lock", "node-1")


# ── Contract: DLQ ──────────────────────────────────────────────────


class TestDLQContract:
    """Every backend MUST support DLQ add/list/get operations."""

    async def test_add_and_list(self, backend: Backend) -> None:
        entry = DLQEntry(
            workflow_run_id="dlq-1",
            step_order=1,
            error_message="boom",
        )
        await backend.add_dlq_entry(entry)
        entries = await backend.list_dlq_entries()
        assert any(e.workflow_run_id == "dlq-1" for e in entries)

    async def test_get_by_id(self, backend: Backend) -> None:
        entry = DLQEntry(
            workflow_run_id="dlq-get",
            step_order=1,
            error_message="boom",
        )
        await backend.add_dlq_entry(entry)
        entries = await backend.list_dlq_entries()
        target = next(e for e in entries if e.workflow_run_id == "dlq-get")
        got = await backend.get_dlq_entry(target.id)
        assert got is not None
        assert got.workflow_run_id == "dlq-get"


# ── Contract: Signals ──────────────────────────────────────────────


class TestSignalContract:
    """Every backend MUST support signal send/consume."""

    async def test_send_and_consume(self, backend: Backend) -> None:
        sig = Signal(
            workflow_run_id="sig-1",
            signal_name="approval",
            signal_data=b'{"approved": true}',
        )
        await backend.send_signal(sig)
        got = await backend.consume_signal("sig-1", "approval")
        assert got is not None
        assert got.signal_data == b'{"approved": true}'

    async def test_consume_returns_none_when_absent(self, backend: Backend) -> None:
        got = await backend.consume_signal("no-run", "no-signal")
        assert got is None


# ── Contract: Workflow definitions ────────────────────────────────


class TestWorkflowDefinitionContract:
    """Every backend MUST support dynamic workflow definition persistence."""

    async def test_save_and_load(self, backend: Backend) -> None:
        await backend.save_workflow_definition("wf-a", 1, '{"steps": []}')
        defs = await backend.load_workflow_definitions()
        assert len(defs) >= 1
        found = [d for d in defs if d[0] == "wf-a" and d[1] == 1]
        assert len(found) == 1
        assert found[0][2] == '{"steps": []}'

    async def test_save_upsert(self, backend: Backend) -> None:
        await backend.save_workflow_definition("wf-b", 1, '{"v": 1}')
        await backend.save_workflow_definition("wf-b", 1, '{"v": 2}')
        defs = await backend.load_workflow_definitions()
        found = [d for d in defs if d[0] == "wf-b" and d[1] == 1]
        assert len(found) == 1
        assert found[0][2] == '{"v": 2}'

    async def test_delete(self, backend: Backend) -> None:
        await backend.save_workflow_definition("wf-del", 1, "{}")
        await backend.delete_workflow_definition("wf-del", 1)
        defs = await backend.load_workflow_definitions()
        found = [d for d in defs if d[0] == "wf-del" and d[1] == 1]
        assert len(found) == 0

    async def test_delete_nonexistent(self, backend: Backend) -> None:
        # Should not raise
        await backend.delete_workflow_definition("nonexistent", 99)

    async def test_multiple_versions(self, backend: Backend) -> None:
        await backend.save_workflow_definition("wf-mv", 1, '{"v": 1}')
        await backend.save_workflow_definition("wf-mv", 2, '{"v": 2}')
        defs = await backend.load_workflow_definitions()
        found = sorted(
            [d for d in defs if d[0] == "wf-mv"],
            key=lambda d: d[1],
        )
        assert len(found) == 2
        assert found[0][1] == 1
        assert found[1][1] == 2

    async def test_save_batch(self, backend: Backend) -> None:
        batch = [
            ("wf-batch-1", 1, '{"batch": 1}'),
            ("wf-batch-2", 1, '{"batch": 2}'),
            ("wf-batch-3", 1, '{"batch": 3}'),
        ]
        count = await backend.save_workflow_definitions_batch(batch)
        assert count == 3
        defs = await backend.load_workflow_definitions()
        found = [d for d in defs if d[0].startswith("wf-batch-")]
        assert len(found) == 3

    async def test_delete_batch(self, backend: Backend) -> None:
        await backend.save_workflow_definition("wf-dbatch-1", 1, "{}")
        await backend.save_workflow_definition("wf-dbatch-2", 1, "{}")
        count = await backend.delete_workflow_definitions_batch(
            [
                ("wf-dbatch-1", 1),
                ("wf-dbatch-2", 1),
            ]
        )
        assert count == 2
        defs = await backend.load_workflow_definitions()
        found = [d for d in defs if d[0].startswith("wf-dbatch-")]
        assert len(found) == 0


# ── Contract: Circuit breaker state ──────────────────────────────


class TestCircuitBreakerStateContract:
    """Every backend MUST support circuit breaker state persistence."""

    async def test_save_and_load(self, backend: Backend) -> None:
        await backend.save_circuit_state("cb-a", '{"state": "closed"}')
        got = await backend.load_circuit_state("cb-a")
        assert got == '{"state": "closed"}'

    async def test_load_nonexistent(self, backend: Backend) -> None:
        got = await backend.load_circuit_state("cb-missing")
        assert got is None

    async def test_save_overwrites(self, backend: Backend) -> None:
        await backend.save_circuit_state("cb-ow", '{"state": "closed"}')
        await backend.save_circuit_state("cb-ow", '{"state": "open"}')
        got = await backend.load_circuit_state("cb-ow")
        assert got == '{"state": "open"}'
