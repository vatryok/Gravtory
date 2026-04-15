"""T-002: Concurrent integration tests.

Validates correctness under concurrent multi-worker access patterns:
- Multiple workers claiming from the same queue
- Concurrent signal send/consume
- Concurrent lock acquire/release
"""

from __future__ import annotations

import asyncio

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    PendingStep,
    Signal,
    WorkerInfo,
    WorkflowRun,
    WorkflowStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _init_backend():
    backend = InMemoryBackend()
    await backend.initialize()
    return backend


# ---------------------------------------------------------------------------
# T-002a: Concurrent step claiming — no duplicate claims
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_claim_step_no_duplicates():
    """10 workers claiming concurrently from the same queue must not
    claim the same step twice."""
    backend = await _init_backend()

    # Register 10 workers
    for i in range(10):
        await backend.register_worker(
            WorkerInfo(
                worker_id=f"worker-{i}",
                node_id="localhost",
            )
        )

    # Create a workflow run
    run = WorkflowRun(
        id="conc-claim-1",
        workflow_name="test_wf",
        workflow_version=1,
        namespace="default",
        status=WorkflowStatus.RUNNING,
        input_data=b"{}",
    )
    await backend.create_workflow_run(run)

    # Enqueue 5 pending steps
    for step_order in range(5):
        await backend.enqueue_step(
            PendingStep(
                workflow_run_id="conc-claim-1",
                step_order=step_order,
                priority=0,
            )
        )

    # 10 workers claim concurrently
    async def claim(worker_id: str):
        return await backend.claim_step(worker_id)

    results = await asyncio.gather(*[claim(f"worker-{i}") for i in range(10)])

    claimed = [r for r in results if r is not None]

    # We had 5 steps — at most 5 should be claimed
    assert len(claimed) <= 5

    # No duplicate step_orders
    claimed_orders = [c.step_order for c in claimed]
    assert len(claimed_orders) == len(set(claimed_orders)), (
        f"Duplicate claims detected: {claimed_orders}"
    )


# ---------------------------------------------------------------------------
# T-002b: Concurrent signal send/consume — no double consumption
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_signal_consume_no_duplicates():
    """Concurrent consumers of the same signal should not both receive it."""
    backend = await _init_backend()

    # Create workflow run
    run = WorkflowRun(
        id="conc-signal-1",
        workflow_name="test_wf",
        workflow_version=1,
        namespace="default",
        status=WorkflowStatus.RUNNING,
        input_data=b"{}",
    )
    await backend.create_workflow_run(run)

    # Send a single signal
    await backend.send_signal(
        Signal(
            workflow_run_id="conc-signal-1",
            signal_name="approve",
            signal_data=b'{"ok": true}',
        )
    )

    # 5 concurrent consumers
    async def consume():
        return await backend.consume_signal("conc-signal-1", "approve")

    results = await asyncio.gather(*[consume() for _ in range(5)])
    consumed = [r for r in results if r is not None]

    # Only one consumer should get the signal
    assert len(consumed) == 1, f"Expected 1 consumer, got {len(consumed)}"


# ---------------------------------------------------------------------------
# T-002c: Concurrent lock acquire — mutual exclusion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_lock_acquire_mutual_exclusion():
    """Only one holder should acquire a named lock concurrently."""
    backend = await _init_backend()

    async def try_acquire(holder_id: str):
        return await backend.acquire_lock(
            lock_name="leader",
            holder_id=holder_id,
            ttl_seconds=60,
        )

    results = await asyncio.gather(*[try_acquire(f"node-{i}") for i in range(10)])

    acquired = [i for i, r in enumerate(results) if r]

    # Exactly one should succeed
    assert len(acquired) == 1, f"Expected 1 lock holder, got {len(acquired)}: nodes {acquired}"


# ---------------------------------------------------------------------------
# T-002d: Concurrent workflow status updates don't corrupt state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_status_updates():
    """Multiple concurrent status updates should not lose writes."""
    backend = await _init_backend()

    run = WorkflowRun(
        id="conc-status-1",
        workflow_name="test_wf",
        workflow_version=1,
        namespace="default",
        status=WorkflowStatus.PENDING,
        input_data=b"{}",
    )
    await backend.create_workflow_run(run)

    # Multiple status transitions
    statuses = [
        WorkflowStatus.RUNNING,
        WorkflowStatus.RUNNING,
        WorkflowStatus.COMPLETED,
    ]

    async def update(status):
        await backend.update_workflow_status("conc-status-1", status)

    await asyncio.gather(*[update(s) for s in statuses])

    final = await backend.get_workflow_run("conc-status-1")
    assert final is not None
    # The run should be in one of the valid final states
    assert final.status in (
        WorkflowStatus.RUNNING,
        WorkflowStatus.COMPLETED,
    )
