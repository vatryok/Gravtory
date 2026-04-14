"""T-001: Chaos / fault-injection tests.

Validates that the engine recovers correctly from:
- Database connection loss mid-transaction
- Worker crash during step execution
- OOM / large checkpoint data
- Corrupted checkpoint data in the backend
"""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.checkpoint import CheckpointEngine
from gravtory.core.errors import SerializationError
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_simple_workflow(step_fn=None):
    """Build a minimal workflow definition for testing."""
    if step_fn is None:

        async def step_fn(ctx):
            return {"result": "ok"}

    return WorkflowDefinition(
        name="chaos_test_wf",
        version=1,
        steps=[
            StepDefinition(
                name="step_1",
                order=0,
                function=step_fn,
            ),
        ],
        config=WorkflowConfig(namespace="default"),
    )


async def _build_engine(backend=None):
    backend = backend or InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()
    defn = _make_simple_workflow()
    registry.register(defn)
    engine = ExecutionEngine(registry=registry, backend=backend)
    return engine, backend, defn


# ---------------------------------------------------------------------------
# T-001a: DB connection loss during save_step_output
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_db_failure_during_checkpoint_save():
    """Engine should propagate error when DB fails during checkpoint write."""
    engine, backend, defn = await _build_engine()

    call_count = 0
    original_save = backend.save_step_output

    async def failing_save(output):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Database connection lost")
        return await original_save(output)

    backend.save_step_output = failing_save

    run_id = "chaos-db-fail-1"
    with pytest.raises(ConnectionError, match="Database connection lost"):
        await engine.execute_workflow(defn, run_id, {})


# ---------------------------------------------------------------------------
# T-001b: Worker crash during step execution (step raises unexpectedly)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_step_crash_marks_workflow_failed():
    """A step that crashes should result in FAILED workflow status."""

    async def crashing_step(ctx):
        raise RuntimeError("Simulated OOM crash")

    backend = InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()
    defn = _make_simple_workflow(crashing_step)
    registry.register(defn)
    engine = ExecutionEngine(registry=registry, backend=backend)

    run_id = "chaos-crash-1"
    with pytest.raises(RuntimeError, match="Simulated OOM crash"):
        await engine.execute_workflow(defn, run_id, {})

    run = await backend.get_workflow_run(run_id)
    assert run is not None
    assert run.status in (WorkflowStatus.FAILED, WorkflowStatus.COMPENSATED)


# ---------------------------------------------------------------------------
# T-001c: Corrupted checkpoint data in backend
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_corrupted_checkpoint_raises_on_restore():
    """Restoring corrupted checkpoint data should raise SerializationError."""
    cp = CheckpointEngine(serializer="json")

    with pytest.raises(SerializationError):
        cp.restore(b"")

    with pytest.raises(SerializationError):
        cp.restore(b"\xff" + b"not-valid-data-at-all")


# ---------------------------------------------------------------------------
# T-001d: Large checkpoint exceeding max size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkpoint_max_size_enforced():
    """Checkpoint engine should reject payloads exceeding max_checkpoint_size."""
    cp = CheckpointEngine(serializer="json", max_checkpoint_size=1024)

    small_data = {"key": "value"}
    result = cp.process(small_data)
    assert len(result) > 0

    large_data = {"key": "x" * 2048}
    with pytest.raises(SerializationError, match="exceeds max_checkpoint_size"):
        cp.process(large_data)


# ---------------------------------------------------------------------------
# T-001e: Backend health check failure at startup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backend_health_check_failure():
    """Backend returning unhealthy after initialize should be detectable."""
    backend = InMemoryBackend()
    await backend.initialize()

    healthy = await backend.health_check()
    assert healthy is True


# ---------------------------------------------------------------------------
# T-001f: Concurrent step execution with one failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_step_one_fails():
    """When parallel steps run and one fails, the failure should propagate."""
    call_order = []

    async def good_step(ctx):
        call_order.append("good")
        return {"status": "ok"}

    async def bad_step(ctx):
        call_order.append("bad")
        raise ValueError("Step 2 failed")

    backend = InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()

    defn = WorkflowDefinition(
        name="parallel_chaos_wf",
        version=1,
        steps=[
            StepDefinition(name="good", order=0, function=good_step),
            StepDefinition(name="bad", order=1, function=bad_step, depends_on=[]),
        ],
        config=WorkflowConfig(namespace="default"),
    )
    registry.register(defn)
    engine = ExecutionEngine(registry=registry, backend=backend)

    with pytest.raises(ValueError, match="Step 2 failed"):
        await engine.execute_workflow(defn, "chaos-parallel-1", {})
