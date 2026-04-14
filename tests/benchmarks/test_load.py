"""Load and stress tests for Gravtory.

Run with: pytest tests/benchmarks/test_load.py -v -m benchmark --benchmark-enable

These tests measure throughput and latency under load:
1. Sequential workflow execution throughput
2. Concurrent workflow execution throughput
3. Checkpoint write/read throughput
4. Large workflow (many steps) execution time
5. Parallel step fan-out scalability
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.checkpoint import CheckpointEngine
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)
from gravtory.serialization.json import JSONSerializer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_linear_workflow(num_steps: int, name: str = "bench") -> WorkflowDefinition:
    """Create a linear N-step workflow where each step returns a small dict."""
    steps: dict[int, StepDefinition] = {}
    for i in range(1, num_steps + 1):

        async def _step_fn(order: int = i, **kwargs: Any) -> dict[str, Any]:
            return {"step": order, "ok": True}

        steps[i] = StepDefinition(
            order=i,
            name=f"step_{i}",
            depends_on=[i - 1] if i > 1 else [],
            function=_step_fn,
        )
    return WorkflowDefinition(
        name=name,
        version=1,
        steps=steps,
        config=WorkflowConfig(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
class TestSequentialThroughput:
    """Measure sequential workflow execution throughput."""

    @pytest.mark.asyncio
    async def test_100_sequential_3step_workflows(self) -> None:
        """Execute 100 three-step workflows sequentially."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        checkpoint = CheckpointEngine(serializer="json")
        engine = ExecutionEngine(registry, backend, checkpoint)

        definition = _make_linear_workflow(3, "seq-bench")
        registry.register(definition)

        start = time.monotonic()
        for i in range(100):
            await engine.execute_workflow(
                definition=definition,
                run_id=f"seq-{i}",
                input_data={},
            )
        elapsed = time.monotonic() - start

        assert elapsed < 30.0, f"100 workflows took {elapsed:.1f}s (expected <30s)"
        throughput = 100 / elapsed
        print(f"\nSequential throughput: {throughput:.1f} workflows/sec ({elapsed:.2f}s total)")


@pytest.mark.benchmark
class TestConcurrentThroughput:
    """Measure concurrent workflow execution throughput."""

    @pytest.mark.asyncio
    async def test_50_concurrent_3step_workflows(self) -> None:
        """Execute 50 three-step workflows concurrently."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        checkpoint = CheckpointEngine(serializer="json")
        engine = ExecutionEngine(registry, backend, checkpoint)

        definition = _make_linear_workflow(3, "conc-bench")
        registry.register(definition)

        start = time.monotonic()
        tasks = [
            engine.execute_workflow(
                definition=definition,
                run_id=f"conc-{i}",
                input_data={},
            )
            for i in range(50)
        ]
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        assert all(r.status == WorkflowStatus.COMPLETED for r in results)
        assert elapsed < 15.0, f"50 concurrent workflows took {elapsed:.1f}s"
        throughput = 50 / elapsed
        print(f"\nConcurrent throughput: {throughput:.1f} workflows/sec ({elapsed:.2f}s total)")


@pytest.mark.benchmark
class TestCheckpointThroughput:
    """Measure checkpoint serialize/deserialize throughput."""

    def test_json_serialize_10k_small_objects(self) -> None:
        """Serialize 10,000 small dicts."""
        ser = JSONSerializer()
        data = {"order_id": "ord_123", "amount": 99.99, "status": "ok"}

        start = time.monotonic()
        for _ in range(10_000):
            ser.serialize(data)
        elapsed = time.monotonic() - start

        ops = 10_000 / elapsed
        print(f"\nJSON serialize: {ops:.0f} ops/sec ({elapsed:.3f}s for 10k)")

    def test_checkpoint_pipeline_roundtrip_5k(self) -> None:
        """Full pipeline (JSON + gzip) round-trip 5,000 times."""
        engine = CheckpointEngine(serializer="json", compression="gzip")
        data = {"items": [{"id": i, "value": f"item_{i}"} for i in range(20)]}

        start = time.monotonic()
        for _ in range(5_000):
            stored = engine.process(data)
            engine.restore(stored)
        elapsed = time.monotonic() - start

        ops = 5_000 / elapsed
        print(f"\nCheckpoint roundtrip (JSON+gzip): {ops:.0f} ops/sec ({elapsed:.3f}s for 5k)")


@pytest.mark.benchmark
class TestLargeWorkflow:
    """Measure execution time for workflows with many steps."""

    @pytest.mark.asyncio
    async def test_50_step_linear_workflow(self) -> None:
        """Execute a single 50-step linear workflow."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        checkpoint = CheckpointEngine(serializer="json")
        engine = ExecutionEngine(registry, backend, checkpoint)

        definition = _make_linear_workflow(50, "large-bench")
        registry.register(definition)

        start = time.monotonic()
        result = await engine.execute_workflow(
            definition=definition,
            run_id="large-1",
            input_data={},
        )
        elapsed = time.monotonic() - start

        assert result.status == WorkflowStatus.COMPLETED
        assert elapsed < 10.0, f"50-step workflow took {elapsed:.1f}s"
        print(f"\n50-step workflow: {elapsed:.3f}s")
