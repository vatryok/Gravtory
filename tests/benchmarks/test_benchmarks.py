"""Performance benchmark tests for Gravtory core operations.

Uses pytest-benchmark to measure execution time of critical paths:
- Serialization (JSON, Pickle)
- Compression (Gzip)
- DAG topological sort
- Cron next-fire-time computation
- Workflow execution (InMemory backend)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.dag import DAG
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepDefinition
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow
from gravtory.scheduling.cron import CronExpression
from gravtory.serialization.compression import GzipCompressor
from gravtory.serialization.json import JSONSerializer
from gravtory.serialization.pickle import PickleSerializer

pytestmark = pytest.mark.benchmark


# ── Fixture workflows ────────────────────────────────────────────


@workflow(id="bench-single-{x}")
class BenchSingleStep:
    @step(1)
    async def compute(self, x: int) -> dict[str, int]:
        return {"result": x * 2}


@workflow(id="bench-chain-{x}")
class BenchChainWorkflow:
    @step(1)
    async def first(self, x: int) -> dict[str, int]:
        return {"a": x + 1}

    @step(2, depends_on=1)
    async def second(self, a: int, **kw: object) -> dict[str, int]:
        return {"b": a * 10}

    @step(3, depends_on=2)
    async def third(self, b: int, **kw: object) -> dict[str, int]:
        return {"c": b + 5}


# ── Helpers ──────────────────────────────────────────────────────


def _make_linear_dag(n: int) -> dict[int, StepDefinition]:
    steps: dict[int, StepDefinition] = {1: StepDefinition(order=1, name="s1", depends_on=[])}
    for i in range(2, n + 1):
        steps[i] = StepDefinition(order=i, name=f"s{i}", depends_on=[i - 1])
    return steps


def _make_wide_dag(n: int) -> dict[int, StepDefinition]:
    """1 root → n-1 children."""
    steps: dict[int, StepDefinition] = {1: StepDefinition(order=1, name="s1", depends_on=[])}
    for i in range(2, n + 1):
        steps[i] = StepDefinition(order=i, name=f"s{i}", depends_on=[1])
    return steps


_run_counter = 0


async def _run_workflow_once(wf_proxy: object, input_data: dict) -> None:
    global _run_counter
    _run_counter += 1
    backend = InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()
    registry.register(wf_proxy.definition)  # type: ignore[attr-defined]
    engine = ExecutionEngine(registry, backend)
    await engine.execute_workflow(
        definition=wf_proxy.definition,  # type: ignore[attr-defined]
        run_id=f"bench-run-{_run_counter}",
        input_data=input_data,
    )


# ── Serialization Benchmarks ────────────────────────────────────


class TestJSONSerializerBenchmark:
    def test_serialize_small(self, benchmark: object) -> None:
        ser = JSONSerializer()
        data = {"key": "value", "num": 42, "nested": {"a": [1, 2, 3]}}
        benchmark(ser.serialize, data)  # type: ignore[operator]

    def test_deserialize_small(self, benchmark: object) -> None:
        ser = JSONSerializer()
        encoded = ser.serialize({"key": "value", "num": 42, "nested": {"a": [1, 2, 3]}})
        benchmark(ser.deserialize, encoded)  # type: ignore[operator]

    def test_serialize_large(self, benchmark: object) -> None:
        ser = JSONSerializer()
        data = {"items": [{"id": i, "value": f"item-{i}"} for i in range(1000)]}
        benchmark(ser.serialize, data)  # type: ignore[operator]


class TestPickleSerializerBenchmark:
    def test_serialize_small(self, benchmark: object) -> None:
        ser = PickleSerializer()
        data = {"key": "value", "num": 42, "nested": {"a": [1, 2, 3]}}
        benchmark(ser.serialize, data)  # type: ignore[operator]

    def test_serialize_large(self, benchmark: object) -> None:
        ser = PickleSerializer()
        data = {"items": [{"id": i, "value": f"item-{i}"} for i in range(1000)]}
        benchmark(ser.serialize, data)  # type: ignore[operator]


# ── Compression Benchmarks ──────────────────────────────────────


class TestGzipBenchmark:
    def test_compress_1kb(self, benchmark: object) -> None:
        comp = GzipCompressor()
        data = b"x" * 1024
        benchmark(comp.compress, data)  # type: ignore[operator]

    def test_compress_100kb(self, benchmark: object) -> None:
        comp = GzipCompressor()
        data = b"abcdefghij" * 10240
        benchmark(comp.compress, data)  # type: ignore[operator]

    def test_decompress_100kb(self, benchmark: object) -> None:
        comp = GzipCompressor()
        data = b"abcdefghij" * 10240
        compressed = comp.compress(data)
        benchmark(comp.decompress, compressed)  # type: ignore[operator]


# ── DAG Benchmarks ──────────────────────────────────────────────


class TestDAGBenchmark:
    def test_topo_sort_linear_50(self, benchmark: object) -> None:
        steps = _make_linear_dag(50)
        dag = DAG(steps)
        benchmark(dag.topological_sort)  # type: ignore[operator]

    def test_topo_sort_linear_200(self, benchmark: object) -> None:
        steps = _make_linear_dag(200)
        dag = DAG(steps)
        benchmark(dag.topological_sort)  # type: ignore[operator]

    def test_topo_sort_wide_50(self, benchmark: object) -> None:
        steps = _make_wide_dag(50)
        dag = DAG(steps)
        benchmark(dag.topological_sort)  # type: ignore[operator]

    def test_dag_construction_100(self, benchmark: object) -> None:
        steps = _make_linear_dag(100)
        benchmark(DAG, steps)  # type: ignore[operator]


# ── Cron Benchmarks ─────────────────────────────────────────────


class TestCronBenchmark:
    def test_next_fire_simple(self, benchmark: object) -> None:
        cron = CronExpression("*/5 * * * *")
        ref = datetime(2025, 6, 15, 12, 0, tzinfo=timezone.utc)
        benchmark(cron.next_fire_time, ref)  # type: ignore[operator]

    def test_next_fire_complex(self, benchmark: object) -> None:
        cron = CronExpression("30 2 15 */3 1-5")
        ref = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        benchmark(cron.next_fire_time, ref)  # type: ignore[operator]

    def test_matches(self, benchmark: object) -> None:
        cron = CronExpression("*/5 * * * *")
        ref = datetime(2025, 6, 15, 12, 5, tzinfo=timezone.utc)
        benchmark(cron.matches, ref)  # type: ignore[operator]


# ── Workflow Execution Benchmarks ────────────────────────────────


class TestWorkflowExecutionBenchmark:
    def test_single_step_workflow(self, benchmark: object) -> None:
        def _run() -> None:
            asyncio.run(_run_workflow_once(BenchSingleStep, {"x": 42}))

        benchmark(_run)  # type: ignore[operator]

    def test_three_step_chain(self, benchmark: object) -> None:
        def _run() -> None:
            asyncio.run(_run_workflow_once(BenchChainWorkflow, {"x": 10}))

        benchmark(_run)  # type: ignore[operator]
