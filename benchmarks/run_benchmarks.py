"""Gravtory Benchmark Suite — run and publish performance results.

Usage:
    python benchmarks/run_benchmarks.py

Outputs a formatted table of benchmark results suitable for documentation.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from gravtory.scheduling.cron import CronExpression
from gravtory.core.dag import DAG
from gravtory.core.types import StepDefinition
from gravtory.serialization.json import JSONSerializer


def benchmark(func, iterations=1000, warmup=100):
    """Run a function many times and return timing statistics."""
    # Warmup
    for _ in range(warmup):
        func()

    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed)

    return {
        "mean_ns": statistics.mean(times),
        "median_ns": statistics.median(times),
        "p99_ns": sorted(times)[int(len(times) * 0.99)],
        "min_ns": min(times),
        "max_ns": max(times),
        "ops_per_sec": 1_000_000_000 / statistics.mean(times),
        "iterations": iterations,
    }


def benchmark_async(coro_factory, iterations=1000, warmup=100):
    """Benchmark an async function."""
    loop = asyncio.new_event_loop()

    # Warmup
    for _ in range(warmup):
        loop.run_until_complete(coro_factory())

    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        loop.run_until_complete(coro_factory())
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed)

    loop.close()

    return {
        "mean_ns": statistics.mean(times),
        "median_ns": statistics.median(times),
        "p99_ns": sorted(times)[int(len(times) * 0.99)],
        "min_ns": min(times),
        "max_ns": max(times),
        "ops_per_sec": 1_000_000_000 / statistics.mean(times),
        "iterations": iterations,
    }


def format_ns(ns: float) -> str:
    """Format nanoseconds to human-readable string."""
    if ns < 1_000:
        return f"{ns:.0f}ns"
    elif ns < 1_000_000:
        return f"{ns / 1_000:.1f}us"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f}ms"
    else:
        return f"{ns / 1_000_000_000:.2f}s"


def format_ops(ops: float) -> str:
    """Format operations per second."""
    if ops >= 1_000_000:
        return f"{ops / 1_000_000:.1f}M/s"
    elif ops >= 1_000:
        return f"{ops / 1_000:.1f}K/s"
    else:
        return f"{ops:.0f}/s"


# ─── Benchmark Functions ──────────────────────────────────────────────

def bench_json_serialize_small():
    """Serialize a small Python dict to JSON bytes."""
    serializer = JSONSerializer()
    data = {"order_id": "ord_123", "amount": 99.99, "status": "completed"}
    return benchmark(lambda: serializer.serialize(data), iterations=5000)


def bench_json_serialize_medium():
    """Serialize a medium Python dict (~1KB) to JSON bytes."""
    serializer = JSONSerializer()
    data = {
        "order_id": "ord_123",
        "items": [{"id": f"item_{i}", "price": 10.0 + i, "qty": i + 1} for i in range(20)],
        "metadata": {"source": "api", "version": "2.0", "tags": [f"tag_{i}" for i in range(10)]},
    }
    return benchmark(lambda: serializer.serialize(data), iterations=5000)


def bench_json_serialize_large():
    """Serialize a large Python dict (~100KB) to JSON bytes."""
    serializer = JSONSerializer()
    data = {
        "records": [
            {"id": i, "name": f"record_{i}", "values": list(range(50)), "meta": {"x": i}}
            for i in range(200)
        ]
    }
    return benchmark(lambda: serializer.serialize(data), iterations=500)


def bench_json_roundtrip():
    """Serialize then deserialize a medium dict."""
    serializer = JSONSerializer()
    data = {"order_id": "ord_123", "amount": 99.99, "items": list(range(50))}

    def roundtrip():
        b = serializer.serialize(data)
        serializer.deserialize(b)

    return benchmark(roundtrip, iterations=5000)


def bench_dag_topological_sort_small():
    """Topological sort of a 5-step linear DAG."""
    steps = {i: StepDefinition(
        name=f"step_{i}", order=i,
        depends_on=[i - 1] if i > 0 else [],
    ) for i in range(5)}
    dag = DAG(steps)
    return benchmark(lambda: dag.topological_sort(), iterations=5000)


def bench_dag_topological_sort_large():
    """Topological sort of a 50-step DAG with fan-out/fan-in."""
    steps = {}
    steps[0] = StepDefinition(name="start", order=0, depends_on=[])
    for i in range(1, 26):
        steps[i] = StepDefinition(
            name=f"parallel_{i}", order=i, depends_on=[0],
        )
    for i in range(26, 50):
        steps[i] = StepDefinition(
            name=f"final_{i}", order=i,
            depends_on=list(range(1, 26)),
        )
    dag = DAG(steps)
    return benchmark(lambda: dag.topological_sort(), iterations=2000)


def bench_cron_next_fire():
    """Calculate next fire time for a cron expression."""
    from datetime import datetime, timezone
    cron = CronExpression("*/5 * * * *")
    now = datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    return benchmark(lambda: cron.next_fire_time(now), iterations=5000)


def bench_cron_parse():
    """Parse a cron expression."""
    return benchmark(lambda: CronExpression("0 9 * * 1-5"), iterations=5000)


def bench_id_template():
    """Generate a workflow ID from a template."""
    from gravtory.core.id_template import generate_workflow_id
    return benchmark(
        lambda: generate_workflow_id("order-{customer}-{date}", customer="cust_123", date="2025-01-15"),
        iterations=10000,
    )


# ─── Compression benchmarks ──────────────────────────────────────────

def bench_gzip_compress():
    """Gzip compress a ~1KB JSON payload."""
    from gravtory.serialization.compression import GzipCompressor
    compressor = GzipCompressor()
    data = json.dumps({"records": [{"id": i, "value": f"data_{i}"} for i in range(50)]}).encode()
    return benchmark(lambda: compressor.compress(data), iterations=2000)


def bench_gzip_roundtrip():
    """Gzip compress then decompress."""
    from gravtory.serialization.compression import GzipCompressor
    compressor = GzipCompressor()
    data = json.dumps({"records": [{"id": i, "value": f"data_{i}"} for i in range(50)]}).encode()

    def roundtrip():
        compressed = compressor.compress(data)
        compressor.decompress(compressed)

    return benchmark(roundtrip, iterations=2000)


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 78)
    print("  GRAVTORY BENCHMARK SUITE")
    print("=" * 78)
    print()

    benchmarks = [
        ("JSON Serialize (small, ~100B)", bench_json_serialize_small),
        ("JSON Serialize (medium, ~1KB)", bench_json_serialize_medium),
        ("JSON Serialize (large, ~100KB)", bench_json_serialize_large),
        ("JSON Roundtrip (medium)", bench_json_roundtrip),
        ("DAG Topological Sort (5 steps)", bench_dag_topological_sort_small),
        ("DAG Topological Sort (50 steps)", bench_dag_topological_sort_large),
        ("Cron Next Fire Time", bench_cron_next_fire),
        ("Cron Parse Expression", bench_cron_parse),
        ("Workflow ID Template", bench_id_template),
        ("Gzip Compress (~1KB)", bench_gzip_compress),
        ("Gzip Roundtrip (~1KB)", bench_gzip_roundtrip),
    ]

    results = []
    for name, func in benchmarks:
        print(f"  Running: {name}...", end="", flush=True)
        result = func()
        results.append((name, result))
        print(f" {format_ops(result['ops_per_sec'])} ({format_ns(result['median_ns'])} median)")

    print()
    print("=" * 78)
    print(f"  {'Benchmark':<40} {'Median':>10} {'P99':>10} {'Ops/sec':>12}")
    print("-" * 78)
    for name, result in results:
        print(
            f"  {name:<40} "
            f"{format_ns(result['median_ns']):>10} "
            f"{format_ns(result['p99_ns']):>10} "
            f"{format_ops(result['ops_per_sec']):>12}"
        )
    print("=" * 78)

    # Write results to JSON for CI consumption
    output_path = Path(__file__).parent / "results.json"
    json_results = {name: {k: v for k, v in r.items()} for name, r in results}
    output_path.write_text(json.dumps(json_results, indent=2))
    print(f"\n  Results written to {output_path}")


if __name__ == "__main__":
    main()
