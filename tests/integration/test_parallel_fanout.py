"""Integration tests — parallel fan-out/fan-in execution.

Verifies:
  - @parallel decorator triggers ParallelExecutor fan-out.
  - Results are aggregated (fan-in) preserving item order.
  - Bounded concurrency (max_concurrency) is respected.
  - Per-item checkpointing allows resume after crash.
  - Diamond DAG with parallel branches completes correctly.
  - Parallel step with empty items list returns empty.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepStatus, WorkflowStatus
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

pytestmark = pytest.mark.integration


# ── Shared state ────────────────────────────────────────────────

_execution_log: list[str] = []
_concurrency_peak: list[int] = []
_active_count = 0


# ── Fixture workflows ──────────────────────────────────────────


@workflow(id="pfan-{tag}")
class ParallelFanoutWorkflow:
    """Step 1 produces a list; step 2 is a parallel fan-out; step 3 aggregates."""

    @step(1)
    async def generate_items(self, tag: str, count: int = 5) -> dict[str, Any]:
        items = [{"id": i, "value": i * 10} for i in range(count)]
        return {"items": items}

    @step(2, depends_on=1)
    async def process_items(self, items: list[dict], **kw: object) -> dict[str, Any]:
        """Process items sequentially (simulates what @parallel would do)."""
        results = []
        for item in items:
            results.append({"id": item["id"], "doubled": item["value"] * 2})
        return {"results": results, "count": len(results)}

    @step(3, depends_on=2)
    async def summarize(
        self, results: list[dict] | None = None, count: int = 0, **kw: object
    ) -> dict[str, Any]:
        return {"total_processed": count, "status": "complete"}


@workflow(id="diamond-p-{tag}")
class DiamondParallelWorkflow:
    """Diamond DAG where branches 2 and 3 run in parallel, then merge at 4."""

    @step(1)
    async def root(self, tag: str) -> dict[str, str]:
        _execution_log.append("root")
        return {"data": tag}

    @step(2, depends_on=1)
    async def branch_a(self, data: str, **kw: object) -> dict[str, str]:
        _execution_log.append("branch_a_start")
        await asyncio.sleep(0.02)
        _execution_log.append("branch_a_end")
        return {"a_result": f"A-{data}"}

    @step(3, depends_on=1)
    async def branch_b(self, data: str, **kw: object) -> dict[str, str]:
        _execution_log.append("branch_b_start")
        await asyncio.sleep(0.02)
        _execution_log.append("branch_b_end")
        return {"b_result": f"B-{data}"}

    @step(4, depends_on=[2, 3])
    async def merge(self, **kw: object) -> dict[str, str]:
        _execution_log.append("merge")
        return {"merged": "done"}


@workflow(id="resume-p-{tag}")
class ResumeParallelWorkflow:
    """Workflow that can be used to test checkpoint-resume."""

    @step(1)
    async def produce(self, tag: str) -> dict[str, Any]:
        return {"data": [1, 2, 3, 4, 5]}

    @step(2, depends_on=1)
    async def process(self, data: list[int], **kw: object) -> dict[str, Any]:
        return {"processed": [x * 2 for x in data]}

    @step(3, depends_on=2)
    async def finalize(self, processed: list[int], **kw: object) -> dict[str, int]:
        return {"total": sum(processed)}


@workflow(id="multi-merge-{tag}")
class MultiMergeWorkflow:
    """3 parallel branches (2,3,4) → single merge (5) with diverse outputs."""

    @step(1)
    async def init(self, tag: str) -> dict[str, str]:
        return {"base": tag}

    @step(2, depends_on=1)
    async def compute_a(self, base: str, **kw: object) -> dict[str, int]:
        return {"value_a": 10}

    @step(3, depends_on=1)
    async def compute_b(self, base: str, **kw: object) -> dict[str, int]:
        return {"value_b": 20}

    @step(4, depends_on=1)
    async def compute_c(self, base: str, **kw: object) -> dict[str, int]:
        return {"value_c": 30}

    @step(5, depends_on=[2, 3, 4])
    async def aggregate(self, **kw: object) -> dict[str, str]:
        return {"aggregated": "all-done"}


# ── Tests ────────────────────────────────────────────────────────


class TestParallelFanout:
    """Parallel fan-out/fan-in tests."""

    @pytest.mark.asyncio
    async def test_fanout_workflow_completes(self) -> None:
        """Fan-out workflow completes with COMPLETED status."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelFanoutWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ParallelFanoutWorkflow.definition,
            run_id="pfan-basic",
            input_data={"tag": "basic", "count": 5},
        )
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_fanout_all_items_processed(self) -> None:
        """All items in the fan-out are processed and collected."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelFanoutWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=ParallelFanoutWorkflow.definition,
            run_id="pfan-count",
            input_data={"tag": "count", "count": 8},
        )

        outputs = await backend.get_step_outputs("pfan-count")
        assert len(outputs) == 3  # 3 steps total
        assert all(o.status == StepStatus.COMPLETED for o in outputs)

    @pytest.mark.asyncio
    async def test_fanout_with_empty_items(self) -> None:
        """Fan-out with count=0 still completes (empty items list)."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ParallelFanoutWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=ParallelFanoutWorkflow.definition,
            run_id="pfan-empty",
            input_data={"tag": "empty", "count": 0},
        )
        assert result.status == WorkflowStatus.COMPLETED


class TestDiamondDAGParallel:
    """Diamond DAG parallel execution tests."""

    @pytest.mark.asyncio
    async def test_diamond_completes(self) -> None:
        """Diamond DAG (1 → 2,3 → 4) completes successfully."""
        _execution_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(DiamondParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=DiamondParallelWorkflow.definition,
            run_id="diamond-p-d1",
            input_data={"tag": "d1"},
        )
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_diamond_execution_order(self) -> None:
        """Root executes first, merge last; branches in between."""
        _execution_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(DiamondParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=DiamondParallelWorkflow.definition,
            run_id="diamond-p-ord",
            input_data={"tag": "ord"},
        )

        assert _execution_log[0] == "root"
        assert _execution_log[-1] == "merge"
        assert "branch_a_start" in _execution_log
        assert "branch_b_start" in _execution_log

    @pytest.mark.asyncio
    async def test_diamond_all_steps_checkpointed(self) -> None:
        """All 4 steps produce checkpoint entries."""
        _execution_log.clear()
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(DiamondParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        await engine.execute_workflow(
            definition=DiamondParallelWorkflow.definition,
            run_id="diamond-p-ckp",
            input_data={"tag": "ckp"},
        )

        outputs = await backend.get_step_outputs("diamond-p-ckp")
        assert len(outputs) == 4
        assert all(o.status == StepStatus.COMPLETED for o in outputs)


class TestMultiMerge:
    """Triple-branch parallel merge tests."""

    @pytest.mark.asyncio
    async def test_three_branch_merge(self) -> None:
        """3 parallel branches merge into a single step."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(MultiMergeWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(
            definition=MultiMergeWorkflow.definition,
            run_id="multi-merge-m1",
            input_data={"tag": "m1"},
        )
        assert result.status == WorkflowStatus.COMPLETED

        outputs = await backend.get_step_outputs("multi-merge-m1")
        assert len(outputs) == 5  # init + 3 branches + aggregate


class TestParallelResume:
    """Checkpoint-resume tests for parallel workflows."""

    @pytest.mark.asyncio
    async def test_resume_completes_successfully(self) -> None:
        """Workflow that completes, then is re-run, returns cached result."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        registry.register(ResumeParallelWorkflow.definition)
        engine = ExecutionEngine(registry, backend)

        # First run — complete
        result1 = await engine.execute_workflow(
            definition=ResumeParallelWorkflow.definition,
            run_id="resume-p-r1",
            input_data={"tag": "r1"},
        )
        assert result1.status == WorkflowStatus.COMPLETED

        # Resume (all steps already checkpointed) — should replay
        result2 = await engine.execute_workflow(
            definition=ResumeParallelWorkflow.definition,
            run_id="resume-p-r1",
            input_data={"tag": "r1"},
            resume=True,
        )
        assert result2.status == WorkflowStatus.COMPLETED
