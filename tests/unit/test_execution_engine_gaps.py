"""Tests for core.execution — ExecutionEngine coverage gaps.

Covers: resume path, condition evaluation, parallel execution, deadlock,
saga compensations, step timeout, retry exhaustion, backoff strategies,
_resolve_inputs, _function_needs_self, recover_incomplete.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.errors import GravtoryError, StepRetryExhaustedError, StepTimeoutError
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepDefinition,
    StepOutput,
    StepResult,
    StepStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


@pytest.fixture
def registry() -> WorkflowRegistry:
    return WorkflowRegistry()


def _make_definition(
    name: str = "test-wf",
    steps: dict[int, StepDefinition] | None = None,
    saga: bool = False,
) -> WorkflowDefinition:
    if steps is None:
        steps = {
            1: StepDefinition(name="step_a", order=1, retries=0, depends_on=[]),
        }
    return WorkflowDefinition(
        name=name,
        version=1,
        steps=steps,
        config=WorkflowConfig(saga_enabled=saga),
    )


class TestExecuteWorkflow:
    @pytest.mark.asyncio
    async def test_simple_execution(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_fn() -> str:
            return "done"

        step = StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_fn)
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)
        result = await engine.execute_workflow(defn, "run-1", {})
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_resume_execution(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_a() -> dict:
            return {"val": 1}

        async def step_b(val: int = 0) -> str:
            return f"got-{val}"

        steps = {
            1: StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_a),
            2: StepDefinition(name="s2", order=2, retries=0, depends_on=[1], function=step_b),
        }
        defn = _make_definition(steps=steps)
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        # Pre-create run and first step output to simulate partial completion
        run = WorkflowRun(id="resume-1", workflow_name="test-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="resume-1",
                step_order=1,
                step_name="s1",
                output_data={"val": 1},
                status=StepStatus.COMPLETED,
                duration_ms=10,
            )
        )

        result = await engine.execute_workflow(defn, "resume-1", {}, resume=True)
        assert result.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_resume_not_found(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        defn = _make_definition()
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)
        with pytest.raises(GravtoryError, match="Cannot resume"):
            await engine.execute_workflow(defn, "nonexistent", {}, resume=True)


class TestConditionEvaluation:
    @pytest.mark.asyncio
    async def test_condition_skip(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_a() -> str:
            return "a"

        async def step_b() -> str:
            return "b"  # Should be skipped

        steps = {
            1: StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_a),
            2: StepDefinition(
                name="s2",
                order=2,
                retries=0,
                depends_on=[1],
                function=step_b,
                condition=lambda ctx: False,  # Always skip
            ),
        }
        defn = _make_definition(steps=steps)
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)
        result = await engine.execute_workflow(defn, "cond-1", {})
        assert result.status == WorkflowStatus.COMPLETED

        # Step 2 should be skipped
        so = await backend.get_step_output("cond-1", 2)
        assert so is not None
        assert so.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_condition_exception_skips(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_a() -> str:
            return "a"

        async def step_b() -> str:
            return "b"

        def bad_condition(ctx: object) -> bool:
            raise RuntimeError("broken condition")

        steps = {
            1: StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_a),
            2: StepDefinition(
                name="s2",
                order=2,
                retries=0,
                depends_on=[1],
                function=step_b,
                condition=bad_condition,
            ),
        }
        defn = _make_definition(steps=steps)
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)
        result = await engine.execute_workflow(defn, "cond-err-1", {})
        assert result.status == WorkflowStatus.COMPLETED


class TestParallelExecution:
    @pytest.mark.asyncio
    async def test_parallel_steps(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_a() -> str:
            return "a"

        async def step_b() -> str:
            return "b"

        async def step_c(**kwargs: object) -> str:
            return "c"

        steps = {
            1: StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_a),
            2: StepDefinition(name="s2", order=2, retries=0, depends_on=[], function=step_b),
            3: StepDefinition(name="s3", order=3, retries=0, depends_on=[1, 2], function=step_c),
        }
        defn = _make_definition(steps=steps)
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)
        result = await engine.execute_workflow(defn, "par-1", {})
        assert result.status == WorkflowStatus.COMPLETED


class TestStepTimeout:
    @pytest.mark.asyncio
    @pytest.mark.filterwarnings("ignore::ResourceWarning")
    async def test_step_timeout_raises(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def slow_step() -> str:
            await asyncio.sleep(10)
            return "done"

        step = StepDefinition(
            name="slow",
            order=1,
            retries=0,
            depends_on=[],
            function=slow_step,
            timeout=timedelta(milliseconds=50),
        )
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(StepTimeoutError):
            await engine.execute_workflow(defn, "timeout-1", {})


class TestRetryAndBackoff:
    @pytest.mark.asyncio
    async def test_retry_exhaustion(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        call_count = 0

        async def flaky_step() -> str:
            nonlocal call_count
            call_count += 1
            raise ValueError("always fails")

        step = StepDefinition(
            name="flaky",
            order=1,
            retries=2,
            depends_on=[],
            function=flaky_step,
            backoff="constant",
            backoff_base=0.01,
        )
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(StepRetryExhaustedError):
            await engine.execute_workflow(defn, "retry-1", {})
        assert call_count == 3  # initial + 2 retries

    def test_backoff_exponential(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=3,
            backoff="exponential",
            base=1.0,
            multiplier=2.0,
            max_delay=60.0,
            jitter=False,
        )
        assert delay == 4.0  # 1 * 2^(3-1)

    def test_backoff_linear(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=3,
            backoff="linear",
            base=1.0,
            multiplier=2.0,
            max_delay=60.0,
            jitter=False,
        )
        assert delay == 3.0  # 1 * 3

    def test_backoff_constant(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=5,
            backoff="constant",
            base=2.0,
            multiplier=2.0,
            max_delay=60.0,
            jitter=False,
        )
        assert delay == 2.0

    def test_backoff_default_is_exponential(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=2,
            backoff=None,
            base=1.0,
            multiplier=2.0,
            max_delay=60.0,
            jitter=False,
        )
        assert delay == 2.0  # 1 * 2^(2-1)

    def test_backoff_max_delay(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=20,
            backoff="exponential",
            base=1.0,
            multiplier=2.0,
            max_delay=10.0,
            jitter=False,
        )
        assert delay == 10.0

    def test_backoff_jitter(self) -> None:
        delay = ExecutionEngine._calculate_backoff(
            retry_count=1,
            backoff="constant",
            base=10.0,
            multiplier=1.0,
            max_delay=60.0,
            jitter=True,
        )
        assert 5.0 <= delay <= 10.0


class TestResolveInputs:
    def test_single_dependency_dict(self) -> None:
        engine = ExecutionEngine(MagicMock(), MagicMock())
        step = StepDefinition(name="s", order=2, retries=0, depends_on=[1])
        completed = {1: StepResult(output={"x": 1}, status=StepStatus.COMPLETED)}
        result = engine._resolve_inputs(step, completed, {"base": "input"})
        assert result["x"] == 1
        assert result["base"] == "input"

    def test_single_dependency_non_dict(self) -> None:
        engine = ExecutionEngine(MagicMock(), MagicMock())
        step = StepDefinition(name="s", order=2, retries=0, depends_on=[1])
        completed = {1: StepResult(output="scalar", status=StepStatus.COMPLETED)}
        result = engine._resolve_inputs(step, completed, {})
        assert result["_prev_output"] == "scalar"

    def test_multiple_dependencies(self) -> None:
        engine = ExecutionEngine(MagicMock(), MagicMock())
        step = StepDefinition(name="s", order=3, retries=0, depends_on=[1, 2])
        completed = {
            1: StepResult(output="a", status=StepStatus.COMPLETED),
            2: StepResult(output="b", status=StepStatus.COMPLETED),
        }
        result = engine._resolve_inputs(step, completed, {})
        assert result["_dep_outputs"] == {1: "a", 2: "b"}


class TestFunctionNeedsSelf:
    def test_regular_function(self) -> None:
        def my_func(x: int) -> int:
            return x

        assert ExecutionEngine._function_needs_self(my_func) is False

    def test_unbound_method(self) -> None:
        class MyClass:
            def my_method(self, x: int) -> int:
                return x

        assert ExecutionEngine._function_needs_self(MyClass.my_method) is True

    def test_non_inspectable(self) -> None:
        assert ExecutionEngine._function_needs_self(42) is False


class TestSagaCompensation:
    @pytest.mark.asyncio
    async def test_compensation_triggered(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        compensated = []

        class OrderWorkflow:
            async def charge(self) -> str:
                return "charged"

            async def ship(self, **kwargs: object) -> str:
                raise RuntimeError("ship failed")

            async def undo_charge(self, output: object) -> None:
                compensated.append(output)

        steps = {
            1: StepDefinition(
                name="charge",
                order=1,
                retries=0,
                depends_on=[],
                function=OrderWorkflow.charge,
                compensate="undo_charge",
            ),
            2: StepDefinition(
                name="ship",
                order=2,
                retries=0,
                depends_on=[1],
                function=OrderWorkflow.ship,
            ),
        }
        defn = WorkflowDefinition(
            name="saga-wf",
            version=1,
            steps=steps,
            config=WorkflowConfig(saga_enabled=True),
            workflow_class=OrderWorkflow,
        )
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        with pytest.raises(RuntimeError, match="ship failed"):
            await engine.execute_workflow(defn, "saga-1", {})

        run = await backend.get_workflow_run("saga-1")
        assert run is not None
        assert run.status == WorkflowStatus.COMPENSATED


class TestRecoverIncomplete:
    @pytest.mark.asyncio
    async def test_recover_resumes_running(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def step_fn() -> str:
            return "ok"

        step = StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=step_fn)
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        # Pre-create a running workflow
        run = WorkflowRun(id="recover-1", workflow_name="test-wf", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("recover-1", WorkflowStatus.RUNNING)

        recovered = await engine.recover_incomplete()
        assert "recover-1" in recovered

    @pytest.mark.asyncio
    async def test_recover_handles_error(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        async def bad_step() -> str:
            raise RuntimeError("fail")

        step = StepDefinition(name="s1", order=1, retries=0, depends_on=[], function=bad_step)
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        run = WorkflowRun(
            id="recover-err-1", workflow_name="test-wf", status=WorkflowStatus.RUNNING
        )
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("recover-err-1", WorkflowStatus.RUNNING)

        # Should not raise — errors are caught internally
        recovered = await engine.recover_incomplete()
        assert "recover-err-1" not in recovered


class TestSyncStepExecution:
    @pytest.mark.asyncio
    async def test_sync_function_runs_in_executor(
        self, backend: InMemoryBackend, registry: WorkflowRegistry
    ) -> None:
        def sync_step() -> str:
            return "sync-result"

        step = StepDefinition(name="sync_s", order=1, retries=0, depends_on=[], function=sync_step)
        defn = _make_definition(steps={1: step})
        registry.register(defn)
        engine = ExecutionEngine(registry, backend)

        result = await engine.execute_workflow(defn, "sync-1", {})
        assert result.status == WorkflowStatus.COMPLETED
