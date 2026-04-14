"""Unit tests for the ExecutionEngine using InMemoryBackend."""

from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepDefinition,
    StepStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)


async def _make_engine() -> tuple[ExecutionEngine, InMemoryBackend, WorkflowRegistry]:
    backend = InMemoryBackend()
    await backend.initialize()
    registry = WorkflowRegistry()
    engine = ExecutionEngine(registry, backend)
    return engine, backend, registry


def _linear_workflow() -> WorkflowDefinition:
    """3-step linear workflow: 1 -> 2 -> 3."""
    call_log: list[int] = []

    async def step1(**kwargs: object) -> str:
        call_log.append(1)
        return "result_1"

    async def step2(**kwargs: object) -> str:
        call_log.append(2)
        return "result_2"

    async def step3(**kwargs: object) -> str:
        call_log.append(3)
        return "result_3"

    defn = WorkflowDefinition(
        name="LinearWorkflow",
        version=1,
        steps={
            1: StepDefinition(order=1, name="step1", function=step1),
            2: StepDefinition(order=2, name="step2", depends_on=[1], function=step2),
            3: StepDefinition(order=3, name="step3", depends_on=[2], function=step3),
        },
        config=WorkflowConfig(),
    )
    defn._call_log = call_log  # type: ignore[attr-defined]
    return defn


def _dag_workflow() -> WorkflowDefinition:
    """DAG: 1 -> [2, 3] -> 4 (fan-out/fan-in)."""
    call_log: list[int] = []

    async def s1(**kwargs: object) -> str:
        call_log.append(1)
        return "r1"

    async def s2(**kwargs: object) -> str:
        call_log.append(2)
        return "r2"

    async def s3(**kwargs: object) -> str:
        call_log.append(3)
        return "r3"

    async def s4(**kwargs: object) -> str:
        call_log.append(4)
        return "r4"

    defn = WorkflowDefinition(
        name="DAGWorkflow",
        version=1,
        steps={
            1: StepDefinition(order=1, name="s1", function=s1),
            2: StepDefinition(order=2, name="s2", depends_on=[1], function=s2),
            3: StepDefinition(order=3, name="s3", depends_on=[1], function=s3),
            4: StepDefinition(order=4, name="s4", depends_on=[2, 3], function=s4),
        },
        config=WorkflowConfig(),
    )
    defn._call_log = call_log  # type: ignore[attr-defined]
    return defn


class TestLinearWorkflow:
    @pytest.mark.asyncio
    async def test_completes_in_order(self) -> None:
        engine, backend, registry = await _make_engine()
        defn = _linear_workflow()
        registry.register(defn)

        run = await engine.execute_workflow(defn, "run-1", {})
        assert run.status == WorkflowStatus.COMPLETED

        # Verify all steps checkpointed
        outputs = await backend.get_step_outputs("run-1")
        assert len(outputs) == 3
        assert [o.step_order for o in outputs] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_call_order(self) -> None:
        engine, _backend, registry = await _make_engine()
        defn = _linear_workflow()
        registry.register(defn)

        await engine.execute_workflow(defn, "run-1", {})
        assert defn._call_log == [1, 2, 3]  # type: ignore[attr-defined]


class TestDAGWorkflow:
    @pytest.mark.asyncio
    async def test_dag_parallel_execution(self) -> None:
        engine, backend, registry = await _make_engine()
        defn = _dag_workflow()
        registry.register(defn)

        run = await engine.execute_workflow(defn, "run-dag", {})
        assert run.status == WorkflowStatus.COMPLETED

        outputs = await backend.get_step_outputs("run-dag")
        assert len(outputs) == 4

        # Step 1 must run first, step 4 must run last
        orders = [o.step_order for o in outputs]
        assert orders[0] == 1
        assert orders[-1] == 4
        # Steps 2 and 3 can be in either order
        assert set(orders[1:3]) == {2, 3}


class TestResumeAfterCrash:
    @pytest.mark.asyncio
    async def test_resume_skips_completed_steps(self) -> None:
        engine, backend, registry = await _make_engine()
        defn = _linear_workflow()
        registry.register(defn)

        # Simulate: step 1 completed, then "crash"
        from gravtory.core.types import StepOutput, WorkflowRun

        run = WorkflowRun(id="run-crash", workflow_name="LinearWorkflow")
        await backend.create_workflow_run(run)
        await backend.update_workflow_status("run-crash", WorkflowStatus.RUNNING)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="run-crash",
                step_order=1,
                step_name="step1",
                status=StepStatus.COMPLETED,
            )
        )

        # Resume — step 1 should NOT re-execute
        defn._call_log.clear()  # type: ignore[attr-defined]
        result = await engine.execute_workflow(defn, "run-crash", {}, resume=True)
        assert result.status == WorkflowStatus.COMPLETED

        # Step 1 was not called again (was replayed from checkpoint)
        assert 1 not in defn._call_log  # type: ignore[attr-defined]
        # Steps 2 and 3 were executed
        assert 2 in defn._call_log  # type: ignore[attr-defined]
        assert 3 in defn._call_log  # type: ignore[attr-defined]


class TestConditions:
    @pytest.mark.asyncio
    async def test_condition_true_executes(self) -> None:
        engine, _backend, registry = await _make_engine()

        executed = []

        async def s1(**kw: object) -> str:
            executed.append(1)
            return "ok"

        async def s2(**kw: object) -> str:
            executed.append(2)
            return "done"

        defn = WorkflowDefinition(
            name="CondTrue",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=s1),
                2: StepDefinition(
                    order=2,
                    name="s2",
                    depends_on=[1],
                    function=s2,
                    condition=lambda ctx: True,
                ),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)
        run = await engine.execute_workflow(defn, "cond-true", {})
        assert run.status == WorkflowStatus.COMPLETED
        assert executed == [1, 2]

    @pytest.mark.asyncio
    async def test_condition_false_skips(self) -> None:
        engine, backend, registry = await _make_engine()

        executed = []

        async def s1(**kw: object) -> str:
            executed.append(1)
            return "ok"

        async def s2(**kw: object) -> str:
            executed.append(2)
            return "skipped"

        defn = WorkflowDefinition(
            name="CondFalse",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=s1),
                2: StepDefinition(
                    order=2,
                    name="s2",
                    depends_on=[1],
                    function=s2,
                    condition=lambda ctx: False,
                ),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)
        run = await engine.execute_workflow(defn, "cond-false", {})
        assert run.status == WorkflowStatus.COMPLETED
        # Step 2 was skipped
        assert 2 not in executed

        # Verify step 2 recorded as SKIPPED
        output = await backend.get_step_output("cond-false", 2)
        assert output is not None
        assert output.status == StepStatus.SKIPPED


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_already_completed_workflow(self) -> None:
        engine, _backend, registry = await _make_engine()
        defn = _linear_workflow()
        registry.register(defn)

        # First run
        await engine.execute_workflow(defn, "run-idem", {})
        list(defn._call_log)  # type: ignore[attr-defined]

        # Second run with resume — should replay, not re-execute
        defn._call_log.clear()  # type: ignore[attr-defined]
        result = await engine.execute_workflow(defn, "run-idem", {}, resume=True)
        assert result.status == WorkflowStatus.COMPLETED
        # All steps were replayed (not called)
        assert defn._call_log == []  # type: ignore[attr-defined]


class TestClassBasedWorkflow:
    """Tests for the PRIMARY use case: class-based workflows with @workflow/@step."""

    @pytest.mark.asyncio
    async def test_class_based_workflow_executes(self) -> None:
        """Class methods get 'self' bound correctly via workflow instance."""
        from gravtory.decorators.step import step
        from gravtory.decorators.workflow import workflow

        @workflow(id="class-test-{x}")
        class MyWorkflow:
            @step(1)
            async def first(self, **kwargs: object) -> str:
                return "from_first"

            @step(2, depends_on=1)
            async def second(self, **kwargs: object) -> str:
                return "from_second"

        engine, backend, registry = await _make_engine()
        proxy: Any = MyWorkflow
        registry.register(proxy.definition)

        run = await engine.execute_workflow(proxy.definition, "class-test-abc", {})
        assert run.status == WorkflowStatus.COMPLETED

        outputs = await backend.get_step_outputs("class-test-abc")
        assert len(outputs) == 2
        assert outputs[0].output_data == "from_first"  # type: ignore[comparison-overlap]
        assert outputs[1].output_data == "from_second"  # type: ignore[comparison-overlap]

    @pytest.mark.asyncio
    async def test_class_based_workflow_shared_instance(self) -> None:
        """All steps of a class-based workflow share the same instance."""
        from gravtory.decorators.step import step
        from gravtory.decorators.workflow import workflow

        @workflow(id="shared-{x}")
        class SharedWF:
            def __init__(self) -> None:
                self.state: list[str] = []

            @step(1)
            async def first(self, **kwargs: object) -> str:
                self.state.append("first")
                return "a"

            @step(2, depends_on=1)
            async def second(self, **kwargs: object) -> str:
                self.state.append("second")
                # self.state should have both entries since same instance
                return f"state={len(self.state)}"

        engine, backend, registry = await _make_engine()
        proxy: Any = SharedWF
        registry.register(proxy.definition)

        run = await engine.execute_workflow(proxy.definition, "shared-x", {})
        assert run.status == WorkflowStatus.COMPLETED

        outputs = await backend.get_step_outputs("shared-x")
        # Second step should see state from first step
        assert outputs[1].output_data == "state=2"  # type: ignore[comparison-overlap]


class TestResumeWithOutputValues:
    @pytest.mark.asyncio
    async def test_resume_preserves_step_output_values(self) -> None:
        """On resume, step outputs (not just bytes) are correctly restored."""
        engine, backend, registry = await _make_engine()

        call_log: list[int] = []

        async def s1(**kw: object) -> dict:  # type: ignore[type-arg]
            call_log.append(1)
            return {"charge_id": "ch_123", "amount": 99.99}

        async def s2(**kw: object) -> str:
            call_log.append(2)
            return "shipped"

        defn = WorkflowDefinition(
            name="ResumeOutput",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=s1),
                2: StepDefinition(order=2, name="s2", depends_on=[1], function=s2),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)

        # Full run — step 1 output is a dict
        await engine.execute_workflow(defn, "resume-out", {})
        assert call_log == [1, 2]

        # Resume — step 1 should be replayed from checkpoint with correct value
        call_log.clear()
        result = await engine.execute_workflow(defn, "resume-out", {}, resume=True)
        assert result.status == WorkflowStatus.COMPLETED
        assert call_log == []  # Both replayed

        # Verify the actual output value was preserved in checkpoint
        s1_output = await backend.get_step_output("resume-out", 1)
        assert s1_output is not None
        assert s1_output.output_data == {"charge_id": "ch_123", "amount": 99.99}  # type: ignore[comparison-overlap]


class TestExecutionGapFill:
    """Gap-fill tests for execution engine edge cases."""

    @pytest.mark.asyncio
    async def test_workflow_with_all_patterns(self) -> None:
        """Workflow combining retry + parallel + condition."""
        engine, backend, registry = await _make_engine()

        call_counts: dict[str, int] = {"s1": 0, "s2": 0, "s3": 0, "s4": 0}

        async def s1(**kw: object) -> str:
            call_counts["s1"] += 1
            return "ok"

        async def s2(**kw: object) -> str:
            call_counts["s2"] += 1
            return "parallel_a"

        async def s3(**kw: object) -> str:
            call_counts["s3"] += 1
            return "parallel_b"

        async def s4(**kw: object) -> str:
            call_counts["s4"] += 1
            return "final"

        defn = WorkflowDefinition(
            name="AllPatterns",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=s1, retries=2),
                2: StepDefinition(order=2, name="s2", depends_on=[1], function=s2),
                3: StepDefinition(
                    order=3,
                    name="s3",
                    depends_on=[1],
                    function=s3,
                    condition=lambda ctx: True,
                ),
                4: StepDefinition(order=4, name="s4", depends_on=[2, 3], function=s4),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)

        run = await engine.execute_workflow(defn, "all-patterns", {})
        assert run.status == WorkflowStatus.COMPLETED
        assert all(v == 1 for v in call_counts.values())

    @pytest.mark.asyncio
    async def test_large_dag_30_steps(self) -> None:
        """30-step linear chain completes successfully."""
        engine, backend, registry = await _make_engine()

        executed: list[int] = []

        def _make_fn(order: int):  # type: ignore[no-untyped-def]
            async def fn(**kw: object) -> str:
                executed.append(order)
                return f"result_{order}"

            return fn

        steps = {}
        for i in range(1, 31):
            deps = [i - 1] if i > 1 else []
            steps[i] = StepDefinition(order=i, name=f"s{i}", depends_on=deps, function=_make_fn(i))

        defn = WorkflowDefinition(
            name="Large30",
            version=1,
            steps=steps,
            config=WorkflowConfig(),
        )
        registry.register(defn)

        run = await engine.execute_workflow(defn, "large-30", {})
        assert run.status == WorkflowStatus.COMPLETED
        assert executed == list(range(1, 31))

    @pytest.mark.asyncio
    async def test_workflow_with_only_skipped_steps(self) -> None:
        """All steps have condition=False -> all skipped, workflow completes."""
        engine, _backend, registry = await _make_engine()

        async def should_not_run(**kw: object) -> str:
            raise AssertionError("should not execute")

        defn = WorkflowDefinition(
            name="AllSkipped",
            version=1,
            steps={
                1: StepDefinition(
                    order=1,
                    name="s1",
                    function=should_not_run,
                    condition=lambda ctx: False,
                ),
                2: StepDefinition(
                    order=2,
                    name="s2",
                    depends_on=[1],
                    function=should_not_run,
                    condition=lambda ctx: False,
                ),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)

        run = await engine.execute_workflow(defn, "all-skip", {})
        assert run.status == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_backoff_calculation_exponential(self) -> None:
        """Exponential backoff produces increasing delays."""
        from gravtory.core.execution import ExecutionEngine

        d1 = ExecutionEngine._calculate_backoff(1, "exponential", 1.0, 2.0, 60.0, False)
        d2 = ExecutionEngine._calculate_backoff(2, "exponential", 1.0, 2.0, 60.0, False)
        d3 = ExecutionEngine._calculate_backoff(3, "exponential", 1.0, 2.0, 60.0, False)
        assert d1 == 1.0
        assert d2 == 2.0
        assert d3 == 4.0

    @pytest.mark.asyncio
    async def test_backoff_calculation_linear(self) -> None:
        """Linear backoff produces linearly increasing delays."""
        from gravtory.core.execution import ExecutionEngine

        d1 = ExecutionEngine._calculate_backoff(1, "linear", 1.0, 2.0, 60.0, False)
        d2 = ExecutionEngine._calculate_backoff(2, "linear", 1.0, 2.0, 60.0, False)
        assert d1 == 1.0
        assert d2 == 2.0

    @pytest.mark.asyncio
    async def test_backoff_calculation_constant(self) -> None:
        """Constant backoff returns same delay regardless of attempt."""
        from gravtory.core.execution import ExecutionEngine

        d1 = ExecutionEngine._calculate_backoff(1, "constant", 5.0, 2.0, 60.0, False)
        d5 = ExecutionEngine._calculate_backoff(5, "constant", 5.0, 2.0, 60.0, False)
        assert d1 == 5.0
        assert d5 == 5.0

    @pytest.mark.asyncio
    async def test_backoff_respects_max_delay(self) -> None:
        """Delay is capped at max_delay."""
        from gravtory.core.execution import ExecutionEngine

        d = ExecutionEngine._calculate_backoff(100, "exponential", 1.0, 2.0, 10.0, False)
        assert d == 10.0

    @pytest.mark.asyncio
    async def test_backoff_jitter_randomizes(self) -> None:
        """With jitter=True, delay varies between 0.5x and 1.0x of base delay."""
        from gravtory.core.execution import ExecutionEngine

        delays = set()
        for _ in range(20):
            d = ExecutionEngine._calculate_backoff(3, "exponential", 1.0, 2.0, 60.0, True)
            delays.add(round(d, 4))
            assert 2.0 <= d <= 4.0  # 4.0 * [0.5, 1.0]
        # With 20 iterations, jitter should produce varied values
        assert len(delays) > 1


class TestStepFailure:
    @pytest.mark.asyncio
    async def test_workflow_fails_on_step_error(self) -> None:
        engine, backend, registry = await _make_engine()

        async def s1(**kw: object) -> str:
            return "ok"

        async def s2(**kw: object) -> str:
            raise ValueError("step 2 failed")

        defn = WorkflowDefinition(
            name="FailWF",
            version=1,
            steps={
                1: StepDefinition(order=1, name="s1", function=s1),
                2: StepDefinition(order=2, name="s2", depends_on=[1], function=s2),
            },
            config=WorkflowConfig(),
        )
        registry.register(defn)

        with pytest.raises(ValueError, match="step 2 failed"):
            await engine.execute_workflow(defn, "run-fail", {})

        run = await backend.get_workflow_run("run-fail")
        assert run is not None
        assert run.status == WorkflowStatus.FAILED

        # Check DLQ
        dlq = await backend.list_dlq()
        assert len(dlq) == 1
