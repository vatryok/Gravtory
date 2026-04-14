"""Unit tests for @step and @workflow decorators."""

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.decorators.step import step
from gravtory.decorators.workflow import WorkflowProxy, workflow

if TYPE_CHECKING:
    from gravtory.core.types import StepDefinition


class TestStepDecorator:
    def test_basic_order_and_name(self) -> None:
        @step(1)
        async def charge(self: object, order_id: str) -> dict[str, object]:
            return {}

        assert hasattr(charge, "__gravtory_step__")
        sd: StepDefinition = charge.__gravtory_step__
        assert sd.order == 1
        assert sd.name == "charge"

    def test_custom_name(self) -> None:
        @step(1, name="custom_name")
        async def do_thing(self: object) -> None:
            pass

        assert do_thing.__gravtory_step__.name == "custom_name"

    def test_depends_on_int(self) -> None:
        @step(2, depends_on=1)
        async def ship(self: object) -> None:
            pass

        assert ship.__gravtory_step__.depends_on == [1]

    def test_depends_on_list(self) -> None:
        @step(3, depends_on=[1, 2])
        async def summarize(self: object) -> None:
            pass

        assert summarize.__gravtory_step__.depends_on == [1, 2]

    def test_retry_config(self) -> None:
        @step(1, retries=5, backoff="exponential", backoff_base=2.0, jitter=True)
        async def flaky(self: object) -> None:
            pass

        sd = flaky.__gravtory_step__
        assert sd.retries == 5
        assert sd.backoff == "exponential"
        assert sd.backoff_base == 2.0
        assert sd.jitter is True

    def test_type_extraction(self) -> None:
        @step(1)
        async def process(self: object, order_id: str, amount: float) -> dict[str, object]:
            return {}

        sd = process.__gravtory_step__
        assert "order_id" in sd.input_types
        assert "amount" in sd.input_types

    def test_original_function_returned(self) -> None:
        @step(1)
        async def my_func(self: object) -> None:
            pass

        # The original function is returned, not wrapped
        assert my_func.__name__ == "my_func"
        assert callable(my_func)

    def test_compensate_config(self) -> None:
        @step(1, compensate="refund")
        async def charge(self: object) -> dict[str, object]:
            return {}

        assert charge.__gravtory_step__.compensate == "refund"

    def test_timeout_config(self) -> None:
        @step(1, timeout=timedelta(seconds=30))
        async def slow(self: object) -> None:
            pass

        assert slow.__gravtory_step__.timeout == timedelta(seconds=30)


class TestWorkflowDecorator:
    def test_class_creates_proxy(self) -> None:
        @workflow(id="test-{x}")
        class TestWF:
            @step(1)
            async def do_work(self, x: str) -> str:
                return x

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert isinstance(proxy, WorkflowProxy)
        assert proxy.definition.name == "TestWF"

    def test_steps_extracted(self) -> None:
        @workflow(id="test-{x}")
        class TestWF:
            @step(1)
            async def first(self) -> str:
                return "a"

            @step(2, depends_on=1)
            async def second(self) -> str:
                return "b"

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert 1 in proxy.definition.steps
        assert 2 in proxy.definition.steps
        assert proxy.definition.steps[1].name == "first"
        assert proxy.definition.steps[2].name == "second"

    def test_non_step_methods_ignored(self) -> None:
        @workflow(id="test")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

            def helper(self) -> str:
                return "not a step"

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert len(proxy.definition.steps) == 1

    def test_version_and_config(self) -> None:
        @workflow(id="test", version=2, priority=5, namespace="prod")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.definition.version == 2
        assert proxy.definition.config.priority == 5
        assert proxy.definition.config.namespace == "prod"

    def test_saga_flag(self) -> None:
        @workflow(id="test", saga=True)
        class TestWF:
            @step(1, compensate="undo")
            async def do_work(self) -> None:
                pass

            async def undo(self, output: object) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.definition.config.saga_enabled is True


class TestWorkflowProxy:
    def test_generate_id_simple(self) -> None:
        @workflow(id="order-{order_id}")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.generate_id(order_id="123") == "order-123"

    def test_generate_id_multiple_vars(self) -> None:
        @workflow(id="batch-{date}-{batch}")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.generate_id(date="2025-03", batch=1) == "batch-2025-03-1"

    def test_generate_id_missing_var(self) -> None:
        @workflow(id="order-{order_id}")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        with pytest.raises(ConfigurationError, match="requires parameter"):
            proxy.generate_id(wrong_param="123")

    def test_generate_id_no_variables(self) -> None:
        @workflow(id="static-id")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.generate_id() == "static-id"

    def test_repr(self) -> None:
        @workflow(id="test-{x}")
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        r = repr(TestWF)
        assert "WorkflowProxy" in r
        assert "TestWF" in r


class TestDecoratorGapFill:
    """Gap-fill tests for decorator edge cases."""

    def test_step_with_all_backoff_strategies(self) -> None:
        """Step decorator accepts all backoff strategies."""
        for strategy in ("constant", "linear", "exponential"):

            @step(1, retries=3, backoff=strategy)
            async def fn(self: object) -> None:
                pass

            assert fn.__gravtory_step__.backoff == strategy

    def test_step_order_uniqueness_not_enforced_by_decorator(self) -> None:
        """Decorator itself doesn't enforce order uniqueness (registry does)."""

        @step(1)
        async def a(self: object) -> None:
            pass

        @step(1)
        async def b(self: object) -> None:
            pass

        assert a.__gravtory_step__.order == b.__gravtory_step__.order

    def test_step_preserves_docstring(self) -> None:
        @step(1)
        async def documented(self: object) -> None:
            """This is my docstring."""
            pass

        assert documented.__doc__ == "This is my docstring."

    def test_workflow_with_deadline(self) -> None:
        @workflow(id="test", deadline=timedelta(minutes=30))
        class TestWF:
            @step(1)
            async def do_work(self) -> None:
                pass

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.definition.config.deadline == timedelta(minutes=30)

    def test_workflow_steps_have_correct_functions(self) -> None:
        """Each step definition retains its function reference."""

        @workflow(id="test")
        class TestWF:
            @step(1)
            async def first(self) -> str:
                return "a"

            @step(2, depends_on=1)
            async def second(self) -> str:
                return "b"

        proxy: WorkflowProxy = TestWF  # type: ignore[assignment]
        assert proxy.definition.steps[1].function is not None
        assert proxy.definition.steps[2].function is not None
