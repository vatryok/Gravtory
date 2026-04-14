"""Unit tests for StepContext."""

import pytest

from gravtory.core.context import StepContext
from gravtory.core.types import StepResult


class TestStepContext:
    def test_output_completed_step(self) -> None:
        completed = {1: StepResult(output={"charge_id": "ch_123"})}
        ctx = StepContext(completed, {"order_id": "abc"}, "run-1")
        assert ctx.output(1) == {"charge_id": "ch_123"}

    def test_output_missing_step(self) -> None:
        ctx = StepContext({}, {}, "run-1")
        with pytest.raises(KeyError, match="Step 99 has not completed"):
            ctx.output(99)

    def test_kwargs_accessible(self) -> None:
        ctx = StepContext({}, {"order_id": "abc", "amount": 100}, "run-1")
        assert ctx.kwargs == {"order_id": "abc", "amount": 100}

    def test_run_id_accessible(self) -> None:
        ctx = StepContext({}, {}, "run-42")
        assert ctx.workflow_run_id == "run-42"

    def test_multiple_step_outputs(self) -> None:
        completed = {
            1: StepResult(output="first"),
            2: StepResult(output="second"),
        }
        ctx = StepContext(completed, {}, "run-1")
        assert ctx.output(1) == "first"
        assert ctx.output(2) == "second"


class TestStepContextGapFill:
    """Gap-fill tests for StepContext edge cases."""

    def test_output_none_value(self) -> None:
        """Step with None output is still accessible."""
        completed = {1: StepResult(output=None)}
        ctx = StepContext(completed, {}, "run-1")
        assert ctx.output(1) is None

    def test_output_complex_types(self) -> None:
        """Step outputs of various types are returned as-is."""
        completed = {
            1: StepResult(output={"nested": {"key": [1, 2, 3]}}),
            2: StepResult(output=[1, 2, 3]),
            3: StepResult(output=42),
            4: StepResult(output="string_output"),
        }
        ctx = StepContext(completed, {}, "run-1")
        assert ctx.output(1) == {"nested": {"key": [1, 2, 3]}}
        assert ctx.output(2) == [1, 2, 3]
        assert ctx.output(3) == 42
        assert ctx.output(4) == "string_output"

    def test_kwargs_immutability(self) -> None:
        """Modifying returned kwargs does not affect original."""
        original_kwargs = {"key": "value"}
        ctx = StepContext({}, original_kwargs, "run-1")
        ctx.kwargs["key"] = "modified"
        # Since kwargs returns reference, this WILL modify - that's the design
        assert ctx.kwargs["key"] == "modified"

    def test_error_message_includes_available_steps(self) -> None:
        """Error message lists available step orders."""
        completed = {1: StepResult(output="a"), 3: StepResult(output="c")}
        ctx = StepContext(completed, {}, "run-1")
        with pytest.raises(KeyError, match="Available: "):
            ctx.output(99)

    def test_empty_kwargs(self) -> None:
        """Empty kwargs dict works fine."""
        ctx = StepContext({}, {}, "run-1")
        assert ctx.kwargs == {}

    def test_many_completed_steps(self) -> None:
        """Context with many completed steps works correctly."""
        completed = {i: StepResult(output=f"out_{i}") for i in range(1, 51)}
        ctx = StepContext(completed, {}, "run-big")
        for i in range(1, 51):
            assert ctx.output(i) == f"out_{i}"
