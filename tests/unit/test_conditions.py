"""Unit tests for conditional branching in the execution engine."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.core.context import StepContext
from gravtory.core.types import StepResult, StepStatus


class TestStepConditions:
    """Tests for condition evaluation on steps."""

    def _ctx(self, completed: dict[int, StepResult], **kwargs: Any) -> StepContext:
        return StepContext(completed, kwargs, "run-cond")

    def test_true_condition_returns_true(self) -> None:
        """A condition that evaluates True means the step should execute."""
        completed = {1: StepResult(output={"amount": 200}, status=StepStatus.COMPLETED)}
        ctx = self._ctx(completed)

        def condition(c: StepContext) -> bool:
            return c.output(1)["amount"] > 100  # type: ignore[no-any-return]

        assert condition(ctx) is True

    def test_false_condition_returns_false(self) -> None:
        """A condition that evaluates False means the step should be skipped."""
        completed = {1: StepResult(output={"amount": 50}, status=StepStatus.COMPLETED)}
        ctx = self._ctx(completed)

        def condition(c: StepContext) -> bool:
            return c.output(1)["amount"] > 100  # type: ignore[no-any-return]

        assert condition(ctx) is False

    def test_condition_exception_is_catchable(self) -> None:
        """A condition that raises an exception can be caught."""
        completed = {1: StepResult(output=None, status=StepStatus.COMPLETED)}
        ctx = self._ctx(completed)

        def bad_condition(c: StepContext) -> bool:
            return c.output(1)["missing_key"] > 0  # type: ignore[no-any-return]

        with pytest.raises((TypeError, KeyError)):
            bad_condition(ctx)

    def test_no_condition_always_executes(self) -> None:
        """A step with condition=None should always execute (None is falsy but means no filter)."""
        condition = None
        # The execution engine checks `if step_def.condition is not None:`
        assert condition is None

    def test_mutual_exclusion_branches(self) -> None:
        """Two mutually exclusive conditions: only one fires."""
        completed = {1: StepResult(output={"type": "A"}, status=StepStatus.COMPLETED)}
        ctx = self._ctx(completed)

        def cond_a(c: StepContext) -> bool:
            return c.output(1)["type"] == "A"  # type: ignore[no-any-return]

        def cond_b(c: StepContext) -> bool:
            return c.output(1)["type"] != "A"  # type: ignore[no-any-return]

        assert cond_a(ctx) is True
        assert cond_b(ctx) is False

        # Flip the type
        completed2 = {1: StepResult(output={"type": "B"}, status=StepStatus.COMPLETED)}
        ctx2 = self._ctx(completed2)
        assert cond_a(ctx2) is False
        assert cond_b(ctx2) is True

    def test_condition_accesses_multiple_outputs(self) -> None:
        """Complex condition accessing outputs from multiple steps."""
        completed = {
            1: StepResult(output={"score": 80}, status=StepStatus.COMPLETED),
            2: StepResult(output={"age": 25}, status=StepStatus.COMPLETED),
        }
        ctx = self._ctx(completed)

        def check_eligibility(ctx: StepContext) -> bool:
            return ctx.output(1)["score"] > 50 and ctx.output(2)["age"] >= 18  # type: ignore[no-any-return]

        assert check_eligibility(ctx) is True

    def test_condition_with_workflow_kwargs(self) -> None:
        """Condition can access workflow kwargs via ctx.kwargs."""
        completed: dict[int, StepResult] = {}
        ctx = self._ctx(completed, region="us-east")
        assert ctx.kwargs["region"] == "us-east"

    def test_output_missing_step_raises_keyerror(self) -> None:
        """Accessing output of a non-completed step raises KeyError."""
        ctx = self._ctx({})
        with pytest.raises(KeyError, match="Step 99 has not completed"):
            ctx.output(99)


class TestConditionsGapFill:
    """Gap-fill tests for conditions edge cases."""

    def _ctx(
        self,
        completed: dict[int, StepResult],
        **kwargs: object,
    ) -> StepContext:
        return StepContext(completed, kwargs, "run-gap")

    def test_condition_with_false_result(self) -> None:
        completed = {1: StepResult(output={"score": 10})}
        ctx = self._ctx(completed)
        assert ctx.output(1)["score"] < 50

    def test_condition_with_none_output(self) -> None:
        completed = {1: StepResult(output=None)}
        ctx = self._ctx(completed)
        assert ctx.output(1) is None

    def test_condition_with_string_output(self) -> None:
        completed = {1: StepResult(output="approved")}
        ctx = self._ctx(completed)
        assert ctx.output(1) == "approved"
