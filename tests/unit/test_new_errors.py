"""Tests for newly added error classes."""

from __future__ import annotations

from gravtory.core.errors import (
    StepAbortError,
    StepExhaustedError,
    StepOutputTypeError,
    StepRetryExhaustedError,
    ValidationError,
    WorkflowDeadlockError,
)


class TestWorkflowDeadlockError:
    def test_message(self) -> None:
        err = WorkflowDeadlockError("run-123")
        assert "run-123" in str(err)
        assert "deadlock" in str(err).lower()
        assert err.run_id == "run-123"


class TestStepAbortError:
    def test_message(self) -> None:
        original = ValueError("bad input")
        err = StepAbortError("my_step", original)
        assert "my_step" in str(err)
        assert "non-retryable" in str(err)
        assert err.original_error is original
        assert err.step_name == "my_step"


class TestStepOutputTypeError:
    def test_message(self) -> None:
        err = StepOutputTypeError("my_step", "dict", "str")
        assert "my_step" in str(err)
        assert "dict" in str(err)
        assert "str" in str(err)
        assert err.expected_type == "dict"
        assert err.actual_type == "str"


class TestStepExhaustedError:
    def test_is_alias(self) -> None:
        assert StepExhaustedError is StepRetryExhaustedError

    def test_can_instantiate_via_alias(self) -> None:
        err = StepExhaustedError("my_step", 3, None)
        assert "my_step" in str(err)
        assert err.last_error is None


class TestValidationError:
    def test_single_error_string(self) -> None:
        err = ValidationError("field is required")
        assert "field is required" in str(err)
        assert err.errors == ["field is required"]

    def test_multiple_errors(self) -> None:
        errors = ["name is empty", "age must be positive"]
        err = ValidationError(errors)
        msg = str(err)
        assert "name is empty" in msg
        assert "age must be positive" in msg
        assert err.errors == errors
