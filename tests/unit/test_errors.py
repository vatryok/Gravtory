"""Unit tests for the error hierarchy."""

from gravtory.core.errors import (
    BackendConnectionError,
    BackendError,
    BackendLockError,
    BackendMigrationError,
    CompensationError,
    ConfigurationError,
    GravtoryError,
    InvalidWorkflowError,
    SerializationError,
    SignalError,
    SignalTimeoutError,
    StepConditionError,
    StepDependencyError,
    StepError,
    StepRetryExhaustedError,
    StepTimeoutError,
    WorkflowAlreadyExistsError,
    WorkflowCancelledError,
    WorkflowDeadlineExceededError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
    WorkflowRunNotFoundError,
)


class TestGravtoryError:
    def test_base_error(self) -> None:
        err = GravtoryError("something broke")
        assert str(err) == "something broke"
        assert err.details == {}

    def test_base_error_with_details(self) -> None:
        err = GravtoryError("fail", details={"key": "val"})
        assert err.details == {"key": "val"}

    def test_is_exception(self) -> None:
        assert issubclass(GravtoryError, Exception)


class TestWorkflowErrors:
    def test_not_found(self) -> None:
        err = WorkflowNotFoundError("my-wf")
        assert "my-wf" in str(err)
        assert err.workflow_name == "my-wf"
        assert isinstance(err, GravtoryError)

    def test_already_exists(self) -> None:
        err = WorkflowAlreadyExistsError("my-wf")
        assert err.workflow_name == "my-wf"
        assert isinstance(err, GravtoryError)

    def test_run_not_found(self) -> None:
        err = WorkflowRunNotFoundError("run-1")
        assert err.run_id == "run-1"
        assert isinstance(err, GravtoryError)

    def test_run_already_exists(self) -> None:
        err = WorkflowRunAlreadyExistsError("run-1")
        assert err.run_id == "run-1"

    def test_cancelled(self) -> None:
        err = WorkflowCancelledError("run-1")
        assert err.run_id == "run-1"

    def test_deadline_exceeded(self) -> None:
        err = WorkflowDeadlineExceededError("run-1")
        assert err.run_id == "run-1"


class TestStepErrors:
    def test_step_error_base(self) -> None:
        err = StepError("step broke", step_name="charge", step_order=1)
        assert err.step_name == "charge"
        assert err.step_order == 1
        assert isinstance(err, GravtoryError)

    def test_step_timeout(self) -> None:
        err = StepTimeoutError("charge", 30.0)
        assert "charge" in str(err)
        assert "30" in str(err)
        assert isinstance(err, StepError)

    def test_step_retry_exhausted(self) -> None:
        original = ValueError("bad value")
        err = StepRetryExhaustedError("charge", 3, last_error=original)
        assert err.last_error is original
        assert "3" in str(err)
        assert isinstance(err, StepError)

    def test_step_dependency(self) -> None:
        err = StepDependencyError("send_email", 2)
        assert "send_email" in str(err)
        assert isinstance(err, StepError)

    def test_step_condition(self) -> None:
        err = StepConditionError("optional_step")
        assert "optional_step" in str(err)
        assert isinstance(err, StepError)


class TestCompensationError:
    def test_compensation_error(self) -> None:
        original = RuntimeError("boom")
        err = CompensationError("refund", original_error=original)
        assert err.step_name == "refund"
        assert err.original_error is original
        assert isinstance(err, GravtoryError)


class TestBackendErrors:
    def test_backend_connection(self) -> None:
        err = BackendConnectionError("postgresql", "connection refused")
        assert err.backend_name == "postgresql"
        assert "connection refused" in str(err)
        assert isinstance(err, BackendError)
        assert isinstance(err, GravtoryError)

    def test_backend_migration(self) -> None:
        err = BackendMigrationError("sqlite", "table exists")
        assert err.backend_name == "sqlite"
        assert isinstance(err, BackendError)

    def test_backend_lock(self) -> None:
        err = BackendLockError("scheduler-lock")
        assert err.lock_name == "scheduler-lock"
        assert isinstance(err, BackendError)


class TestSerializationError:
    def test_serialization_error(self) -> None:
        err = SerializationError("cannot encode", data_type="dict")
        assert err.data_type == "dict"
        assert isinstance(err, GravtoryError)


class TestSignalErrors:
    def test_signal_timeout(self) -> None:
        err = SignalTimeoutError("approval", 3600.0)
        assert err.signal_name == "approval"
        assert "3600" in str(err)
        assert isinstance(err, SignalError)
        assert isinstance(err, GravtoryError)


class TestConfigurationErrors:
    def test_invalid_workflow(self) -> None:
        err = InvalidWorkflowError("bad-wf", "no steps defined")
        assert err.workflow_name == "bad-wf"
        assert "no steps defined" in str(err)
        assert isinstance(err, ConfigurationError)
        assert isinstance(err, GravtoryError)


class TestCatchAll:
    """Verify that all errors can be caught with a single except GravtoryError."""

    def test_catch_all_errors(self) -> None:
        error_classes = [
            lambda: WorkflowNotFoundError("x"),
            lambda: WorkflowAlreadyExistsError("x"),
            lambda: WorkflowRunNotFoundError("x"),
            lambda: WorkflowRunAlreadyExistsError("x"),
            lambda: WorkflowCancelledError("x"),
            lambda: WorkflowDeadlineExceededError("x"),
            lambda: StepError("x"),
            lambda: StepTimeoutError("x", 1.0),
            lambda: StepRetryExhaustedError("x", 1),
            lambda: StepDependencyError("x", 1),
            lambda: StepConditionError("x"),
            lambda: CompensationError("x"),
            lambda: BackendError("x"),
            lambda: BackendConnectionError("x", "y"),
            lambda: BackendMigrationError("x", "y"),
            lambda: BackendLockError("x"),
            lambda: SerializationError("x"),
            lambda: SignalError("x"),
            lambda: SignalTimeoutError("x", 1.0),
            lambda: ConfigurationError("x"),
            lambda: InvalidWorkflowError("x", "y"),
        ]
        for make_error in error_classes:
            err = make_error()
            assert isinstance(err, GravtoryError), f"{type(err).__name__} is not a GravtoryError"
