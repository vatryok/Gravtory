"""Gravtory error hierarchy.

All exceptions inherit from GravtoryError so users can catch
everything with a single except clause if desired.
"""

from __future__ import annotations

from typing import Any


class GravtoryError(Exception):
    """Base exception for all Gravtory errors."""

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        self.details = details or {}
        super().__init__(message)


# ── Workflow errors ──────────────────────────────────────────────


class WorkflowNotFoundError(GravtoryError):
    """Raised when a workflow definition cannot be found in the registry."""

    def __init__(self, workflow_name: str) -> None:
        self.workflow_name = workflow_name
        super().__init__(
            f"Workflow '{workflow_name}' is not registered. "
            f"Did you forget to decorate it with @workflow?"
        )


class WorkflowAlreadyExistsError(GravtoryError):
    """Raised when registering a workflow with a name that already exists."""

    def __init__(self, workflow_name: str) -> None:
        self.workflow_name = workflow_name
        super().__init__(
            f"Workflow '{workflow_name}' is already registered. Use a different name or version."
        )


class WorkflowRunNotFoundError(GravtoryError):
    """Raised when a workflow run cannot be found."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"Workflow run '{run_id}' not found.")


class WorkflowRunAlreadyExistsError(GravtoryError):
    """Raised when creating a run with an ID that already exists."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(
            f"Workflow run '{run_id}' already exists. "
            f"Use a unique ID or check if the workflow is already running."
        )


class WorkflowCancelledError(GravtoryError):
    """Raised when a workflow execution is cancelled."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"Workflow run '{run_id}' was cancelled.")


class WorkflowDeadlineExceededError(GravtoryError):
    """Raised when a workflow exceeds its deadline."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"Workflow run '{run_id}' exceeded its deadline.")


# ── Step errors ──────────────────────────────────────────────────


class StepError(GravtoryError):
    """Base exception for step execution errors."""

    def __init__(
        self,
        message: str,
        *,
        step_name: str | None = None,
        step_order: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.step_name = step_name
        self.step_order = step_order
        super().__init__(message, details=details)


class StepTimeoutError(StepError):
    """Raised when a step exceeds its timeout."""

    def __init__(self, step_name: str, timeout_seconds: float) -> None:
        super().__init__(
            f"Step '{step_name}' timed out after {timeout_seconds}s.",
            step_name=step_name,
        )


class StepRetryExhaustedError(StepError):
    """Raised when a step has exhausted all retry attempts."""

    def __init__(self, step_name: str, retries: int, last_error: Exception | None = None) -> None:
        self.last_error = last_error
        super().__init__(
            f"Step '{step_name}' failed after {retries} retries. Last error: {last_error}",
            step_name=step_name,
        )


class StepDependencyError(StepError):
    """Raised when a step's dependency has not been satisfied."""

    def __init__(self, step_name: str, depends_on: int) -> None:
        super().__init__(
            f"Step '{step_name}' depends on step {depends_on} which has not completed.",
            step_name=step_name,
        )


class StepConditionError(StepError):
    """Raised when a step's condition evaluation fails."""

    def __init__(self, step_name: str) -> None:
        super().__init__(
            f"Step '{step_name}' condition evaluation failed.",
            step_name=step_name,
        )


# ── Compensation / Saga errors ──────────────────────────────────


class CompensationError(GravtoryError):
    """Raised when a compensation handler fails during saga rollback."""

    def __init__(
        self,
        step_name: str,
        original_error: Exception | None = None,
    ) -> None:
        self.step_name = step_name
        self.original_error = original_error
        super().__init__(
            f"Compensation for step '{step_name}' failed. Original error: {original_error}"
        )


# ── Backend errors ───────────────────────────────────────────────


class BackendError(GravtoryError):
    """Base exception for backend / database errors."""


class BackendConnectionError(BackendError):
    """Raised when the backend cannot establish a connection."""

    def __init__(self, backend_name: str, reason: str) -> None:
        self.backend_name = backend_name
        super().__init__(
            f"Cannot connect to {backend_name} backend: {reason}. "
            f"Check your connection string and ensure the database is running."
        )


class BackendMigrationError(BackendError):
    """Raised when schema migration fails."""

    def __init__(self, backend_name: str, reason: str) -> None:
        self.backend_name = backend_name
        super().__init__(f"Migration failed for {backend_name}: {reason}")


class BackendLockError(BackendError):
    """Raised when a distributed lock cannot be acquired."""

    def __init__(self, lock_name: str) -> None:
        self.lock_name = lock_name
        super().__init__(f"Cannot acquire lock '{lock_name}'.")


# ── Serialization errors ────────────────────────────────────────


class SerializationError(GravtoryError):
    """Raised when data cannot be serialized or deserialized."""

    def __init__(self, message: str, *, data_type: str | None = None) -> None:
        self.data_type = data_type
        super().__init__(message)


# ── Signal errors ────────────────────────────────────────────────


class SignalError(GravtoryError):
    """Base exception for signal-related errors."""


class SignalTimeoutError(SignalError):
    """Raised when waiting for a signal exceeds the timeout."""

    def __init__(self, signal_name: str, timeout_seconds: float) -> None:
        self.signal_name = signal_name
        super().__init__(f"Timed out waiting for signal '{signal_name}' after {timeout_seconds}s.")


# ── Configuration errors ────────────────────────────────────────


class ConfigurationError(GravtoryError):
    """Raised when Gravtory is misconfigured."""


class InvalidWorkflowError(ConfigurationError):
    """Raised when a workflow definition is invalid."""

    def __init__(self, workflow_name: str, reason: str) -> None:
        self.workflow_name = workflow_name
        super().__init__(f"Invalid workflow '{workflow_name}': {reason}")
