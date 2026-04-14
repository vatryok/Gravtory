"""Core type definitions used throughout the Gravtory library."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class WorkflowStatus(str, Enum):
    """Status of a workflow run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Status of a step execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkerStatus(str, Enum):
    """Status of a worker."""

    ACTIVE = "active"
    DRAINING = "draining"
    STOPPED = "stopped"


class ScheduleType(str, Enum):
    """Type of schedule."""

    CRON = "cron"
    INTERVAL = "interval"
    EVENT = "event"
    ONE_TIME = "one_time"


@dataclass
class WorkflowRun:
    """Represents a single workflow execution."""

    id: str
    workflow_name: str
    workflow_version: int = 1
    namespace: str = "default"
    status: WorkflowStatus = WorkflowStatus.PENDING
    current_step: int | None = None
    input_data: bytes | None = None
    output_data: bytes | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    parent_run_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None
    deadline_at: datetime | None = None


@dataclass
class StepOutput:
    """Persisted output of a completed step."""

    id: int | None = None
    workflow_run_id: str = ""
    step_order: int = 0
    step_name: str = ""
    output_data: bytes | None = None
    output_type: str | None = None
    duration_ms: int | None = None
    retry_count: int = 0
    status: StepStatus = StepStatus.COMPLETED
    error_message: str | None = None
    created_at: datetime | None = None


@dataclass
class StepResult:
    """Runtime result returned after executing a step."""

    output: Any = None
    status: StepStatus = StepStatus.COMPLETED
    was_replayed: bool = False
    duration_ms: int = 0
    retry_count: int = 0


@dataclass
class PendingStep:
    """A step queued for execution by a worker."""

    id: int | None = None
    workflow_run_id: str = ""
    step_order: int = 0
    priority: int = 0
    status: StepStatus = StepStatus.PENDING
    worker_id: str | None = None
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0
    max_retries: int = 0
    next_retry_at: datetime | None = None
    created_at: datetime | None = None


@dataclass
class Signal:
    """A signal sent to a workflow run."""

    id: int | None = None
    workflow_run_id: str = ""
    signal_name: str = ""
    signal_data: bytes | None = None
    consumed: bool = False
    created_at: datetime | None = None


@dataclass
class SignalWait:
    """Record that a step is waiting for a signal."""

    id: int | None = None
    workflow_run_id: str = ""
    signal_name: str = ""
    timeout_at: datetime | None = None
    created_at: datetime | None = None


@dataclass
class Compensation:
    """A compensation record for saga rollback."""

    id: int | None = None
    workflow_run_id: str = ""
    step_order: int = 0
    handler_name: str = ""
    step_output: bytes | None = None
    status: StepStatus = StepStatus.PENDING
    error_message: str | None = None
    created_at: datetime | None = None


@dataclass
class Schedule:
    """A registered schedule for automatic workflow triggering."""

    id: str = ""
    workflow_name: str = ""
    schedule_type: ScheduleType = ScheduleType.CRON
    schedule_config: str = ""
    namespace: str = "default"
    enabled: bool = True
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None
    created_at: datetime | None = None


@dataclass
class Lock:
    """A distributed lock record."""

    lock_name: str = ""
    holder_id: str = ""
    acquired_at: datetime | None = None
    expires_at: datetime | None = None


@dataclass
class DLQEntry:
    """An entry in the dead letter queue."""

    id: int | None = None
    workflow_run_id: str = ""
    step_order: int = 0
    error_message: str | None = None
    error_traceback: str | None = None
    step_input: bytes | None = None
    retry_count: int = 0
    created_at: datetime | None = None


@dataclass
class WorkerInfo:
    """Information about a registered worker."""

    worker_id: str = ""
    node_id: str = ""
    status: WorkerStatus = WorkerStatus.ACTIVE
    last_heartbeat: datetime | None = None
    current_task: str | None = None
    started_at: datetime | None = None


@dataclass
class WorkflowConfig:
    """Configuration for a workflow definition."""

    deadline: timedelta | None = None
    priority: int = 0
    namespace: str = "default"
    saga_enabled: bool = False
    version: int = 1


@dataclass
class StepDefinition:
    """Definition of a single step within a workflow."""

    order: int = 0
    name: str = ""
    depends_on: list[int] = field(default_factory=list)
    timeout: timedelta | None = None
    retries: int = 0
    backoff: str | None = None
    backoff_base: float = 1.0
    backoff_max: float = 300.0
    backoff_multiplier: float = 2.0
    jitter: bool = False
    retry_on: list[type[Exception]] = field(default_factory=list)
    abort_on: list[type[Exception]] = field(default_factory=list)
    compensate: str | None = None
    condition: Callable[..., bool] | None = None
    parallel_config: ParallelConfig | None = None
    signal_config: SignalConfig | None = None
    rate_limit: str | None = None
    input_types: dict[str, type] = field(default_factory=dict)
    output_type: type | None = None
    function: Callable[..., Any] | None = None


@dataclass
class ParallelConfig:
    """Configuration for parallel step execution."""

    max_concurrency: int = 10


@dataclass
class SignalConfig:
    """Configuration for a signal-waiting step."""

    name: str = ""
    timeout: timedelta = field(default_factory=lambda: timedelta(days=7))


@dataclass
class WorkflowDefinition:
    """Complete definition of a registered workflow."""

    name: str = ""
    version: int = 1
    steps: dict[int, StepDefinition] = field(default_factory=dict)
    input_schema: type | None = None
    output_schema: type | None = None
    config: WorkflowConfig = field(default_factory=WorkflowConfig)
    workflow_class: type | None = None
