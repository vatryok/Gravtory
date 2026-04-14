"""Unit tests for core type definitions."""

from datetime import datetime, timedelta

from gravtory.core.types import (
    Compensation,
    DLQEntry,
    Lock,
    ParallelConfig,
    PendingStep,
    Schedule,
    ScheduleType,
    Signal,
    SignalConfig,
    SignalWait,
    StepDefinition,
    StepOutput,
    StepResult,
    StepStatus,
    WorkerInfo,
    WorkerStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)


class TestWorkflowStatus:
    def test_all_values_exist(self) -> None:
        expected = {
            "pending",
            "running",
            "completed",
            "failed",
            "compensating",
            "compensated",
            "compensation_failed",
            "cancelled",
        }
        assert {s.value for s in WorkflowStatus} == expected

    def test_string_value(self) -> None:
        assert WorkflowStatus.PENDING == "pending"
        assert WorkflowStatus.RUNNING == "running"
        assert WorkflowStatus.COMPLETED == "completed"
        assert WorkflowStatus.FAILED == "failed"

    def test_is_str_subclass(self) -> None:
        assert isinstance(WorkflowStatus.PENDING, str)


class TestStepStatus:
    def test_all_values_exist(self) -> None:
        expected = {"pending", "running", "completed", "failed", "skipped"}
        assert {s.value for s in StepStatus} == expected


class TestWorkerStatus:
    def test_all_values_exist(self) -> None:
        expected = {"active", "draining", "stopped"}
        assert {s.value for s in WorkerStatus} == expected


class TestScheduleType:
    def test_all_values_exist(self) -> None:
        expected = {"cron", "interval", "event", "one_time"}
        assert {s.value for s in ScheduleType} == expected


class TestWorkflowRun:
    def test_defaults(self) -> None:
        run = WorkflowRun(id="run-1", workflow_name="test")
        assert run.id == "run-1"
        assert run.workflow_name == "test"
        assert run.status == WorkflowStatus.PENDING
        assert run.workflow_version == 1
        assert run.namespace == "default"
        assert run.current_step is None
        assert run.input_data is None
        assert run.output_data is None
        assert run.error_message is None
        assert run.parent_run_id is None

    def test_custom_values(self) -> None:
        now = datetime.utcnow()
        run = WorkflowRun(
            id="run-2",
            workflow_name="order",
            status=WorkflowStatus.RUNNING,
            current_step=3,
            namespace="production",
            created_at=now,
        )
        assert run.status == WorkflowStatus.RUNNING
        assert run.current_step == 3
        assert run.namespace == "production"
        assert run.created_at == now


class TestStepOutput:
    def test_defaults(self) -> None:
        output = StepOutput()
        assert output.id is None
        assert output.workflow_run_id == ""
        assert output.step_order == 0
        assert output.status == StepStatus.COMPLETED
        assert output.retry_count == 0

    def test_with_data(self) -> None:
        output = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge_card",
            output_data=b'{"charge_id": "ch_123"}',
            output_type="dict",
            duration_ms=142,
        )
        assert output.step_name == "charge_card"
        assert output.output_data == b'{"charge_id": "ch_123"}'
        assert output.duration_ms == 142


class TestStepResult:
    def test_defaults(self) -> None:
        result = StepResult()
        assert result.output is None
        assert result.status == StepStatus.COMPLETED
        assert result.was_replayed is False
        assert result.duration_ms == 0
        assert result.retry_count == 0

    def test_replayed(self) -> None:
        result = StepResult(output={"charge_id": "ch_123"}, was_replayed=True)
        assert result.was_replayed is True
        assert result.output == {"charge_id": "ch_123"}


class TestPendingStep:
    def test_defaults(self) -> None:
        step = PendingStep()
        assert step.status == StepStatus.PENDING
        assert step.priority == 0
        assert step.retry_count == 0
        assert step.worker_id is None


class TestSignal:
    def test_defaults(self) -> None:
        sig = Signal()
        assert sig.consumed is False
        assert sig.signal_data is None

    def test_with_data(self) -> None:
        sig = Signal(
            workflow_run_id="run-1",
            signal_name="approval",
            signal_data=b'{"approved": true}',
        )
        assert sig.signal_name == "approval"


class TestSignalWait:
    def test_defaults(self) -> None:
        wait = SignalWait()
        assert wait.timeout_at is None


class TestCompensation:
    def test_defaults(self) -> None:
        comp = Compensation()
        assert comp.status == StepStatus.PENDING
        assert comp.handler_name == ""


class TestSchedule:
    def test_defaults(self) -> None:
        sched = Schedule()
        assert sched.schedule_type == ScheduleType.CRON
        assert sched.enabled is True
        assert sched.namespace == "default"


class TestLock:
    def test_defaults(self) -> None:
        lock = Lock()
        assert lock.lock_name == ""
        assert lock.holder_id == ""


class TestDLQEntry:
    def test_defaults(self) -> None:
        entry = DLQEntry()
        assert entry.retry_count == 0
        assert entry.error_message is None


class TestWorkerInfo:
    def test_defaults(self) -> None:
        worker = WorkerInfo()
        assert worker.status == WorkerStatus.ACTIVE
        assert worker.current_task is None


class TestWorkflowConfig:
    def test_defaults(self) -> None:
        config = WorkflowConfig()
        assert config.deadline is None
        assert config.priority == 0
        assert config.namespace == "default"
        assert config.saga_enabled is False
        assert config.version == 1

    def test_with_deadline(self) -> None:
        config = WorkflowConfig(deadline=timedelta(hours=1))
        assert config.deadline == timedelta(hours=1)


class TestStepDefinition:
    def test_defaults(self) -> None:
        step = StepDefinition()
        assert step.order == 0
        assert step.depends_on == []
        assert step.retries == 0
        assert step.timeout is None
        assert step.compensate is None
        assert step.parallel_config is None
        assert step.signal_config is None

    def test_mutable_defaults_are_independent(self) -> None:
        s1 = StepDefinition()
        s2 = StepDefinition()
        s1.depends_on.append(1)
        assert s2.depends_on == []


class TestParallelConfig:
    def test_defaults(self) -> None:
        config = ParallelConfig()
        assert config.max_concurrency == 10


class TestSignalConfig:
    def test_defaults(self) -> None:
        config = SignalConfig()
        assert config.name == ""
        assert config.timeout == timedelta(days=7)


class TestWorkflowDefinition:
    def test_defaults(self) -> None:
        defn = WorkflowDefinition()
        assert defn.name == ""
        assert defn.version == 1
        assert defn.steps == {}
        assert defn.input_schema is None
        assert defn.output_schema is None
        assert defn.workflow_class is None

    def test_mutable_defaults_are_independent(self) -> None:
        d1 = WorkflowDefinition()
        d2 = WorkflowDefinition()
        d1.steps[1] = StepDefinition(order=1)
        assert d2.steps == {}
