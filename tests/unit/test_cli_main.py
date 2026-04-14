"""Tests for cli.main — CLI commands via click CliRunner."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from gravtory.cli.main import _format_status, cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _mock_grav(**overrides: object) -> MagicMock:
    """Create a mock Gravtory instance with sensible defaults."""
    grav = MagicMock()
    grav.start = AsyncMock()
    grav.shutdown = AsyncMock()
    grav.count = AsyncMock(return_value=0)
    grav.list = AsyncMock(return_value=[])
    grav.inspect = AsyncMock()
    grav.signal = AsyncMock()
    grav.backend = MagicMock()
    grav.backend.get_step_outputs = AsyncMock(return_value=[])
    grav.backend.list_dlq = AsyncMock(return_value=[])
    grav.backend.remove_from_dlq = AsyncMock()
    grav.backend.get_dlq_entry = AsyncMock(return_value=None)
    grav.backend.list_workers = AsyncMock(return_value=[])
    grav.backend.list_all_schedules = AsyncMock(return_value=[])
    grav.backend.save_schedule = AsyncMock()
    grav.backend.update_workflow_status = AsyncMock()
    grav.backend.validated_update_workflow_status = AsyncMock()
    grav.backend.purge_dlq = AsyncMock(return_value=0)
    grav.engine = MagicMock()
    grav.engine.cancel_workflow = AsyncMock(return_value=["run-1"])
    for k, v in overrides.items():
        setattr(grav, k, v)
    return grav


class TestFormatStatus:
    def test_completed(self) -> None:
        result = _format_status("completed")
        assert "completed" in result

    def test_failed(self) -> None:
        result = _format_status("failed")
        assert "failed" in result

    def test_running(self) -> None:
        result = _format_status("running")
        assert "running" in result

    def test_unknown(self) -> None:
        result = _format_status("weird")
        assert "weird" in result


class TestVersionCommand:
    def test_version(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert "gravtory" in result.output


class TestStatusCommand:
    def test_status_text(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(cli, ["--backend", "sqlite://:memory:", "status"])
        assert result.exit_code == 0
        assert "Backend:" in result.output

    def test_status_json(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        grav.count = AsyncMock(side_effect=[5, 3, 100, 2])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(cli, ["--backend", "sqlite://:memory:", "status", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["running"] == 5


class TestInitCommand:
    def test_init(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(cli, ["--backend", "sqlite://:memory:", "init"])
        assert result.exit_code == 0
        assert "initialized" in result.output


class TestWorkflowsListCommand:
    def test_list_empty(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(cli, ["--backend", "sqlite://:memory:", "workflows", "list"])
        assert result.exit_code == 0
        assert "Total: 0" in result.output

    def test_list_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import WorkflowRun, WorkflowStatus

        run = WorkflowRun(
            id="run-1",
            workflow_name="MyWF",
            status=WorkflowStatus.COMPLETED,
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        grav = _mock_grav()
        grav.list = AsyncMock(return_value=[run])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "list",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["id"] == "run-1"

    def test_list_with_filters(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "list",
                    "--status",
                    "failed",
                    "--workflow",
                    "MyWF",
                    "--limit",
                    "5",
                ],
            )
        assert result.exit_code == 0

    def test_list_text_with_runs(self, runner: CliRunner) -> None:
        from gravtory.core.types import WorkflowRun, WorkflowStatus

        run = WorkflowRun(
            id="run-1",
            workflow_name="MyWF",
            status=WorkflowStatus.RUNNING,
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        grav = _mock_grav()
        grav.list = AsyncMock(return_value=[run])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "list",
                ],
            )
        assert result.exit_code == 0
        assert "run-1" in result.output


class TestWorkflowsInspectCommand:
    def test_inspect_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus

        run = WorkflowRun(
            id="run-1",
            workflow_name="MyWF",
            status=WorkflowStatus.COMPLETED,
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        step = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=50,
        )
        grav = _mock_grav()
        grav.inspect = AsyncMock(return_value=run)
        grav.backend.get_step_outputs = AsyncMock(return_value=[step])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "inspect",
                    "run-1",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["id"] == "run-1"
        assert len(data["steps"]) == 1

    def test_inspect_text(self, runner: CliRunner) -> None:
        from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus

        run = WorkflowRun(
            id="run-1",
            workflow_name="MyWF",
            status=WorkflowStatus.FAILED,
            error_message="boom",
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        step = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="s1",
            status=StepStatus.COMPLETED,
            duration_ms=50,
        )
        grav = _mock_grav()
        grav.inspect = AsyncMock(return_value=run)
        grav.backend.get_step_outputs = AsyncMock(return_value=[step])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "inspect",
                    "run-1",
                ],
            )
        assert result.exit_code == 0
        assert "run-1" in result.output
        assert "boom" in result.output
        assert "Steps:" in result.output

    def test_inspect_not_found(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        grav.inspect = AsyncMock(side_effect=Exception("Not found"))
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "inspect",
                    "nonexistent",
                ],
            )
        assert result.exit_code != 0


class TestWorkflowsRetryCommand:
    def test_retry(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "retry",
                    "run-1",
                ],
            )
        assert result.exit_code == 0
        assert "retry" in result.output


class TestWorkflowsCancelCommand:
    def test_cancel(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "cancel",
                    "run-1",
                ],
            )
        assert result.exit_code == 0
        assert "cancelled" in result.output


class TestWorkflowsCountCommand:
    def test_count(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        grav.count = AsyncMock(return_value=42)
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "count",
                ],
            )
        assert result.exit_code == 0
        assert "42" in result.output

    def test_count_with_filters(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        grav.count = AsyncMock(return_value=5)
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workflows",
                    "count",
                    "--status",
                    "failed",
                    "--workflow",
                    "MyWF",
                ],
            )
        assert result.exit_code == 0
        assert "5" in result.output


class TestStepsListCommand:
    def test_steps_list_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import StepOutput, StepStatus

        step = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge",
            status=StepStatus.COMPLETED,
            duration_ms=100,
            retry_count=0,
        )
        grav = _mock_grav()
        grav.backend.get_step_outputs = AsyncMock(return_value=[step])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "steps",
                    "list",
                    "run-1",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["name"] == "charge"

    def test_steps_list_text(self, runner: CliRunner) -> None:
        from gravtory.core.types import StepOutput, StepStatus

        step = StepOutput(
            workflow_run_id="run-1",
            step_order=1,
            step_name="charge",
            status=StepStatus.COMPLETED,
            duration_ms=100,
            retry_count=0,
        )
        grav = _mock_grav()
        grav.backend.get_step_outputs = AsyncMock(return_value=[step])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "steps",
                    "list",
                    "run-1",
                ],
            )
        assert result.exit_code == 0
        assert "charge" in result.output


class TestSignalSendCommand:
    def test_signal_send(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "signal",
                    "send",
                    "run-1",
                    "approval",
                    "--data",
                    '{"approved": true}',
                ],
            )
        assert result.exit_code == 0
        assert "sent" in result.output


class TestDlqCommands:
    def test_dlq_list_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import DLQEntry

        entry = DLQEntry(
            id=1,
            workflow_run_id="run-1",
            step_order=2,
            error_message="boom",
            retry_count=0,
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        grav = _mock_grav()
        grav.backend.list_dlq = AsyncMock(return_value=[entry])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "dlq",
                    "list",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1

    def test_dlq_list_text(self, runner: CliRunner) -> None:
        from gravtory.core.types import DLQEntry

        entry = DLQEntry(
            id=1,
            workflow_run_id="run-1",
            step_order=2,
            error_message="boom",
            retry_count=0,
        )
        grav = _mock_grav()
        grav.backend.list_dlq = AsyncMock(return_value=[entry])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "dlq",
                    "list",
                ],
            )
        assert result.exit_code == 0
        assert "Total:" in result.output

    def test_dlq_retry(self, runner: CliRunner) -> None:
        from gravtory.core.types import DLQEntry

        entry = DLQEntry(id=1, workflow_run_id="run-1", step_order=0, error_message="e")
        grav = _mock_grav()
        grav.backend.list_dlq = AsyncMock(return_value=[entry])
        grav.backend.get_dlq_entry = AsyncMock(return_value=entry)
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "dlq",
                    "retry",
                    "1",
                ],
            )
        assert result.exit_code == 0
        assert "removed" in result.output

    def test_dlq_purge(self, runner: CliRunner) -> None:
        from gravtory.core.types import DLQEntry

        entry = DLQEntry(id=1, workflow_run_id="run-1", step_order=0, error_message="e")
        grav = _mock_grav()
        grav.backend.list_dlq = AsyncMock(return_value=[entry])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "dlq",
                    "purge",
                ],
            )
        assert result.exit_code == 0
        assert "Purged" in result.output


class TestWorkersListCommand:
    def test_workers_list_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import WorkerInfo, WorkerStatus

        w = WorkerInfo(
            worker_id="w-1",
            node_id="node-1",
            status=WorkerStatus.ACTIVE,
            last_heartbeat=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        grav = _mock_grav()
        grav.backend.list_workers = AsyncMock(return_value=[w])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workers",
                    "list",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1

    def test_workers_list_text(self, runner: CliRunner) -> None:
        from gravtory.core.types import WorkerInfo, WorkerStatus

        w = WorkerInfo(
            worker_id="w-1",
            node_id="node-1",
            status=WorkerStatus.ACTIVE,
            last_heartbeat=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        grav = _mock_grav()
        grav.backend.list_workers = AsyncMock(return_value=[w])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "workers",
                    "list",
                ],
            )
        assert result.exit_code == 0
        assert "w-1" in result.output


class TestSchedulesCommands:
    def test_schedules_list_json(self, runner: CliRunner) -> None:
        from gravtory.core.types import Schedule, ScheduleType

        s = Schedule(
            id="sched-1",
            workflow_name="MyWF",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        grav = _mock_grav()
        grav.backend.list_all_schedules = AsyncMock(return_value=[s])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "schedules",
                    "list",
                    "--json",
                ],
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1

    def test_schedules_list_text(self, runner: CliRunner) -> None:
        from gravtory.core.types import Schedule, ScheduleType

        s = Schedule(
            id="sched-1",
            workflow_name="MyWF",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        grav = _mock_grav()
        grav.backend.list_all_schedules = AsyncMock(return_value=[s])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "schedules",
                    "list",
                ],
            )
        assert result.exit_code == 0
        assert "MyWF" in result.output

    def test_schedules_toggle_found(self, runner: CliRunner) -> None:
        from gravtory.core.types import Schedule, ScheduleType

        s = Schedule(
            id="sched-1",
            workflow_name="MyWF",
            schedule_type=ScheduleType.CRON,
            schedule_config="*/5 * * * *",
            enabled=True,
        )
        grav = _mock_grav()
        grav.backend.list_all_schedules = AsyncMock(return_value=[s])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "schedules",
                    "toggle",
                    "sched-1",
                ],
            )
        assert result.exit_code == 0
        assert "disabled" in result.output

    def test_schedules_toggle_not_found(self, runner: CliRunner) -> None:
        grav = _mock_grav()
        grav.backend.list_all_schedules = AsyncMock(return_value=[])
        with patch("gravtory.cli.main._make_grav", return_value=grav):
            result = runner.invoke(
                cli,
                [
                    "--backend",
                    "sqlite://:memory:",
                    "schedules",
                    "toggle",
                    "nonexistent",
                ],
            )
        assert result.exit_code != 0
