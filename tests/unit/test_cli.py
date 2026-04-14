"""Tests for the Gravtory CLI (Section 11.4)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from gravtory.cli.main import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestCLI:
    def test_version_command(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert "gravtory" in result.output
        assert "1.0.0" in result.output

    def test_help_shows_commands(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "workflows" in result.output
        assert "dlq" in result.output
        assert "workers" in result.output
        assert "schedules" in result.output
        assert "version" in result.output
        assert "status" in result.output
        assert "init" in result.output
        assert "dashboard" in result.output

    def test_status_command(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.count = AsyncMock(return_value=0)
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "status"])
            assert result.exit_code == 0
            assert "Backend:" in result.output

    def test_status_json_output(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.count = AsyncMock(return_value=3)
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "status", "--json"])
            assert result.exit_code == 0
            assert '"running": 3' in result.output
            assert '"failed": 3' in result.output

    def test_init_command(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "init"])
            assert result.exit_code == 0
            assert "Database initialized" in result.output

    def test_workflows_list(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.list = AsyncMock(return_value=[])
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "workflows", "list"])
            assert result.exit_code == 0
            assert "Total: 0 runs" in result.output

    def test_workflows_list_json(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.list = AsyncMock(return_value=[])
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "workflows", "list", "--json"])
            assert result.exit_code == 0
            assert "[]" in result.output

    def test_dlq_list(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.backend = AsyncMock()
            grav.backend.list_dlq = AsyncMock(return_value=[])
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "dlq", "list"])
            assert result.exit_code == 0
            assert "Total: 0 entries" in result.output

    def test_backend_option(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.count = AsyncMock(return_value=0)
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "postgresql://localhost/test", "status"])
            assert result.exit_code == 0
            mock_grav.assert_called_with("postgresql://localhost/test")

    def test_workers_list(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.backend = AsyncMock()
            grav.backend.list_workers = AsyncMock(return_value=[])
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "workers", "list"])
            assert result.exit_code == 0

    def test_schedules_list(self, runner: CliRunner) -> None:
        with patch("gravtory.cli.main._make_grav") as mock_grav:
            grav = AsyncMock()
            grav.backend = AsyncMock()
            grav.backend.list_all_schedules = AsyncMock(return_value=[])
            mock_grav.return_value = grav

            result = runner.invoke(cli, ["-b", "memory://", "schedules", "list"])
            assert result.exit_code == 0


class TestCLIGapFill:
    """Gap-fill tests for CLI edge cases."""

    def test_unknown_command(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["nonexistent-command"])
        assert result.exit_code != 0

    def test_workflows_help(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["workflows", "--help"])
        assert result.exit_code == 0
        assert "list" in result.output

    def test_dlq_help(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["dlq", "--help"])
        assert result.exit_code == 0
