"""E2E tests — CLI commands via click's CliRunner.

Tests the ``gravtory`` CLI end-to-end: init, status, workflows list,
workflows inspect — with a real SQLite database.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from gravtory.cli.main import cli
from gravtory.core.engine import Gravtory
from gravtory.decorators.step import step
from gravtory.decorators.workflow import workflow

pytestmark = pytest.mark.e2e


# ── Fixture workflow ─────────────────────────────────────────────


@workflow(id="cli-wf-{tag}")
class CLITestWorkflow:
    @step(1)
    async def compute(self, tag: str) -> dict[str, str]:
        return {"result": f"done-{tag}"}


# ── Helper ───────────────────────────────────────────────────────


async def _seed_db(db_path: str) -> None:
    """Run a workflow so the DB has data for CLI to query."""
    grav = Gravtory(backend=f"sqlite:///{db_path}")
    await grav.start()
    await grav.run(CLITestWorkflow, tag="abc")
    await grav.shutdown()


# ── Tests ────────────────────────────────────────────────────────


class TestCLIVersion:
    def test_version_command(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert "gravtory" in result.output


class TestCLIInit:
    def test_init_creates_tables(self, tmp_path: Path) -> None:
        db = str(tmp_path / "init_test.db")
        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "init"])
        assert result.exit_code == 0
        assert "initialized" in result.output.lower()


class TestCLIStatus:
    def test_status_text(self, tmp_path: Path) -> None:
        db = str(tmp_path / "status_test.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "status"])
        assert result.exit_code == 0

    def test_status_json(self, tmp_path: Path) -> None:
        db = str(tmp_path / "status_json.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "status", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "completed" in data
        assert isinstance(data["completed"], int)


class TestCLIWorkflowsList:
    def test_list_text(self, tmp_path: Path) -> None:
        db = str(tmp_path / "list_test.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "workflows", "list"])
        assert result.exit_code == 0
        assert "cli-wf-abc" in result.output

    def test_list_json(self, tmp_path: Path) -> None:
        db = str(tmp_path / "list_json.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "workflows", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) >= 1
        assert data[0]["id"] == "cli-wf-abc"


class TestCLIWorkflowsInspect:
    def test_inspect_text(self, tmp_path: Path) -> None:
        db = str(tmp_path / "inspect_test.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(cli, ["-b", f"sqlite:///{db}", "workflows", "inspect", "cli-wf-abc"])
        assert result.exit_code == 0
        assert "cli-wf-abc" in result.output

    def test_inspect_json(self, tmp_path: Path) -> None:
        db = str(tmp_path / "inspect_json.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(
            cli, ["-b", f"sqlite:///{db}", "workflows", "inspect", "cli-wf-abc", "--json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["id"] == "cli-wf-abc"
        assert data["status"] == "completed"

    def test_inspect_nonexistent_run(self, tmp_path: Path) -> None:
        db = str(tmp_path / "inspect_404.db")
        import asyncio

        asyncio.run(_seed_db(db))

        runner = CliRunner()
        result = runner.invoke(
            cli, ["-b", f"sqlite:///{db}", "workflows", "inspect", "nonexistent"]
        )
        assert result.exit_code != 0
