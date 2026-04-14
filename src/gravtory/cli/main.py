# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Gravtory CLI — manage workflows, workers, and schedules from the terminal.

Entry point: ``gravtory`` (configured in pyproject.toml).
"""

from __future__ import annotations

import asyncio
import json as json_mod
import sys
from typing import Any

import click

from gravtory._version import __version__


def _run_async(coro: Any) -> Any:
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


def _make_grav(backend: str) -> Any:
    """Create a Gravtory instance from a connection string."""
    from gravtory.core.engine import Gravtory

    return Gravtory(backend)


def _format_status(status: str) -> str:
    """Color-code a workflow status string."""
    colors: dict[str, str] = {
        "completed": "green",
        "failed": "red",
        "running": "blue",
        "pending": "yellow",
        "cancelled": "white",
        "compensating": "magenta",
        "compensated": "magenta",
        "compensation_failed": "red",
    }
    return click.style(status, fg=colors.get(status, "white"))


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group()
@click.option(
    "--backend",
    "-b",
    envvar="GRAVTORY_BACKEND",
    default="sqlite:///gravtory.db",
    help="Database connection string (or set GRAVTORY_BACKEND env var).",
)
@click.pass_context
def cli(ctx: click.Context, backend: str) -> None:
    """Gravtory — crash-proof workflow engine CLI."""
    ctx.ensure_object(dict)
    ctx.obj["backend"] = backend


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------


@cli.command()
def version() -> None:
    """Print Gravtory version."""
    click.echo(f"gravtory {__version__}")


@cli.command()
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"]))
def completion(shell: str) -> None:
    """Print shell completion script.

    Usage:
      gravtory completion bash >> ~/.bashrc
      gravtory completion zsh >> ~/.zshrc
      gravtory completion fish > ~/.config/fish/completions/gravtory.fish
    """
    import os

    env_var = "_GRAVTORY_COMPLETE"
    shell_map = {"bash": "bash_source", "zsh": "zsh_source", "fish": "fish_source"}
    os.environ[env_var] = shell_map[shell]
    try:
        cli.main(standalone_mode=False)
    except SystemExit:
        pass
    finally:
        os.environ.pop(env_var, None)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def status(ctx: click.Context, as_json: bool) -> None:
    """Show system status summary."""

    async def _status() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        stats = {
            "running": await grav.count(status="running"),
            "pending": await grav.count(status="pending"),
            "completed": await grav.count(status="completed"),
            "failed": await grav.count(status="failed"),
        }
        await grav.shutdown()

        if as_json:
            click.echo(json_mod.dumps(stats, indent=2))
        else:
            click.echo(f"Backend: {ctx.obj['backend']}")
            for name, cnt in stats.items():
                click.echo(f"  {_format_status(name)}: {cnt}")

    _run_async(_status())


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Initialize database tables."""

    async def _init() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        click.echo(f"Database initialized: {ctx.obj['backend']}")
        await grav.shutdown()

    _run_async(_init())


# ---------------------------------------------------------------------------
# workflows
# ---------------------------------------------------------------------------


@cli.group()
def workflows() -> None:
    """Manage workflow runs."""


@workflows.command("list")
@click.option("--status", "-s", "status_filter", default=None, help="Filter by status.")
@click.option("--workflow", "-w", "workflow_name", default=None, help="Filter by workflow name.")
@click.option("--limit", "-l", default=20, help="Max results.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def workflows_list(
    ctx: click.Context,
    status_filter: str | None,
    workflow_name: str | None,
    limit: int,
    as_json: bool,
) -> None:
    """List workflow runs."""

    async def _list() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        runs = await grav.list(
            status=status_filter,
            workflow=workflow_name,
            limit=limit,
        )
        await grav.shutdown()

        if as_json:
            data = [
                {
                    "id": r.id,
                    "workflow_name": r.workflow_name,
                    "status": r.status.value if hasattr(r.status, "value") else str(r.status),
                    "created_at": str(r.created_at),
                }
                for r in runs
            ]
            click.echo(json_mod.dumps(data, indent=2))
        else:
            click.echo(f"{'ID':<36} {'Workflow':<25} {'Status':<20} {'Created'}")
            click.echo("-" * 100)
            for r in runs:
                st = r.status.value if hasattr(r.status, "value") else str(r.status)
                click.echo(
                    f"{r.id:<36} {r.workflow_name:<25} {_format_status(st):<29} {r.created_at}"
                )
            click.echo(f"\nTotal: {len(runs)} runs")

    _run_async(_list())


@workflows.command("inspect")
@click.argument("run_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def workflows_inspect(ctx: click.Context, run_id: str, as_json: bool) -> None:
    """Inspect a specific workflow run."""

    async def _inspect() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        try:
            run = await grav.inspect(run_id)
        except Exception as exc:
            click.echo(f"Error: {exc}", err=True)
            await grav.shutdown()
            sys.exit(1)

        steps = await grav.backend.get_step_outputs(run_id)
        await grav.shutdown()

        if as_json:
            data: dict[str, Any] = {
                "id": run.id,
                "workflow_name": run.workflow_name,
                "status": run.status.value if hasattr(run.status, "value") else str(run.status),
                "created_at": str(run.created_at),
                "completed_at": str(run.completed_at),
                "error_message": run.error_message,
                "steps": [
                    {
                        "order": s.step_order,
                        "name": s.step_name,
                        "status": s.status.value if hasattr(s.status, "value") else str(s.status),
                        "duration_ms": s.duration_ms,
                    }
                    for s in steps
                ],
            }
            click.echo(json_mod.dumps(data, indent=2))
        else:
            st = run.status.value if hasattr(run.status, "value") else str(run.status)
            click.echo(f"Run ID:    {run.id}")
            click.echo(f"Workflow:  {run.workflow_name}")
            click.echo(f"Status:    {_format_status(st)}")
            click.echo(f"Created:   {run.created_at}")
            click.echo(f"Completed: {run.completed_at or 'N/A'}")
            if run.error_message:
                click.echo(f"Error:     {run.error_message}")
            if steps:
                click.echo("\nSteps:")
                for s in steps:
                    s_st = s.status.value if hasattr(s.status, "value") else str(s.status)
                    dur = f"{s.duration_ms}ms" if s.duration_ms else "—"
                    click.echo(
                        f"  {s.step_order}. {s.step_name:<20} {_format_status(s_st):<29} {dur}"
                    )

    _run_async(_inspect())


@workflows.command("retry")
@click.argument("run_id")
@click.pass_context
def workflows_retry(ctx: click.Context, run_id: str) -> None:
    """Retry a failed workflow run."""

    async def _retry() -> None:
        from gravtory.core.types import WorkflowStatus

        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        await grav.backend.validated_update_workflow_status(run_id, WorkflowStatus.PENDING)
        click.echo(f"Workflow {run_id} queued for retry.")
        await grav.shutdown()

    _run_async(_retry())


@workflows.command("cancel")
@click.argument("run_id")
@click.pass_context
def workflows_cancel(ctx: click.Context, run_id: str) -> None:
    """Cancel a workflow run."""

    async def _cancel() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        cancelled = await grav.engine.cancel_workflow(run_id, propagate=True)
        for cid in cancelled:
            click.echo(f"Workflow {cid} cancelled.")
        if not cancelled:
            click.echo(f"Workflow {run_id} was not in a cancellable state.")
        await grav.shutdown()

    _run_async(_cancel())


@workflows.command("count")
@click.option("--status", "-s", "status_filter", default=None, help="Filter by status.")
@click.option("--workflow", "-w", "workflow_name", default=None, help="Filter by workflow name.")
@click.pass_context
def workflows_count(
    ctx: click.Context,
    status_filter: str | None,
    workflow_name: str | None,
) -> None:
    """Count workflow runs."""

    async def _count() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        cnt = await grav.count(status=status_filter, workflow=workflow_name)
        click.echo(str(cnt))
        await grav.shutdown()

    _run_async(_count())


# ---------------------------------------------------------------------------
# steps
# ---------------------------------------------------------------------------


@cli.group()
def steps() -> None:
    """Manage workflow steps."""


@steps.command("list")
@click.argument("run_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def steps_list(ctx: click.Context, run_id: str, as_json: bool) -> None:
    """List steps for a workflow run."""

    async def _list() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        step_outputs = await grav.backend.get_step_outputs(run_id)
        await grav.shutdown()

        if as_json:
            data = [
                {
                    "order": s.step_order,
                    "name": s.step_name,
                    "status": s.status.value if hasattr(s.status, "value") else str(s.status),
                    "duration_ms": s.duration_ms,
                    "retry_count": s.retry_count,
                }
                for s in step_outputs
            ]
            click.echo(json_mod.dumps(data, indent=2))
        else:
            click.echo(f"{'#':<5} {'Name':<25} {'Status':<20} {'Duration':<12} {'Retries'}")
            click.echo("-" * 70)
            for s in step_outputs:
                st = s.status.value if hasattr(s.status, "value") else str(s.status)
                dur = f"{s.duration_ms}ms" if s.duration_ms else "—"
                click.echo(
                    f"{s.step_order:<5} {s.step_name:<25} {_format_status(st):<29} {dur:<12} {s.retry_count}"
                )

    _run_async(_list())


# ---------------------------------------------------------------------------
# signal
# ---------------------------------------------------------------------------


@cli.group()
def signal() -> None:
    """Send signals to workflows."""


@signal.command("send")
@click.argument("run_id")
@click.argument("name")
@click.option("--data", "-d", default="{}", help="Signal data as JSON string.")
@click.pass_context
def signal_send(ctx: click.Context, run_id: str, name: str, data: str) -> None:
    """Send a signal to a workflow run."""

    async def _send() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        parsed = json_mod.loads(data)
        signal_bytes = json_mod.dumps(parsed).encode()
        await grav.signal(run_id, name, data=signal_bytes)
        click.echo(f"Signal '{name}' sent to {run_id}.")
        await grav.shutdown()

    _run_async(_send())


# ---------------------------------------------------------------------------
# dlq
# ---------------------------------------------------------------------------


@cli.group()
def dlq() -> None:
    """Manage the dead letter queue."""


@dlq.command("list")
@click.option("--limit", "-l", default=100, help="Max results.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def dlq_list(ctx: click.Context, limit: int, as_json: bool) -> None:
    """List DLQ entries."""

    async def _list() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        entries = await grav.backend.list_dlq(limit=limit)
        await grav.shutdown()

        if as_json:
            data = [
                {
                    "id": e.id,
                    "workflow_run_id": e.workflow_run_id,
                    "step_order": e.step_order,
                    "error_message": e.error_message,
                    "retry_count": e.retry_count,
                    "created_at": str(e.created_at),
                }
                for e in entries
            ]
            click.echo(json_mod.dumps(data, indent=2))
        else:
            click.echo(f"{'ID':<8} {'Run ID':<36} {'Step':<6} {'Error':<30} {'Retries'}")
            click.echo("-" * 90)
            for e in entries:
                err = (e.error_message or "—")[:28]
                click.echo(
                    f"{e.id!s:<8} {e.workflow_run_id:<36} {e.step_order:<6} {err:<30} {e.retry_count}"
                )
            click.echo(f"\nTotal: {len(entries)} entries")

    _run_async(_list())


@dlq.command("retry")
@click.argument("entry_id", type=int)
@click.pass_context
def dlq_retry(ctx: click.Context, entry_id: int) -> None:
    """Retry a DLQ entry."""

    async def _retry() -> None:
        from gravtory.core.types import WorkflowStatus

        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        # O(1) lookup by ID instead of scanning all entries
        target = await grav.backend.get_dlq_entry(entry_id)
        if target is None:
            click.echo(f"DLQ entry {entry_id} not found.", err=True)
            await grav.shutdown()
            return
        # Re-enqueue the workflow for retry by resetting status to PENDING
        await grav.backend.update_workflow_status(target.workflow_run_id, WorkflowStatus.PENDING)
        await grav.backend.remove_from_dlq(entry_id)
        click.echo(
            f"DLQ entry {entry_id} removed. Workflow '{target.workflow_run_id}' queued for retry."
        )
        await grav.shutdown()

    _run_async(_retry())


@dlq.command("purge")
@click.pass_context
def dlq_purge(ctx: click.Context) -> None:
    """Purge all DLQ entries."""

    async def _purge() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        count = await grav.backend.purge_dlq()
        click.echo(f"Purged {count} DLQ entries.")
        await grav.shutdown()

    _run_async(_purge())


# ---------------------------------------------------------------------------
# workers
# ---------------------------------------------------------------------------


@cli.group()
def workers() -> None:
    """Manage workers."""


@workers.command("list")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def workers_list(ctx: click.Context, as_json: bool) -> None:
    """List registered workers."""

    async def _list() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        worker_list = await grav.backend.list_workers()
        await grav.shutdown()

        if as_json:
            data = [
                {
                    "worker_id": w.worker_id,
                    "node_id": w.node_id,
                    "status": w.status.value if hasattr(w.status, "value") else str(w.status),
                    "last_heartbeat": str(w.last_heartbeat),
                    "current_task": w.current_task,
                }
                for w in worker_list
            ]
            click.echo(json_mod.dumps(data, indent=2))
        else:
            click.echo(
                f"{'Worker ID':<25} {'Node':<20} {'Status':<12} {'Last Heartbeat':<25} {'Task'}"
            )
            click.echo("-" * 95)
            for w in worker_list:
                st = w.status.value if hasattr(w.status, "value") else str(w.status)
                click.echo(
                    f"{w.worker_id:<25} {w.node_id:<20} {st:<12} "
                    f"{w.last_heartbeat!s:<25} {w.current_task or '—'}"
                )

    _run_async(_list())


# ---------------------------------------------------------------------------
# schedules
# ---------------------------------------------------------------------------


@cli.group()
def schedules() -> None:
    """Manage schedules."""


@schedules.command("list")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def schedules_list(ctx: click.Context, as_json: bool) -> None:
    """List schedules."""

    async def _list() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        sched_list = await grav.backend.list_all_schedules()
        await grav.shutdown()

        if as_json:
            data = [
                {
                    "id": s.id,
                    "workflow_name": s.workflow_name,
                    "schedule_type": s.schedule_type.value
                    if hasattr(s.schedule_type, "value")
                    else str(s.schedule_type),
                    "schedule_config": s.schedule_config,
                    "enabled": s.enabled,
                    "last_run_at": str(s.last_run_at),
                    "next_run_at": str(s.next_run_at),
                }
                for s in sched_list
            ]
            click.echo(json_mod.dumps(data, indent=2))
        else:
            click.echo(f"{'Workflow':<25} {'Type':<10} {'Config':<20} {'Enabled':<10} {'Next Run'}")
            click.echo("-" * 85)
            for s in sched_list:
                st = (
                    s.schedule_type.value
                    if hasattr(s.schedule_type, "value")
                    else str(s.schedule_type)
                )
                enabled = (
                    click.style("On", fg="green") if s.enabled else click.style("Off", fg="red")
                )
                click.echo(
                    f"{s.workflow_name:<25} {st:<10} {s.schedule_config:<20} "
                    f"{enabled:<19} {s.next_run_at or '—'}"
                )

    _run_async(_list())


@schedules.command("toggle")
@click.argument("schedule_id")
@click.pass_context
def schedules_toggle(ctx: click.Context, schedule_id: str) -> None:
    """Toggle a schedule on/off."""

    async def _toggle() -> None:
        grav = _make_grav(ctx.obj["backend"])
        await grav.start()
        all_scheds = await grav.backend.list_all_schedules()
        found = None
        for s in all_scheds:
            if s.id == schedule_id:
                found = s
                break
        if found is None:
            click.echo(f"Schedule '{schedule_id}' not found.", err=True)
            await grav.shutdown()
            sys.exit(1)
        found.enabled = not found.enabled
        await grav.backend.save_schedule(found)
        state = "enabled" if found.enabled else "disabled"
        click.echo(f"Schedule '{schedule_id}' {state}.")
        await grav.shutdown()

    _run_async(_toggle())


# ---------------------------------------------------------------------------
# dashboard
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--port", "-p", default=7777, help="Dashboard port.")
@click.option("--host", "-H", "host", default="127.0.0.1", help="Dashboard host.")
@click.pass_context
def dashboard(ctx: click.Context, port: int, host: str) -> None:
    """Start the web dashboard."""

    async def _dashboard() -> None:
        from gravtory.dashboard.server import Dashboard

        grav = _make_grav(ctx.obj["backend"])
        await grav.start()

        import os

        auth_token = os.environ.get("GRAVTORY_DASHBOARD_TOKEN")
        dash = Dashboard(grav.backend, grav.registry, host=host, port=port, auth_token=auth_token)
        await dash.start()
        click.echo(f"Dashboard running at http://{host}:{port} — press Ctrl+C to stop")

        try:
            while True:
                await asyncio.sleep(3600)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await dash.stop()
            await grav.shutdown()

    _run_async(_dashboard())


# ---------------------------------------------------------------------------
# dev (Hot Reload)
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("script")
@click.pass_context
def dev(ctx: click.Context, script: str) -> None:
    """Start a script in hot-reload mode."""
    import subprocess

    try:
        from watchfiles import watch
    except ImportError:
        click.echo(
            "watchfiles is required for hot reload. Install with: pip install watchfiles", err=True
        )
        sys.exit(1)

    click.echo(
        click.style(
            f"Gravtory DEV mode starting: watching for changes to reload {script}", fg="cyan"
        )
    )

    process = None

    def start_process() -> None:
        nonlocal process
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
        click.echo(click.style(f"[GRAVTORY DEV] Starting process {script}...", fg="yellow"))
        process = subprocess.Popen([sys.executable, script])

    start_process()

    try:
        for changes in watch(  # type: ignore[call-arg]
            ".",
            watch_filter=lambda change, path: path.endswith(".py"),
            ignore_entity=lambda path: (
                any(
                    part
                    in {
                        ".git",
                        "__pycache__",
                        ".venv",
                        "venv",
                        "node_modules",
                        ".mypy_cache",
                        ".ruff_cache",
                        ".pytest_cache",
                        ".hypothesis",
                        "dist",
                        "build",
                        ".eggs",
                        "*.egg-info",
                    }
                    for part in path.parts
                )
                if hasattr(path, "parts")
                else False
            ),
        ):
            if any(path.endswith(".py") for _, path in changes):
                click.echo(click.style("[GRAVTORY DEV] Detected changes. Reloading...", fg="cyan"))
                start_process()
    except KeyboardInterrupt:
        if process is not None:
            process.terminate()
        click.echo(click.style("[GRAVTORY DEV] Shutting down.", fg="cyan"))
