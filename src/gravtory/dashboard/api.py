# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""REST API endpoints for the Gravtory dashboard.

Each handler receives the aiohttp request and delegates to the backend.
All responses are JSON.  Dataclass instances are serialized via
:func:`_serialize_dataclass`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from aiohttp import web

from gravtory.core.errors import GravtoryError
from gravtory.core.types import Signal, WorkflowStatus

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.registry import WorkflowRegistry

logger = logging.getLogger("gravtory.dashboard.api")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json(data: Any, *, status: int = 200) -> web.Response:
    """Build a JSON response."""
    return web.json_response(data, status=status)


_ERROR_CODES: dict[int, str] = {
    400: "BAD_REQUEST",
    404: "NOT_FOUND",
    409: "CONFLICT",
    413: "PAYLOAD_TOO_LARGE",
    429: "TOO_MANY_REQUESTS",
}


def _error(
    message: str, *, status: int = 400, details: dict[str, Any] | None = None
) -> web.Response:
    """Build a structured JSON error response."""
    body: dict[str, Any] = {
        "error": {
            "code": _ERROR_CODES.get(status, "ERROR"),
            "message": message,
        }
    }
    if details:
        body["error"]["details"] = details
    return web.json_response(body, status=status)


def _serialize_value(val: Any) -> Any:
    """Recursively convert non-JSON-serializable values."""
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, Enum):
        return val.value
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, dict):
        return {k: _serialize_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_serialize_value(v) for v in val]
    return val


def _serialize_dataclass(obj: Any) -> dict[str, Any]:
    """Convert a dataclass instance to a JSON-safe dict."""
    raw = asdict(obj)
    return {k: _serialize_value(v) for k, v in raw.items()}


# ---------------------------------------------------------------------------
# Route setup
# ---------------------------------------------------------------------------


def setup_routes(
    app: web.Application,
    backend: Backend,
    registry: WorkflowRegistry,
    audit_logger: Any | None = None,
) -> None:
    """Register all API routes on the aiohttp application."""
    handlers = _APIHandlers(backend, registry, audit_logger=audit_logger)

    # Static
    app.router.add_get("/", handlers.serve_dashboard)

    # Workflows
    app.router.add_get("/api/workflows", handlers.list_workflows)
    app.router.add_get("/api/workflows/{run_id}", handlers.get_workflow)
    app.router.add_post("/api/workflows/{run_id}/retry", handlers.retry_workflow)
    app.router.add_post("/api/workflows/{run_id}/cancel", handlers.cancel_workflow)
    app.router.add_get("/api/workflows/{run_id}/steps", handlers.get_steps)
    app.router.add_post(
        "/api/workflows/{run_id}/signals/{name}",
        handlers.send_signal,
    )

    # DLQ
    app.router.add_get("/api/dlq", handlers.list_dlq)
    app.router.add_post("/api/dlq/{entry_id}/retry", handlers.retry_dlq)
    app.router.add_delete("/api/dlq", handlers.purge_dlq)

    # Workers
    app.router.add_get("/api/workers", handlers.list_workers)

    # Schedules
    app.router.add_get("/api/schedules", handlers.list_schedules)
    app.router.add_post("/api/schedules/{schedule_id}/toggle", handlers.toggle_schedule)

    # Audit log
    app.router.add_get("/api/audit", handlers.list_audit_log)

    # Stats / Health / SSE / OpenAPI
    app.router.add_get("/api/stats", handlers.get_stats)
    app.router.add_get("/api/health", handlers.health)
    app.router.add_get("/api/openapi.json", handlers.openapi_spec)
    app.router.add_get("/api/events", handlers.sse_events)


class _APIHandlers:
    """Container for all API handler methods."""

    def __init__(
        self, backend: Backend, registry: WorkflowRegistry, *, audit_logger: Any | None = None
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._audit_logger = audit_logger
        self._start_time = time.monotonic()
        # Shared SSE poller state
        self._sse_queues: list[asyncio.Queue[list[dict[str, Any]]]] = []
        self._sse_poller_task: asyncio.Task[None] | None = None
        self._max_sse_connections: int = 50

    # ------------------------------------------------------------------
    # Static
    # ------------------------------------------------------------------

    async def serve_dashboard(self, request: web.Request) -> web.Response:
        """Serve the single-file HTML dashboard."""
        import importlib.resources as pkg_resources
        import secrets

        try:
            ref = pkg_resources.files("gravtory.dashboard") / "static" / "index.html"
            html = ref.read_text(encoding="utf-8")
        except (FileNotFoundError, TypeError, ModuleNotFoundError):
            html = (
                "<html><body><h1>Gravtory Dashboard</h1><p>HTML file not found.</p></body></html>"
            )
        nonce = secrets.token_urlsafe(16)
        html = html.replace("<script>", f'<script nonce="{nonce}">', 1)
        csp = (
            f"default-src 'self'; "
            f"script-src 'nonce-{nonce}'; "
            f"style-src 'self' 'unsafe-inline'; "
            f"connect-src 'self'; "
            f"img-src 'self' data:; "
            f"frame-ancestors 'none'"
        )
        return web.Response(
            text=html,
            content_type="text/html",
            headers={"Content-Security-Policy": csp},
        )

    # ------------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------------

    async def list_workflows(self, request: web.Request) -> web.Response:
        """GET /api/workflows — list runs with optional filters."""
        params = request.query
        status_str = params.get("status")
        try:
            status = WorkflowStatus(status_str) if status_str else None
        except ValueError:
            return _error(f"Invalid status: {status_str}", status=400)
        workflow_name = params.get("workflow_name")
        namespace = params.get("namespace", "default")
        try:
            limit = max(1, min(int(params.get("limit", "50")), 1000))
            offset = max(0, int(params.get("offset", "0")))
        except (ValueError, TypeError):
            return _error("limit and offset must be integers", status=400)

        runs, total = await asyncio.gather(
            self._backend.list_workflow_runs(
                namespace=namespace,
                status=status,
                workflow_name=workflow_name,
                limit=limit,
                offset=offset,
            ),
            self._backend.count_workflow_runs(
                namespace=namespace,
                status=status,
                workflow_name=workflow_name,
            ),
        )
        return _json(
            {
                "workflows": [_serialize_dataclass(r) for r in runs],
                "count": len(runs),
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        )

    async def get_workflow(self, request: web.Request) -> web.Response:
        """GET /api/workflows/{run_id}"""
        run_id = request.match_info["run_id"]
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            return _error("Workflow run not found", status=404)

        steps = await self._backend.get_step_outputs(run_id)
        data = _serialize_dataclass(run)
        data["steps"] = [_serialize_dataclass(s) for s in steps]
        return _json(data)

    async def retry_workflow(self, request: web.Request) -> web.Response:
        """POST /api/workflows/{run_id}/retry"""
        run_id = request.match_info["run_id"]
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            return _error("Workflow run not found", status=404)
        try:
            await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.PENDING)
        except GravtoryError as exc:
            return _error(str(exc), status=409)
        return _json({"status": "retrying", "run_id": run_id})

    async def cancel_workflow(self, request: web.Request) -> web.Response:
        """POST /api/workflows/{run_id}/cancel"""
        run_id = request.match_info["run_id"]
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            return _error("Workflow run not found", status=404)
        try:
            await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.CANCELLED)
        except GravtoryError as exc:
            return _error(str(exc), status=409)
        return _json({"status": "cancelled", "run_id": run_id})

    async def get_steps(self, request: web.Request) -> web.Response:
        """GET /api/workflows/{run_id}/steps"""
        run_id = request.match_info["run_id"]
        steps = await self._backend.get_step_outputs(run_id)
        return _json({"steps": [_serialize_dataclass(s) for s in steps]})

    _MAX_SIGNAL_PAYLOAD = 64 * 1024  # 64 KB
    _SIGNAL_NAME_RE = __import__("re").compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")

    async def send_signal(self, request: web.Request) -> web.Response:
        """POST /api/workflows/{run_id}/signals/{name}"""
        run_id = request.match_info["run_id"]
        name = request.match_info["name"]

        if not self._SIGNAL_NAME_RE.match(name):
            return _error(
                "Invalid signal name. Use alphanumeric, _, -, . (max 128 chars).",
                status=400,
            )

        body: dict[str, Any] = {}
        if request.can_read_body:
            with contextlib.suppress(Exception):
                body = await request.json()

        signal_data = json.dumps(body.get("data", {})).encode()
        if len(signal_data) > self._MAX_SIGNAL_PAYLOAD:
            return _error(
                f"Signal payload exceeds {self._MAX_SIGNAL_PAYLOAD} byte limit.",
                status=413,
            )

        sig = Signal(
            workflow_run_id=run_id,
            signal_name=name,
            signal_data=signal_data,
        )
        await self._backend.send_signal(sig)
        return _json({"status": "signal_sent", "signal_name": name, "run_id": run_id})

    # ------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------

    async def list_dlq(self, request: web.Request) -> web.Response:
        """GET /api/dlq"""
        try:
            limit = max(1, min(int(request.query.get("limit", "100")), 1000))
        except (ValueError, TypeError):
            return _error("limit must be an integer", status=400)
        entries = await self._backend.list_dlq(limit=limit)
        return _json(
            {
                "entries": [_serialize_dataclass(e) for e in entries],
                "total": len(entries),
            }
        )

    async def retry_dlq(self, request: web.Request) -> web.Response:
        """POST /api/dlq/{entry_id}/retry"""
        try:
            entry_id = int(request.match_info["entry_id"])
        except (ValueError, TypeError):
            return _error("entry_id must be an integer", status=400)
        entry = await self._backend.get_dlq_entry(entry_id)
        if entry is None:
            return _error("DLQ entry not found", status=404)
        try:
            await self._backend.validated_update_workflow_status(
                entry.workflow_run_id, WorkflowStatus.PENDING
            )
        except GravtoryError as exc:
            return _error(str(exc), status=409)
        await self._backend.remove_from_dlq(entry_id)
        return _json({"status": "retrying", "entry_id": entry_id, "run_id": entry.workflow_run_id})

    async def purge_dlq(self, request: web.Request) -> web.Response:
        """DELETE /api/dlq"""
        count = await self._backend.purge_dlq()
        return _json({"deleted": count})

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def list_workers(self, request: web.Request) -> web.Response:
        """GET /api/workers"""
        workers = await self._backend.list_workers()
        return _json({"workers": [_serialize_dataclass(w) for w in workers]})

    # ------------------------------------------------------------------
    # Schedules
    # ------------------------------------------------------------------

    async def list_schedules(self, request: web.Request) -> web.Response:
        """GET /api/schedules"""
        schedules = await self._backend.list_all_schedules()
        return _json({"schedules": [_serialize_dataclass(s) for s in schedules]})

    async def toggle_schedule(self, request: web.Request) -> web.Response:
        """POST /api/schedules/{schedule_id}/toggle"""
        schedule_id = request.match_info["schedule_id"]
        found = await self._backend.get_schedule(schedule_id)

        if found is None:
            return _error("Schedule not found", status=404)

        found.enabled = not found.enabled
        await self._backend.save_schedule(found)
        return _json({"id": schedule_id, "enabled": found.enabled})

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    async def list_audit_log(self, request: web.Request) -> web.Response:
        """GET /api/audit?action=&resource_type=&limit=100"""
        if self._audit_logger is None:
            return _json({"entries": [], "message": "Audit logging not configured"})

        action = request.query.get("action")
        resource_type = request.query.get("resource_type")
        actor = request.query.get("actor")
        limit = min(int(request.query.get("limit", "100")), 1000)

        entries = await self._audit_logger.query(
            action=action or None,
            resource_type=resource_type or None,
            actor=actor or None,
            limit=limit,
        )
        return _json(
            {
                "entries": [_serialize_dataclass(e) for e in entries],
                "total": len(entries),
            }
        )

    # ------------------------------------------------------------------
    # Stats / Health / SSE
    # ------------------------------------------------------------------

    async def get_stats(self, request: web.Request) -> web.Response:
        """GET /api/stats"""
        namespace = request.query.get("namespace", "default")
        # Parallelize independent DB queries to reduce latency
        gather_results = await asyncio.gather(
            self._backend.count_workflow_runs(namespace=namespace),
            self._backend.count_workflow_runs(namespace=namespace, status=WorkflowStatus.RUNNING),
            self._backend.count_workflow_runs(namespace=namespace, status=WorkflowStatus.FAILED),
            self._backend.count_workflow_runs(namespace=namespace, status=WorkflowStatus.COMPLETED),
            self._backend.count_dlq(),
            self._backend.list_workers(),
            self._backend.get_all_enabled_schedules(),
        )
        total = gather_results[0]
        running = gather_results[1]
        failed = gather_results[2]
        completed = gather_results[3]
        dlq = gather_results[4]
        workers_list: list[Any] = list(gather_results[5])  # type: ignore[call-overload]
        schedules_list: list[Any] = list(gather_results[6])  # type: ignore[call-overload]

        return _json(
            {
                "total_workflows": total,
                "running": running,
                "failed": failed,
                "completed": completed,
                "dlq_size": dlq,
                "active_workers": len(workers_list),
                "schedules": len(schedules_list),
            }
        )

    async def health(self, request: web.Request) -> web.Response:
        """GET /api/health"""
        healthy = await self._backend.health_check()
        uptime = time.monotonic() - self._start_time
        return _json(
            {
                "status": "healthy" if healthy else "unhealthy",
                "backend": type(self._backend).__name__,
                "uptime_seconds": round(uptime, 1),
            }
        )

    async def openapi_spec(self, request: web.Request) -> web.Response:
        """GET /api/openapi.json — serve OpenAPI specification."""
        import importlib.resources as pkg_resources

        try:
            ref = pkg_resources.files("gravtory.dashboard") / "static" / "openapi.json"
            spec = ref.read_text(encoding="utf-8")
        except (FileNotFoundError, TypeError, ModuleNotFoundError):
            return _error("OpenAPI spec not found", status=404)
        return web.Response(text=spec, content_type="application/json")

    async def sse_events(self, request: web.Request) -> web.StreamResponse:
        """GET /api/events — Server-Sent Events stream.

        Uses a shared poller task so that N connected clients share a single
        DB poll instead of each client issuing its own queries.
        """
        if len(self._sse_queues) >= self._max_sse_connections:
            return _error(
                "Too many SSE connections",
                status=429,
                details={"max": self._max_sse_connections},
            )

        response = web.StreamResponse()
        response.content_type = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Connection"] = "keep-alive"
        await response.prepare(request)

        queue: asyncio.Queue[list[dict[str, Any]]] = asyncio.Queue(maxsize=32)
        self._sse_queues.append(queue)

        # Start the shared poller if not running
        if self._sse_poller_task is None or self._sse_poller_task.done():
            self._sse_poller_task = asyncio.create_task(self._sse_poller_loop())

        try:
            while True:
                events = await queue.get()
                for evt in events:
                    payload = (
                        f"event: {evt['type']}\ndata: {json.dumps(evt['data'], default=str)}\n\n"
                    )
                    await response.write(payload.encode())
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self._sse_queues.remove(queue)
        return response

    async def _sse_poller_loop(self) -> None:
        """Single poller that fans events out to all connected SSE queues."""
        last_check = datetime.now(tz=timezone.utc)
        while True:
            if not self._sse_queues:
                break
            try:
                events = await asyncio.wait_for(
                    self._get_recent_events(last_check),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                continue
            last_check = datetime.now(tz=timezone.utc)
            if events:
                for q in list(self._sse_queues):
                    with contextlib.suppress(asyncio.QueueFull):
                        q.put_nowait(events)
            await asyncio.sleep(2)

    async def _get_recent_events(
        self,
        since: datetime,
    ) -> list[dict[str, Any]]:
        """Poll backend for workflow changes since *since*.

        Queries per-status to avoid fetching idle workflows and reduce
        the amount of data scanned on each poll cycle.
        """
        events: list[dict[str, Any]] = []
        event_map: dict[WorkflowStatus, str] = {
            WorkflowStatus.RUNNING: "workflow_started",
            WorkflowStatus.COMPLETED: "workflow_completed",
            WorkflowStatus.FAILED: "workflow_failed",
        }
        for status, event_type in event_map.items():
            runs = await self._backend.list_workflow_runs(status=status, limit=20)
            for run in runs:
                if run.updated_at is not None and run.updated_at > since:
                    data: dict[str, Any] = {"run_id": run.id}
                    if status == WorkflowStatus.RUNNING:
                        data["workflow"] = run.workflow_name
                    elif status == WorkflowStatus.COMPLETED:
                        data["status"] = "completed"
                    elif status == WorkflowStatus.FAILED:
                        data["error"] = run.error_message
                    events.append({"type": event_type, "data": data})
        return events
