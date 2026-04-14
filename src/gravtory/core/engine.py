# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Gravtory main class — the user-facing entry point."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import contextvars
import logging
import os
import re
import socket
from typing import TYPE_CHECKING, Any

from gravtory.core.errors import ConfigurationError, GravtoryError
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import WorkflowRun, WorkflowStatus
from gravtory.decorators.step import step as step_decorator
from gravtory.decorators.workflow import WorkflowProxy
from gravtory.decorators.workflow import workflow as workflow_decorator

builtins_list = builtins.list

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta

    from gravtory.backends.base import Backend

logger = logging.getLogger("gravtory")


class Gravtory:
    """Main user-facing class.

    Usage:
        grav = Gravtory("postgresql://localhost/mydb")
        await grav.start()
        result = await grav.run(MyWorkflow, order_id="abc")
    """

    def __init__(
        self,
        backend: str | Backend = "sqlite:///gravtory.db",
        *,
        workers: int = 0,
        node_id: str | None = None,
        namespace: str = "default",
        serializer: str = "json",
        compression: str | None = None,
        encryption_key: str | None = None,
        scheduler: bool = False,
        dashboard: bool = False,
        dashboard_port: int = 7777,
        dashboard_token: str | None = None,
        otel_endpoint: str | None = None,
        metrics_port: int | None = None,
        on_failure: Callable[..., Any] | None = None,
        table_prefix: str = "gravtory_",
        pickle_allowed_classes: set[str] | None = None,
    ) -> None:
        # Validate table_prefix to prevent SQL injection via identifier
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_prefix):
            raise ConfigurationError(
                f"Invalid table_prefix: {table_prefix!r}. "
                f"Must contain only alphanumeric characters and underscores, "
                f"and start with a letter or underscore."
            )

        # Parse backend
        if isinstance(backend, str):
            self._backend = self._create_backend(backend, table_prefix=table_prefix)
        else:
            self._backend = backend

        # Create checkpoint pipeline (serialize → compress → encrypt)
        from gravtory.core.checkpoint import CheckpointEngine

        self._checkpoint = CheckpointEngine(
            serializer=serializer,
            compression=compression,
            encryption_key=encryption_key,
            pickle_allowed_classes=pickle_allowed_classes,
        )

        self._registry = WorkflowRegistry()
        self._engine = ExecutionEngine(self._registry, self._backend, self._checkpoint)

        # Config
        self._namespace = namespace
        self._workers_count = workers
        self._node_id = node_id or f"{socket.gethostname()}-{os.getpid()}"
        self._serializer = serializer
        self._compression = compression
        self._encryption_key = encryption_key
        self._scheduler_enabled = scheduler
        self._dashboard_enabled = dashboard
        self._dashboard_port = dashboard_port
        self._dashboard_token = dashboard_token
        self._otel_endpoint = otel_endpoint
        self._metrics_port = metrics_port
        self._on_failure = on_failure
        self._table_prefix = table_prefix

        self._pending_workflows: list[WorkflowProxy] = []
        self._started = False
        self._active_tasks: set[asyncio.Task[Any]] = set()
        self._drain_timeout = 30.0  # seconds to wait for in-flight work

        # Subsystem instances (created in start())
        self._worker_pool: Any = None
        self._scheduler_engine: Any = None
        self._dashboard: Any = None

    @staticmethod
    def _create_backend(url: str, table_prefix: str = "gravtory_") -> Backend:
        """Create a backend from a connection string."""
        if url.startswith("memory://") or url == ":memory:":
            from gravtory.backends.memory import InMemoryBackend

            return InMemoryBackend()
        from gravtory.backends import create_backend

        return create_backend(url, table_prefix=table_prefix)

    # ── Lifecycle ────────────────────────────────────────────────

    async def __aenter__(self) -> Gravtory:
        """Support ``async with Gravtory(...) as grav:`` pattern."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: type | None, exc: BaseException | None, tb: Any) -> None:
        """Shutdown on context exit."""
        await self.shutdown()

    async def start(self) -> None:
        """Start the Gravtory engine and all configured subsystems."""
        await self._backend.initialize()

        # Verify backend is healthy after initialization
        if not await self._backend.health_check():
            raise ConfigurationError(
                "Backend health check failed after initialization. "
                "Verify your database connection and credentials."
            )

        # Register all pending workflows
        for proxy in self._pending_workflows:
            self._registry.register(proxy.definition)
            # Persist schedule if the proxy has one attached
            sched = getattr(proxy, "_schedule", None)
            if sched is not None:
                await self._backend.save_schedule(sched)

        # Recover incomplete runs
        recovered = await self._engine.recover_incomplete()
        if recovered:
            logger.info("Recovered %d incomplete workflow runs", len(recovered))

        # Start workers if configured
        if self._workers_count > 0:
            from gravtory.workers.local import LocalWorker

            self._worker_pool = LocalWorker(
                worker_id=self._node_id,
                backend=self._backend,
                registry=self._registry,
                execution_engine=self._engine,
                max_concurrent=self._workers_count,
            )
            await self._worker_pool.start()
            logger.info("Started %d worker(s)", self._workers_count)

        # Start scheduler if configured
        if self._scheduler_enabled:
            try:
                from gravtory.scheduling.engine import (  # type: ignore[attr-defined]
                    SchedulingEngine,
                )

                self._scheduler_engine = SchedulingEngine(
                    backend=self._backend,
                    registry=self._registry,
                    execution_engine=self._engine,
                )
                await self._scheduler_engine.start()
                logger.info("Scheduler started")
            except Exception:
                logger.exception("Failed to start scheduler")

        # Start dashboard if configured
        if self._dashboard_enabled:
            try:
                from gravtory.dashboard.server import Dashboard

                self._dashboard = Dashboard(
                    self._backend,
                    self._registry,
                    port=self._dashboard_port,
                    auth_token=self._dashboard_token,
                )
                await self._dashboard.start()
                logger.info("Dashboard started on port %d", self._dashboard_port)
            except Exception:
                logger.exception("Failed to start dashboard")

        # Initialize observability if configured
        if self._otel_endpoint:
            try:
                from gravtory.observability.tracing import TracingProvider

                TracingProvider.configure(endpoint=self._otel_endpoint)  # type: ignore[attr-defined]
                logger.info("OpenTelemetry tracing configured: %s", self._otel_endpoint)
            except Exception:
                logger.exception("Failed to configure OpenTelemetry")

        if self._metrics_port:
            try:
                from gravtory.observability.metrics import MetricsCollector

                MetricsCollector.start_server(port=self._metrics_port)  # type: ignore[attr-defined]
                logger.info("Prometheus metrics server started on port %d", self._metrics_port)
            except Exception:
                logger.exception("Failed to start metrics server")

        # Register OS signal handlers for graceful shutdown
        import signal

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._signal_shutdown(s)),  # type: ignore[misc]
                )

        self._started = True
        logger.info(
            "Gravtory started (node=%s, namespace=%s)", self._node_id, self._active_namespace
        )

    async def shutdown(self) -> None:
        """Graceful shutdown of all subsystems.

        Waits up to ``_drain_timeout`` seconds for in-flight tasks to
        complete before forcing cancellation.
        """
        # Stop accepting new work first
        self._started = False

        # Drain in-flight tasks with timeout
        if self._active_tasks:
            logger.info(
                "Draining %d in-flight task(s), timeout=%.0fs",
                len(self._active_tasks),
                self._drain_timeout,
            )
            _done, pending = await asyncio.wait(
                self._active_tasks,
                timeout=self._drain_timeout,
            )
            if pending:
                logger.warning(
                    "%d task(s) did not complete within drain timeout — cancelling",
                    len(pending),
                )
                for task in pending:
                    task.cancel()
                # Give cancelled tasks a moment to handle CancelledError
                await asyncio.wait(pending, timeout=5.0)
            self._active_tasks.clear()

        # Stop dashboard
        if self._dashboard is not None:
            try:
                await self._dashboard.stop()
            except Exception:
                logger.exception("Error stopping dashboard")
            self._dashboard = None

        # Stop scheduler
        if self._scheduler_engine is not None:
            try:
                await self._scheduler_engine.stop()
            except Exception:
                logger.exception("Error stopping scheduler")
            self._scheduler_engine = None

        # Stop workers
        if self._worker_pool is not None:
            try:
                await self._worker_pool.stop(drain=True)
            except Exception:
                logger.exception("Error stopping workers")
            self._worker_pool = None

        await self._backend.close()
        logger.info("Gravtory shutdown complete")

    async def _signal_shutdown(self, sig: Any) -> None:
        """Handle OS signal by initiating graceful shutdown."""
        logger.info("Received signal %s — initiating graceful shutdown", sig)
        await self.shutdown()

    # ── Run workflows ────────────────────────────────────────────

    async def run(
        self,
        workflow: WorkflowProxy | type,
        *,
        background: bool = False,
        **kwargs: Any,
    ) -> WorkflowRun | str:
        """Run a workflow.

        Args:
            workflow: WorkflowProxy (from @grav.workflow) or class type.
            background: If True, enqueue and return run_id immediately.
            **kwargs: Workflow input parameters.

        Returns:
            WorkflowRun if foreground, run_id string if background.
        """
        proxy = self._resolve_proxy(workflow)

        # Ensure workflow is registered
        from gravtory.core.errors import WorkflowNotFoundError

        try:
            self._registry.get(proxy.definition.name)
        except WorkflowNotFoundError:
            self._registry.register(proxy.definition)

        # Validate kwargs against input_schema if defined
        if proxy.definition.input_schema is not None:
            schema = proxy.definition.input_schema
            if hasattr(schema, "model_validate"):
                # Pydantic v2 model
                schema.model_validate(kwargs)
            elif hasattr(schema, "parse_obj"):
                # Pydantic v1 model
                schema.parse_obj(kwargs)

        # Generate run ID
        run_id = proxy.generate_id(**kwargs)

        # Check concurrency limit if configured
        max_conc = proxy.definition.config.max_concurrent
        if max_conc > 0:
            within_limit = await self._backend.check_concurrency_limit(
                proxy.definition.name, self._active_namespace, max_conc
            )
            if not within_limit:
                raise GravtoryError(
                    f"Concurrency limit reached for workflow "
                    f"'{proxy.definition.name}': max {max_conc} concurrent runs allowed."
                )

        # Check if already exists
        existing = await self._backend.get_workflow_run(run_id)
        if existing is not None:
            if existing.status == WorkflowStatus.COMPLETED:
                return existing
            if existing.status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING):
                # Resume
                result = await self._engine.execute_workflow(
                    definition=proxy.definition,
                    run_id=run_id,
                    input_data=kwargs,
                    resume=True,
                )
                return result

        if background:
            # Create run and return ID
            run = WorkflowRun(
                id=run_id,
                workflow_name=proxy.definition.name,
                namespace=self._active_namespace,
            )
            await self._backend.create_workflow_run(run)
            return run_id

        # Foreground execution
        result = await self._engine.execute_workflow(
            definition=proxy.definition,
            run_id=run_id,
            input_data=kwargs,
        )
        return result

    def run_sync(
        self,
        workflow: WorkflowProxy | type,
        **kwargs: Any,
    ) -> WorkflowRun:
        """Synchronous wrapper for run().

        Automatically calls :meth:`start` and :meth:`shutdown` so the
        caller does not need to manage the async lifecycle manually.

        Works in environments with an already-running event loop
        (Jupyter, Django, etc.) by running in a background thread.
        """

        async def _run() -> WorkflowRun | str:
            if not self._started:
                await self.start()
            return await self.run(workflow, **kwargs)

        try:
            asyncio.get_running_loop()
            # Already inside an event loop — run in a separate thread
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _run())
                result = future.result()
        except RuntimeError:
            # No running loop — safe to use asyncio.run directly
            result = asyncio.run(_run())

        if isinstance(result, str):
            raise GravtoryError("run_sync does not support background=True")
        return result

    # ── Inspection ───────────────────────────────────────────────

    async def inspect(self, run_id: str) -> WorkflowRun:
        """Get full workflow state including all step outputs.

        Args:
            run_id: The workflow run ID to inspect.

        Returns:
            The WorkflowRun with current status and metadata.

        Raises:
            WorkflowRunNotFoundError: If no run exists with the given ID.
        """
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            from gravtory.core.errors import WorkflowRunNotFoundError

            raise WorkflowRunNotFoundError(run_id)
        return run

    async def list_runs(
        self,
        status: str | None = None,
        workflow: str | None = None,
        namespace: str | None = None,
        limit: int = 50,
    ) -> list[WorkflowRun]:
        """List workflow runs with filters."""
        ws = WorkflowStatus(status) if status else None
        ns = namespace or self._active_namespace
        runs = await self._backend.list_workflow_runs(
            namespace=ns,
            status=ws,
            workflow_name=workflow,
            limit=limit,
        )
        return builtins_list(runs)

    async def list(
        self,
        status: str | None = None,
        workflow: str | None = None,
        namespace: str | None = None,
        limit: int = 50,
    ) -> list[WorkflowRun]:
        """List workflow runs. Prefer :meth:`list_runs` to avoid shadowing the builtin."""
        return await self.list_runs(
            status=status, workflow=workflow, namespace=namespace, limit=limit
        )

    async def count(
        self,
        status: str | None = None,
        workflow: str | None = None,
    ) -> int:
        """Count workflow runs."""
        ws = WorkflowStatus(status) if status else None
        return await self._backend.count_workflow_runs(
            namespace=self._active_namespace,
            status=ws,
            workflow_name=workflow,
        )

    # ── Decorators ───────────────────────────────────────────────

    def workflow(
        self,
        id: str,
        *,
        version: int = 1,
        deadline: timedelta | None = None,
        priority: int = 0,
        namespace: str = "default",
        saga: bool = False,
    ) -> Callable[..., Any]:
        """Decorator factory for @grav.workflow(id=...)."""

        def decorator(cls_or_func: type | Callable[..., Any]) -> WorkflowProxy:
            proxy = workflow_decorator(
                id=id,
                version=version,
                deadline=deadline,
                priority=priority,
                namespace=namespace,
                saga=saga,
            )(cls_or_func)
            self._pending_workflows.append(proxy)
            return proxy  # type: ignore[no-any-return]

        return decorator

    def step(self, *args: Any, **kwargs: Any) -> Callable[..., Any]:
        """Convenience: @grav.step() is equivalent to @step() from decorators."""
        return step_decorator(*args, **kwargs)

    # ── Signals ──────────────────────────────────────────────────

    async def signal(self, run_id: str, signal_name: str, data: Any = None) -> None:
        """Send a signal to a running workflow.

        Args:
            run_id: The workflow run ID to signal.
            signal_name: Name of the signal (must match @wait_for_signal).
            data: Signal payload (dict, bytes, or None). Serialized to JSON bytes.
        """
        import json

        from gravtory.core.types import Signal

        if isinstance(data, bytes):
            signal_data = data
        elif data is not None:
            signal_data = json.dumps(data).encode("utf-8")
        else:
            signal_data = None

        sig = Signal(
            workflow_run_id=run_id,
            signal_name=signal_name,
            signal_data=signal_data,
        )
        await self._backend.send_signal(sig)

    # ── Namespace context manager ────────────────────────────────

    @property
    def _active_namespace(self) -> str:
        """Return the namespace for the current async task.

        Checks the task-local ContextVar first (set by ``namespace()``
        context manager), falling back to the instance default.
        """
        ns = _namespace_var.get()
        return ns if ns is not None else self._namespace

    def namespace(self, ns: str) -> _NamespaceContext:
        """Temporarily scope all operations to a namespace.

        Usage::

            async with grav.namespace("tenant_acme"):
                await grav.run(MyWorkflow, order_id="123")
        """
        return _NamespaceContext(self, ns)

    # ── Helpers ──────────────────────────────────────────────────

    def _resolve_proxy(self, workflow: WorkflowProxy | type) -> WorkflowProxy:
        """Resolve a workflow argument to a WorkflowProxy."""
        if isinstance(workflow, WorkflowProxy):
            return workflow
        # Check if it was decorated
        if hasattr(workflow, "definition"):
            return workflow  # type: ignore[return-value]
        raise ConfigurationError(
            f"Expected a WorkflowProxy (from @grav.workflow), got {type(workflow).__name__}. "
            f"Did you forget to decorate your workflow class?"
        )

    @property
    def registry(self) -> WorkflowRegistry:
        """Access the workflow registry."""
        return self._registry

    @property
    def backend(self) -> Backend:
        """Access the backend."""
        return self._backend

    @property
    def engine(self) -> ExecutionEngine:
        """Access the execution engine."""
        return self._engine


_namespace_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "gravtory_namespace",
    default=None,
)


class _NamespaceContext:
    """Async context manager that temporarily overrides the Gravtory namespace.

    Uses ``contextvars.ContextVar`` so that concurrent async tasks
    (e.g. via ``asyncio.gather``) each see their own namespace without
    cross-contamination.
    """

    def __init__(self, grav: Gravtory, ns: str) -> None:
        self._grav = grav
        self._ns = ns
        self._token: contextvars.Token[str | None] | None = None

    async def __aenter__(self) -> Gravtory:
        self._token = _namespace_var.set(self._ns)
        return self._grav

    async def __aexit__(self, exc_type: type | None, exc: BaseException | None, tb: Any) -> None:
        if self._token is not None:
            _namespace_var.reset(self._token)
