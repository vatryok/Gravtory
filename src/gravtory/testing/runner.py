# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""In-memory workflow test runner — no database required.

Provides :class:`WorkflowTestRunner` for running workflows in isolation
using :class:`InMemoryBackend`.  Supports step mocking, crash simulation,
and assertion helpers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepStatus,
    WorkflowStatus,
)
from gravtory.decorators.workflow import WorkflowProxy

if TYPE_CHECKING:
    from collections.abc import Callable

    from gravtory.backends.base import Backend


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class StepTestResult:
    """Result of a single step in a test run."""

    order: int
    name: str
    status: StepStatus
    output: Any = None
    duration_ms: int = 0
    retry_count: int = 0
    was_replayed: bool = False
    was_mocked: bool = False


@dataclass
class CompensationTestResult:
    """Result of a compensation handler invocation."""

    step_order: int
    handler_name: str
    success: bool = True
    error: str | None = None


@dataclass
class TestResult:
    """Full result of a test workflow execution."""

    status: WorkflowStatus
    steps: dict[int, StepTestResult]
    run_id: str
    execution_order: list[int]
    total_duration_ms: int
    error: str | None = None
    compensations: list[CompensationTestResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# CrashSimulationError
# ---------------------------------------------------------------------------


class CrashSimulationError(RuntimeError):
    """Raised to simulate a crash after a specific step."""


# ---------------------------------------------------------------------------
# WorkflowTestRunner
# ---------------------------------------------------------------------------


class WorkflowTestRunner:
    """Run workflows in isolation with mocking and crash simulation.

    Usage::

        runner = WorkflowTestRunner()
        runner.register(MyWorkflow)
        result = await runner.run(MyWorkflow, order_id="test-1")
        assert result.status == WorkflowStatus.COMPLETED
    """

    def __init__(
        self,
        *,
        backend: Backend | None = None,
    ) -> None:
        self._backend: Backend = backend or InMemoryBackend()
        self._registry = WorkflowRegistry()
        self._engine = ExecutionEngine(self._registry, self._backend)
        self._mock_steps: dict[str, _MockEntry] = {}
        self._crash_points: set[tuple[str, int]] = set()
        self._call_log: list[_StepCall] = []
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        if not self._initialized:
            await self._backend.initialize()
            self._initialized = True

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, workflow_proxy: WorkflowProxy | type) -> None:
        """Register a workflow for testing."""
        proxy = self._resolve_proxy(workflow_proxy)
        try:
            self._registry.get(proxy.definition.name)
        except Exception:
            self._registry.register(proxy.definition)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    async def run(
        self,
        workflow: WorkflowProxy | type,
        **kwargs: Any,
    ) -> TestResult:
        """Run a workflow and return a detailed :class:`TestResult`."""
        await self._ensure_initialized()
        proxy = self._resolve_proxy(workflow)
        self.register(proxy)

        definition = proxy.definition
        run_id = proxy.generate_id(**kwargs)

        # Install mocks — swap step functions
        originals: dict[int, Any] = {}
        mocked_orders: set[int] = set()
        for order, step_def in definition.steps.items():
            if step_def.name in self._mock_steps:
                originals[order] = step_def.function
                entry = self._mock_steps[step_def.name]
                step_def.function = self._wrap_mock(step_def.name, entry)
                mocked_orders.add(order)

        # Install crash hooks
        if self._crash_points:
            for order, step_def in definition.steps.items():
                wf_name = definition.name
                if (wf_name, order) in self._crash_points:
                    orig_fn = step_def.function
                    if order not in originals:
                        originals[order] = orig_fn
                    step_def.function = self._wrap_crash(
                        step_def.name,
                        orig_fn,
                        wf_name,
                        order,
                    )

        # Detect if this is a resume of a previously failed/crashed run
        existing_run = await self._backend.get_workflow_run(run_id)
        is_resume = existing_run is not None and existing_run.status in (
            WorkflowStatus.FAILED,
            WorkflowStatus.RUNNING,
        )
        if is_resume:
            # Reset status so claim_workflow_run can transition it
            await self._backend.update_workflow_status(
                run_id,
                WorkflowStatus.PENDING,
            )

        start = time.monotonic()
        error_msg: str | None = None
        try:
            await self._engine.execute_workflow(
                definition=definition,
                run_id=run_id,
                input_data=kwargs,
                resume=is_resume,
            )
            final_status = WorkflowStatus.COMPLETED
        except CrashSimulationError as exc:
            final_status = WorkflowStatus.FAILED
            error_msg = str(exc)
            # Mark run as failed in backend so resume works later
            await self._backend.update_workflow_status(
                run_id,
                WorkflowStatus.FAILED,
                error_message=error_msg,
            )
        except Exception as exc:
            final_status = WorkflowStatus.FAILED
            error_msg = str(exc)

        elapsed_ms = int((time.monotonic() - start) * 1000)

        # Restore originals
        for order, orig_fn in originals.items():
            definition.steps[order].function = orig_fn

        # Build result
        step_outputs = await self._backend.get_step_outputs(run_id)
        steps: dict[int, StepTestResult] = {}
        execution_order: list[int] = []
        for so in step_outputs:
            steps[so.step_order] = StepTestResult(
                order=so.step_order,
                name=so.step_name,
                status=so.status,
                output=so.output_data,
                duration_ms=so.duration_ms or 0,
                retry_count=so.retry_count,
                was_replayed=False,
                was_mocked=so.step_order in mocked_orders,
            )
            execution_order.append(so.step_order)

        # Check backend for final status (may have been set by engine)
        run_obj = await self._backend.get_workflow_run(run_id)
        if run_obj is not None:
            final_status = run_obj.status

        return TestResult(
            status=final_status,
            steps=steps,
            run_id=run_id,
            execution_order=sorted(execution_order),
            total_duration_ms=elapsed_ms,
            error=error_msg,
        )

    # ------------------------------------------------------------------
    # Mocking
    # ------------------------------------------------------------------

    def mock_step(
        self,
        step_name: str,
        return_value: Any = None,
        side_effect: Callable[..., Any] | None = None,
        raises: type[Exception] | None = None,
    ) -> None:
        """Replace a step's function with a mock."""
        self._mock_steps[step_name] = _MockEntry(
            return_value=return_value,
            side_effect=side_effect,
            raises=raises,
        )

    # ------------------------------------------------------------------
    # Crash simulation
    # ------------------------------------------------------------------

    def simulate_crash(self, workflow_name: str, after_step: int) -> None:
        """Simulate a crash after *after_step* completes."""
        self._crash_points.add((workflow_name, after_step))

    # ------------------------------------------------------------------
    # Assertions
    # ------------------------------------------------------------------

    def assert_step_called(
        self,
        step_name: str,
        *,
        times: int | None = None,
        with_input: dict[str, Any] | None = None,
    ) -> None:
        """Assert that *step_name* was called."""
        calls = [c for c in self._call_log if c.step_name == step_name]
        if not calls:
            raise AssertionError(f"Step '{step_name}' was never called")
        if times is not None and len(calls) != times:
            raise AssertionError(f"Step '{step_name}' called {len(calls)} times, expected {times}")
        if with_input is not None:
            for key, val in with_input.items():
                found = any(c.inputs.get(key) == val for c in calls)
                if not found:
                    raise AssertionError(f"Step '{step_name}' was never called with {key}={val!r}")

    def assert_step_not_called(self, step_name: str) -> None:
        """Assert that *step_name* was NOT called."""
        calls = [c for c in self._call_log if c.step_name == step_name]
        if calls:
            raise AssertionError(f"Step '{step_name}' was called {len(calls)} time(s), expected 0")

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset all mocks, crash points, call log, and backend state."""
        self._mock_steps.clear()
        self._crash_points.clear()
        self._call_log.clear()
        self._initialized = False
        self._backend = InMemoryBackend()
        self._engine = ExecutionEngine(self._registry, self._backend)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wrap_mock(self, step_name: str, entry: _MockEntry) -> Any:
        """Create an async callable that replaces the real step."""
        runner = self

        async def _mocked(**kwargs: Any) -> Any:
            runner._call_log.append(_StepCall(step_name=step_name, inputs=kwargs))
            if entry.raises is not None:
                raise entry.raises()
            if entry.side_effect is not None:
                import asyncio

                if asyncio.iscoroutinefunction(entry.side_effect):
                    return await entry.side_effect(**kwargs)
                return entry.side_effect(**kwargs)
            return entry.return_value

        return _mocked

    def _wrap_crash(
        self,
        step_name: str,
        original_fn: Any,
        workflow_name: str,
        after_step: int,
    ) -> Any:
        """Wrap a step so it executes normally then raises CrashSimulationError."""
        runner = self

        async def _crash_after(**kwargs: Any) -> Any:
            import asyncio

            runner._call_log.append(_StepCall(step_name=step_name, inputs=kwargs))
            # Execute the original
            if original_fn is not None:
                if asyncio.iscoroutinefunction(original_fn):
                    await original_fn(**kwargs)
                else:
                    original_fn(**kwargs)
            # Now "crash"
            raise CrashSimulationError(
                f"Simulated crash after step {after_step} of '{workflow_name}'"
            )

        return _crash_after

    @staticmethod
    def _resolve_proxy(workflow: WorkflowProxy | type) -> WorkflowProxy:
        """Resolve a workflow argument to a WorkflowProxy."""
        if isinstance(workflow, WorkflowProxy):
            return workflow
        if hasattr(workflow, "definition"):
            return workflow  # type: ignore[return-value]
        msg = f"Expected WorkflowProxy, got {type(workflow).__name__}"
        raise TypeError(msg)


# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------


@dataclass
class _MockEntry:
    return_value: Any = None
    side_effect: Any = None
    raises: type[Exception] | None = None


@dataclass
class _StepCall:
    step_name: str = ""
    inputs: dict[str, Any] = field(default_factory=dict)
