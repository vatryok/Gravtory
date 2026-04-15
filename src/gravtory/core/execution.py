# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Workflow execution engine — orchestrates step execution, checkpointing, and resumption."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import secrets
import time
import traceback
from datetime import datetime, timezone
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from gravtory.core.context import StepContext
from gravtory.core.errors import (
    GravtoryError,
    StepAbortError,
    StepRetryExhaustedError,
    StepTimeoutError,
    WorkflowDeadlineExceededError,
)
from gravtory.core.parallel import ParallelExecutor
from gravtory.core.types import (
    DLQEntry,
    StepOutput,
    StepResult,
    StepStatus,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.signals.handler import SignalHandler
from gravtory.workers.rate_limit import RateLimiter

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.checkpoint import CheckpointEngine
    from gravtory.core.dag import DAG
    from gravtory.core.registry import WorkflowRegistry
    from gravtory.core.types import StepDefinition, WorkflowDefinition
    from gravtory.decorators.middleware import MiddlewareRegistry

logger = logging.getLogger("gravtory.execution")


class ExecutionEngine:
    """The heart of Gravtory — drives workflows to completion."""

    def __init__(
        self,
        registry: WorkflowRegistry,
        backend: Backend,
        checkpoint_engine: CheckpointEngine | None = None,
        middleware: MiddlewareRegistry | None = None,
        metrics: Any | None = None,
        use_checkpoint_aad: bool = False,
    ) -> None:
        self._registry = registry
        self._backend = backend
        self._checkpoint = checkpoint_engine
        self._middleware = middleware
        self._metrics = metrics  # Optional MetricsCollector
        self._rate_limiters: dict[str, Any] = {}  # cache by step name
        self._use_checkpoint_aad = use_checkpoint_aad

    def _make_aad(self, run_id: str, step_order: int) -> bytes | None:
        """Build Additional Authenticated Data for checkpoint encryption.

        When ``use_checkpoint_aad`` is True, returns ``b"run_id:step_order"``
        which cryptographically binds encrypted outputs to their context.
        Swapping ciphertext between runs/steps will cause decryption to fail.
        Returns None when AAD is disabled (backward-compatible default).
        """
        if not self._use_checkpoint_aad:
            return None
        return f"{run_id}:{step_order}".encode()

    async def execute_workflow(
        self,
        definition: WorkflowDefinition,
        run_id: str,
        input_data: dict[str, Any],
        resume: bool = False,
    ) -> WorkflowRun:
        """Execute a workflow from start or resume from checkpoint.

        Phase 1 — Initialize or Load
        Phase 2 — Build DAG
        Phase 3 — Execute loop
        """
        # Phase 1 — Initialize or Load
        completed_steps: dict[int, StepResult] = {}

        if resume:
            run = await self._backend.get_workflow_run(run_id)
            if run is None:
                raise GravtoryError(f"Cannot resume: workflow run '{run_id}' not found")
            # Load completed steps from backend
            step_outputs = await self._backend.get_step_outputs(run_id)
            for so in step_outputs:
                if so.status in (StepStatus.COMPLETED, StepStatus.SKIPPED):
                    # Deserialize checkpoint data back to Python objects
                    output = so.output_data
                    if (
                        output is not None
                        and isinstance(output, (bytes, memoryview))
                        and self._checkpoint is not None
                    ):
                        aad = self._make_aad(run_id, so.step_order)
                        output = self._checkpoint.restore(bytes(output), aad=aad)
                    completed_steps[so.step_order] = StepResult(
                        output=output,
                        status=so.status,
                        was_replayed=True,
                        duration_ms=so.duration_ms or 0,
                        retry_count=so.retry_count,
                    )
        else:
            # Persist input_data as JSON bytes so recovery can restore kwargs
            serialized_input = json.dumps(input_data).encode("utf-8") if input_data else None
            deadline_at = None
            if definition.config.deadline is not None:
                deadline_at = datetime.now(tz=timezone.utc) + definition.config.deadline
            run = WorkflowRun(
                id=run_id,
                workflow_name=definition.name,
                workflow_version=definition.version,
                namespace=definition.config.namespace,
                status=WorkflowStatus.PENDING,
                input_data=serialized_input,
                deadline_at=deadline_at,
            )
            await self._backend.create_workflow_run(run)

        # Atomically claim the workflow run: PENDING → RUNNING
        # If another worker already claimed it, this returns False.
        expected = run.status if resume else WorkflowStatus.PENDING
        claimed = await self._backend.claim_workflow_run(
            run_id,
            expected_status=expected,
            new_status=WorkflowStatus.RUNNING,
        )
        if not claimed:
            raise GravtoryError(
                f"Cannot claim workflow run '{run_id}': already claimed by another worker "
                f"(expected status={expected.value})"
            )
        if self._metrics is not None:
            self._metrics.record_workflow_started(
                definition.name,
                definition.config.namespace,
            )

        # Phase 2 — Build DAG
        dag = self._registry.get_dag(definition.name, definition.version)

        # Phase 3 — Execute loop
        try:
            await self._execute_loop(
                run_id=run_id,
                definition=definition,
                dag=dag,
                completed_steps=completed_steps,
                input_data=input_data,
            )
            # Determine final output from the last completed step (leaf nodes)
            final_output: bytes | None = None
            if completed_steps:
                last_order = max(completed_steps.keys())
                last_result = completed_steps[last_order]
                if last_result.output is not None and self._checkpoint is not None:
                    final_output = self._checkpoint.process(
                        last_result.output,
                        aad=self._make_aad(run_id, last_order),
                    )
                elif last_result.output is not None:
                    final_output = (
                        json.dumps(last_result.output).encode("utf-8")
                        if not isinstance(last_result.output, bytes)
                        else last_result.output
                    )

            # Mark completed with final output
            await self._backend.validated_update_workflow_status(
                run_id, WorkflowStatus.COMPLETED, output_data=final_output
            )
            if self._metrics is not None:
                self._metrics.record_workflow_completed(
                    definition.name, definition.config.namespace
                )
        except Exception as exc:
            await self._handle_step_failure(
                run_id=run_id,
                step_def=None,
                error=exc,
                definition=definition,
                completed_steps=completed_steps,
            )
            raise

        # Return final state
        final_run = await self._backend.get_workflow_run(run_id)
        return final_run if final_run is not None else run

    async def _execute_loop(
        self,
        run_id: str,
        definition: WorkflowDefinition,
        dag: DAG,
        completed_steps: dict[int, StepResult],
        input_data: dict[str, Any],
    ) -> None:
        """Main execution loop — process steps until all done."""
        # Instantiate workflow class once per run (for class-based workflows)
        workflow_instance: Any = None
        if definition.workflow_class is not None:
            try:
                workflow_instance = definition.workflow_class()
            except TypeError:
                # Class may not be instantiable (e.g. synthetic class for function-based)
                workflow_instance = None

        # Cache deadline_at from initial run load to avoid extra DB query per iteration
        cached_deadline_at = None
        if definition.config.deadline is not None:
            initial_run = await self._backend.get_workflow_run(run_id)
            if initial_run is not None:
                cached_deadline_at = initial_run.deadline_at

        iteration_safety_multiplier = 3
        max_iterations = len(definition.steps) * iteration_safety_multiplier
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Check workflow deadline (using cached value — deadline doesn't change)
            if cached_deadline_at is not None:
                now = datetime.now(tz=timezone.utc)
                if now >= cached_deadline_at:
                    raise WorkflowDeadlineExceededError(run_id)

            ready_steps = dag.get_ready_steps(completed_steps)

            if not ready_steps:
                if dag.all_steps_done(completed_steps):
                    return
                # Deadlock — no steps ready but not all done
                raise GravtoryError(
                    f"Workflow '{run_id}' is deadlocked: no steps ready "
                    f"but {len(completed_steps)}/{len(definition.steps)} completed"
                )

            # Evaluate conditions — skip steps whose condition is False
            executable: list[StepDefinition] = []
            ctx = StepContext(MappingProxyType(completed_steps), input_data, run_id)

            for step_def in ready_steps:
                if step_def.condition is not None:
                    try:
                        should_run = step_def.condition(ctx)
                    except Exception:
                        should_run = False

                    if not should_run:
                        # Record as skipped
                        result = StepResult(
                            output=None,
                            status=StepStatus.SKIPPED,
                            was_replayed=False,
                        )
                        completed_steps[step_def.order] = result
                        await self._backend.save_step_output(
                            StepOutput(
                                workflow_run_id=run_id,
                                step_order=step_def.order,
                                step_name=step_def.name,
                                status=StepStatus.SKIPPED,
                            )
                        )
                        continue
                executable.append(step_def)

            if not executable:
                # All ready steps were skipped, loop again
                continue

            # Execute steps
            if len(executable) == 1:
                result = await self._execute_single_step(
                    run_id=run_id,
                    step_def=executable[0],
                    completed_steps=completed_steps,
                    input_data=input_data,
                    workflow_instance=workflow_instance,
                    definition=definition,
                )
                completed_steps[executable[0].order] = result
            else:
                # Parallel execution — each task gets an immutable snapshot
                # of completed_steps to prevent concurrent mutation issues.
                snapshot = dict(completed_steps)
                tasks = [
                    self._execute_single_step(
                        run_id=run_id,
                        step_def=sd,
                        completed_steps=snapshot,
                        input_data=input_data,
                        workflow_instance=workflow_instance,
                        definition=definition,
                    )
                    for sd in executable
                ]
                results: list[StepResult | BaseException] = await asyncio.gather(
                    *tasks, return_exceptions=True
                )
                # Checkpoint successful results first, then raise on failures
                errors: list[Exception] = []
                for sd, gather_result in zip(executable, results, strict=False):
                    if isinstance(gather_result, BaseException):
                        errors.append(
                            gather_result
                            if isinstance(gather_result, Exception)
                            else Exception(str(gather_result))
                        )
                    else:
                        completed_steps[sd.order] = gather_result
                if errors:
                    raise errors[0]

            # Memory optimization: evict outputs for steps whose dependents
            # have all completed. Data is already checkpointed to DB.
            self._evict_consumed_outputs(dag, completed_steps)

        raise GravtoryError(f"Workflow '{run_id}' exceeded maximum iterations ({max_iterations})")

    @staticmethod
    def _evict_consumed_outputs(
        dag: DAG,
        completed_steps: dict[int, StepResult],
    ) -> None:
        """Release step outputs from memory once all their dependents have completed.

        This prevents large intermediate outputs from accumulating for the
        full workflow lifetime. The data is already persisted in the backend.
        """
        for order, result in completed_steps.items():
            if result.output is None:
                continue
            # Check if every dependent of this step is already completed
            dependents = dag._adjacency.get(order, [])
            if dependents and all(d in completed_steps for d in dependents):
                result.output = None

    async def execute_single_step(
        self,
        run_id: str,
        step_def: StepDefinition,
        completed_steps: dict[int, StepResult],
        input_data: dict[str, Any],
        workflow_instance: Any = None,
        definition: WorkflowDefinition | None = None,
    ) -> StepResult:
        """Execute a single step with retry logic and checkpointing.

        This is the public API for workers to execute individual steps.
        """
        return await self._execute_single_step(
            run_id,
            step_def,
            completed_steps,
            input_data,
            workflow_instance,
            definition=definition,
        )

    async def _execute_single_step(
        self,
        run_id: str,
        step_def: StepDefinition,
        completed_steps: dict[int, StepResult],
        input_data: dict[str, Any],
        workflow_instance: Any = None,
        definition: WorkflowDefinition | None = None,
    ) -> StepResult:
        """Execute a single step with retry logic and checkpointing."""
        # 1. Check idempotency — if step already has output, replay it
        existing = await self._backend.get_step_output(run_id, step_def.order)
        if existing is not None and existing.status == StepStatus.COMPLETED:
            output = existing.output_data
            if (
                output is not None
                and isinstance(output, (bytes, memoryview))
                and self._checkpoint is not None
            ):
                aad = self._make_aad(run_id, step_def.order)
                output = self._checkpoint.restore(bytes(output), aad=aad)
            return StepResult(
                output=output,
                status=StepStatus.COMPLETED,
                was_replayed=True,
                duration_ms=existing.duration_ms or 0,
                retry_count=existing.retry_count,
            )

        # 2. Wait for signal if configured
        if step_def.signal_config is not None:
            signal_handler = SignalHandler(self._backend)
            try:
                signal_data = await signal_handler.wait(
                    run_id,
                    step_def.signal_config.name,
                    step_def.signal_config.timeout,
                )
                resolved_inputs = self._resolve_inputs(step_def, completed_steps, input_data)
                resolved_inputs["signal"] = signal_data
                resolved_inputs["signal_data"] = signal_data
            finally:
                await signal_handler.close()
        else:
            # 2b. Resolve inputs (no signal)
            resolved_inputs = self._resolve_inputs(step_def, completed_steps, input_data)

        # 3. Handle parallel execution if configured
        if step_def.parallel_config is not None:
            return await self._execute_parallel_step(
                run_id=run_id,
                step_def=step_def,
                completed_steps=completed_steps,
                input_data=input_data,
                workflow_instance=workflow_instance,
            )

        # 4. Retry loop
        retry_count = 0
        max_retries = step_def.retries
        last_error: Exception | None = None

        while True:
            start_time = time.monotonic()
            try:
                # Apply timeout if configured
                func = step_def.function
                if func is None:
                    raise GravtoryError(f"Step '{step_def.name}' has no function attached")

                # Apply rate limiting if configured
                if step_def.rate_limit is not None:
                    limiter_key = f"step:{step_def.name}"
                    limiter = self._rate_limiters.get(limiter_key)
                    if limiter is None:
                        limiter = RateLimiter(
                            limiter_key,
                            max_tokens=float(step_def.rate_limit.split("/")[0])
                            if "/" in step_def.rate_limit
                            else 10.0,
                            refill_rate=1.0,
                            backend=self._backend,
                        )
                        self._rate_limiters[limiter_key] = limiter
                    wait_time = await limiter.acquire()
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                # Middleware: before_step hook
                if self._middleware is not None:
                    await self._middleware.run_before(
                        workflow_name=definition.name if definition else step_def.name,
                        step_name=step_def.name,
                        run_id=run_id,
                        inputs=resolved_inputs,
                    )

                coro = self._call_step_function(func, step_def, resolved_inputs, workflow_instance)

                if step_def.timeout is not None:
                    try:
                        result_value = await asyncio.wait_for(
                            coro, timeout=step_def.timeout.total_seconds()
                        )
                    except asyncio.TimeoutError as timeout_err:
                        raise StepTimeoutError(
                            step_def.name, step_def.timeout.total_seconds()
                        ) from timeout_err
                else:
                    result_value = await coro

                elapsed_ms = int((time.monotonic() - start_time) * 1000)

                # Middleware: after_step hook
                if self._middleware is not None:
                    await self._middleware.run_after(
                        workflow_name=definition.name if definition else step_def.name,
                        step_name=step_def.name,
                        run_id=run_id,
                        output=result_value,
                        duration_ms=elapsed_ms,
                    )

                # Checkpoint — serialize output through checkpoint pipeline
                # (serialize → compress → encrypt) before persisting.
                # Retry up to 3 times on transient failures to avoid
                # re-executing the step when only the checkpoint write fails.
                checkpoint_data: Any = result_value
                _checkpoint_retries = 3
                for _cp_attempt in range(_checkpoint_retries):
                    try:
                        if self._checkpoint is not None and result_value is not None:
                            checkpoint_data = self._checkpoint.process(
                                result_value,
                                aad=self._make_aad(run_id, step_def.order),
                            )

                        await self._backend.save_step_output(
                            StepOutput(
                                workflow_run_id=run_id,
                                step_order=step_def.order,
                                step_name=step_def.name,
                                output_data=checkpoint_data,
                                status=StepStatus.COMPLETED,
                                duration_ms=elapsed_ms,
                                retry_count=retry_count,
                            )
                        )
                        break
                    except Exception:
                        if _cp_attempt >= _checkpoint_retries - 1:
                            raise
                        logger.warning(
                            "Checkpoint save failed for step '%s' (attempt %d/%d), retrying",
                            step_def.name,
                            _cp_attempt + 1,
                            _checkpoint_retries,
                            exc_info=True,
                        )
                        await asyncio.sleep(0.5 * (2**_cp_attempt))

                return StepResult(
                    output=result_value,
                    status=StepStatus.COMPLETED,
                    was_replayed=False,
                    duration_ms=elapsed_ms,
                    retry_count=retry_count,
                )

            except StepTimeoutError:
                raise

            except Exception as exc:
                # Middleware: on_failure hook
                if self._middleware is not None:
                    await self._middleware.run_on_failure(
                        workflow_name=definition.name if definition else step_def.name,
                        step_name=step_def.name,
                        run_id=run_id,
                        error=exc,
                    )

                # Abort immediately if exception matches abort_on types
                if step_def.abort_on and isinstance(exc, tuple(step_def.abort_on)):
                    raise StepAbortError(step_def.name, exc) from exc

                last_error = exc

                # No retries configured — let original exception propagate
                if max_retries <= 0:
                    raise

                retry_count += 1

                # Check if we should retry this exception type
                if step_def.retry_on and not isinstance(exc, tuple(step_def.retry_on)):
                    raise

                if retry_count > max_retries:
                    raise StepRetryExhaustedError(
                        step_def.name, max_retries, last_error=last_error
                    ) from last_error

                # Calculate backoff delay
                delay = self._calculate_backoff(
                    retry_count=retry_count,
                    backoff=step_def.backoff,
                    base=step_def.backoff_base,
                    multiplier=step_def.backoff_multiplier,
                    max_delay=step_def.backoff_max,
                    jitter=step_def.jitter,
                )

                logger.warning(
                    "Step '%s' failed (attempt %d/%d), retrying in %.1fs: %s",
                    step_def.name,
                    retry_count,
                    max_retries,
                    delay,
                    exc,
                )

                await asyncio.sleep(delay)

    async def _execute_parallel_step(
        self,
        run_id: str,
        step_def: StepDefinition,
        completed_steps: dict[int, StepResult],
        input_data: dict[str, Any],
        workflow_instance: Any = None,
    ) -> StepResult:
        """Execute a parallel fan-out step using ParallelExecutor.

        1. Get items list from dependency output.
        2. Load already-completed items from backend (for resume).
        3. Fan-out execution via ParallelExecutor with bounded concurrency.
        4. Checkpoint each item individually.
        5. Return aggregated list of results.
        """
        config = step_def.parallel_config
        if config is None:
            raise GravtoryError(f"Step '{step_def.name}' has no parallel config")

        func = step_def.function
        if func is None:
            raise GravtoryError(f"Step '{step_def.name}' has no function attached")

        # Resolve items from dependency output (should be a list)
        items: list[Any] = []
        if step_def.depends_on:
            dep_order = step_def.depends_on[0]
            dep_result = completed_steps.get(dep_order)
            if dep_result is not None and isinstance(dep_result.output, list):
                items = dep_result.output
            elif dep_result is not None and dep_result.output is None:
                # Output was evicted by _evict_consumed_outputs — reload from backend
                so = await self._backend.get_step_output(run_id, dep_order)
                if so is not None and so.output_data is not None:
                    restored = so.output_data
                    if isinstance(restored, (bytes, memoryview)) and self._checkpoint is not None:
                        restored = self._checkpoint.restore(
                            bytes(restored),
                            aad=self._make_aad(run_id, dep_order),
                        )
                    if isinstance(restored, list):
                        items = restored

        if not items:
            items = input_data.get("_prev_output", [])
            if not isinstance(items, list):
                items = []

        # Load already-completed parallel items (for resume)
        existing_results = await self._backend.get_parallel_results(run_id, step_def.order)
        completed_items: dict[int, Any] = {}
        for idx, raw_data in existing_results.items():
            if self._checkpoint is not None and isinstance(raw_data, (bytes, memoryview)):
                completed_items[idx] = self._checkpoint.restore(
                    bytes(raw_data),
                    aad=self._make_aad(run_id, step_def.order),
                )
            else:
                completed_items[idx] = raw_data

        # Bind function to workflow instance if needed
        if self._function_needs_self(func) and workflow_instance is not None:
            bound_func = func.__get__(workflow_instance, type(workflow_instance))
        else:
            bound_func = func

        start_time = time.monotonic()

        async def _checkpoint_item(index: int, result: Any) -> None:
            data = result
            if self._checkpoint is not None and result is not None:
                data = self._checkpoint.process(
                    result,
                    aad=self._make_aad(run_id, step_def.order),
                )
            await self._backend.checkpoint_parallel_item(
                run_id,
                step_def.order,
                index,
                data if isinstance(data, bytes) else b"",
            )

        executor = ParallelExecutor(max_concurrency=config.max_concurrency)
        results = await executor.execute(
            bound_func,
            items,
            completed=completed_items,
            on_item_complete=_checkpoint_item,
        )

        elapsed_ms = int((time.monotonic() - start_time) * 1000)

        # Checkpoint the aggregated result
        checkpoint_data: Any = results
        if self._checkpoint is not None and results is not None:
            checkpoint_data = self._checkpoint.process(
                results,
                aad=self._make_aad(run_id, step_def.order),
            )

        await self._backend.save_step_output(
            StepOutput(
                workflow_run_id=run_id,
                step_order=step_def.order,
                step_name=step_def.name,
                output_data=checkpoint_data,
                status=StepStatus.COMPLETED,
                duration_ms=elapsed_ms,
            )
        )

        return StepResult(
            output=results,
            status=StepStatus.COMPLETED,
            was_replayed=False,
            duration_ms=elapsed_ms,
        )

    async def _call_step_function(
        self,
        func: Any,
        step_def: StepDefinition,
        resolved_inputs: dict[str, Any],
        workflow_instance: Any = None,
    ) -> Any:
        """Call the step function, handling both sync and async, class and standalone.

        For class-based workflows, the function is an unbound method that needs
        the workflow instance as the first argument (self).
        """
        # Determine if this is an unbound method that needs an instance
        needs_self = self._function_needs_self(func)

        if needs_self and workflow_instance is not None:
            # Bind the method to the instance
            bound = func.__get__(workflow_instance, type(workflow_instance))
            call_args = resolved_inputs
            callable_fn = bound
        else:
            callable_fn = func
            call_args = resolved_inputs

        # Filter call_args to only include parameters the function accepts,
        # unless the function has a **kwargs catch-all.  This prevents
        # confusing TypeErrors when a previous step's dict output is merged.
        call_args = self._filter_args_for_function(callable_fn, call_args)

        if asyncio.iscoroutinefunction(callable_fn):
            return await callable_fn(**call_args)
        elif (
            inspect.isfunction(callable_fn)
            or inspect.ismethod(callable_fn)
            or callable(callable_fn)
        ):
            # Sync function — run in executor
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: callable_fn(**call_args))
        else:
            raise GravtoryError(f"Step '{step_def.name}' function is not callable: {type(func)}")

    @staticmethod
    def _function_needs_self(func: Any) -> bool:
        """Check if a function is an unbound method that expects 'self' as first param."""
        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            return len(params) > 0 and params[0] == "self"
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _filter_args_for_function(func: Any, args: dict[str, Any]) -> dict[str, Any]:
        """Filter *args* to only include keys the function actually accepts.

        If the function has a ``**kwargs`` parameter, return all args unchanged.
        Otherwise, return only the keys that appear in the function signature.
        This prevents ``TypeError: got an unexpected keyword argument`` when
        a previous step's dict output is merged into the resolved inputs.
        """
        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError):
            return args

        has_var_keyword = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        if has_var_keyword:
            return args

        accepted = {
            name
            for name, p in sig.parameters.items()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }
        return {k: v for k, v in args.items() if k in accepted}

    def _resolve_inputs(
        self,
        step_def: StepDefinition,
        completed_steps: dict[int, StepResult],
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve inputs for a step from dependency outputs and workflow kwargs.

        Rules:
          - Original workflow kwargs are always available
          - If step depends on ONE step: that step's output is available
          - If step depends on MULTIPLE steps: outputs keyed by step order
        """
        resolved = dict(input_data)

        if len(step_def.depends_on) == 1:
            dep_order = step_def.depends_on[0]
            dep_result = completed_steps.get(dep_order)
            if dep_result is not None and dep_result.output is not None:
                # If the output is a dict, merge it into resolved
                if isinstance(dep_result.output, dict):
                    resolved.update(dep_result.output)
                else:
                    resolved["_prev_output"] = dep_result.output
        elif len(step_def.depends_on) > 1:
            dep_outputs: dict[int, Any] = {}
            for dep_order in step_def.depends_on:
                dep_result = completed_steps.get(dep_order)
                if dep_result is not None:
                    dep_outputs[dep_order] = dep_result.output
            resolved["_dep_outputs"] = dep_outputs

        return resolved

    async def _handle_step_failure(
        self,
        run_id: str,
        step_def: StepDefinition | None,
        error: Exception,
        definition: WorkflowDefinition,
        completed_steps: dict[int, StepResult],
    ) -> None:
        """Handle a step failure — saga compensation or workflow failure."""
        tb = traceback.format_exc()

        if definition.config.saga_enabled and completed_steps:
            await self._backend.validated_update_workflow_status(
                run_id, WorkflowStatus.COMPENSATING
            )
            try:
                await self._trigger_compensations(run_id, definition, completed_steps)
                await self._backend.validated_update_workflow_status(
                    run_id,
                    WorkflowStatus.COMPENSATED,
                    error_message=str(error),
                    error_traceback=tb,
                )
            except Exception as comp_exc:
                await self._backend.validated_update_workflow_status(
                    run_id,
                    WorkflowStatus.COMPENSATION_FAILED,
                    error_message=f"Original: {error}; Compensation: {comp_exc}",
                    error_traceback=tb,
                )
        else:
            await self._backend.validated_update_workflow_status(
                run_id,
                WorkflowStatus.FAILED,
                error_message=str(error),
                error_traceback=tb,
            )

        if self._metrics is not None:
            self._metrics.record_workflow_failed(definition.name, definition.config.namespace)

        # Add to DLQ
        await self._backend.add_to_dlq(
            DLQEntry(
                workflow_run_id=run_id,
                step_order=step_def.order if step_def else 0,
                error_message=str(error),
                error_traceback=tb,
            )
        )

    async def _trigger_compensations(
        self,
        run_id: str,
        definition: WorkflowDefinition,
        completed_steps: dict[int, StepResult],
    ) -> None:
        """Run compensation handlers in reverse step order (best-effort).

        All compensations are attempted even if some fail. The workflow status
        is set to COMPENSATED if all succeed, COMPENSATION_FAILED if any fail.
        Failed compensations are added to the DLQ for manual retry.
        """
        # Instantiate workflow class so compensation methods get a proper 'self'
        workflow_instance: Any = None
        if definition.workflow_class is not None:
            try:
                workflow_instance = definition.workflow_class()
            except TypeError:
                workflow_instance = None

        any_failed = False

        # Get compensations in reverse order
        for order in sorted(completed_steps.keys(), reverse=True):
            step_def = definition.steps.get(order)
            if step_def is None or step_def.compensate is None:
                continue
            result = completed_steps[order]
            if result.status != StepStatus.COMPLETED:
                continue

            # Reload output if it was evicted by _evict_consumed_outputs
            if result.output is None:
                so = await self._backend.get_step_output(run_id, order)
                if so is not None and so.output_data is not None:
                    output_data = so.output_data
                    if (
                        isinstance(output_data, (bytes, memoryview))
                        and self._checkpoint is not None
                    ):
                        output_data = self._checkpoint.restore(
                            bytes(output_data), aad=self._make_aad(run_id, order)
                        )
                    elif isinstance(output_data, (bytes, memoryview)):
                        with contextlib.suppress(json.JSONDecodeError, UnicodeDecodeError):
                            output_data = json.loads(bytes(output_data))
                    result = StepResult(
                        output=output_data,
                        status=result.status,
                        was_replayed=result.was_replayed,
                        duration_ms=result.duration_ms,
                        retry_count=result.retry_count,
                    )

            try:
                handler = self._registry.get_compensation_handler(
                    definition.name, step_def.compensate
                )
            except Exception:
                logger.error(
                    "Compensation handler '%s' not found for step %d",
                    step_def.compensate,
                    order,
                )
                any_failed = True
                continue

            # Bind the unbound method to the workflow instance
            if workflow_instance is not None:
                bound_handler = handler.__get__(workflow_instance, type(workflow_instance))
            else:
                bound_handler = handler

            try:
                compensation_timeout = 60.0  # seconds
                if asyncio.iscoroutinefunction(bound_handler):
                    await asyncio.wait_for(
                        bound_handler(result.output),
                        timeout=compensation_timeout,
                    )
                else:
                    # Run sync handlers in executor to avoid blocking event loop
                    loop = asyncio.get_running_loop()
                    await asyncio.wait_for(
                        loop.run_in_executor(None, bound_handler, result.output),
                        timeout=compensation_timeout,
                    )
            except Exception as exc:
                logger.error(
                    "Compensation '%s' for step %d failed: %s",
                    step_def.compensate,
                    order,
                    exc,
                )
                any_failed = True
                # Add failed compensation to DLQ for manual retry
                await self._backend.add_to_dlq(
                    DLQEntry(
                        workflow_run_id=run_id,
                        step_order=order,
                        error_message=f"Compensation '{step_def.compensate}' failed: {exc}",
                    )
                )
                # Best-effort: continue with remaining compensations

        if any_failed:
            raise GravtoryError(f"One or more compensations failed for workflow run '{run_id}'")

    async def cancel_workflow(self, run_id: str, *, propagate: bool = True) -> list[str]:
        """Cancel a workflow run, optionally propagating to child sub-workflows.

        Args:
            run_id: The workflow run to cancel.
            propagate: If True (default), recursively cancel any child
                workflow runs whose ``parent_run_id`` matches *run_id*.

        Returns:
            List of all cancelled run IDs (parent + children).
        """
        cancelled: list[str] = []

        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            raise GravtoryError(f"Workflow run '{run_id}' not found")

        if run.status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING):
            await self._backend.validated_update_workflow_status(run_id, WorkflowStatus.CANCELLED)
            cancelled.append(run_id)
            logger.info("Cancelled workflow run '%s'", run_id)

        if propagate:
            children = await self._backend.list_child_runs(run_id)
            for child in children:
                child_cancelled = await self.cancel_workflow(child.id, propagate=True)
                cancelled.extend(child_cancelled)

        return cancelled

    async def recover_incomplete(self, max_concurrent: int = 10, page_size: int = 100) -> list[str]:
        """Find all incomplete workflow runs and resume them concurrently.

        Called at Gravtory.start() time. Uses bounded concurrency to avoid
        blocking startup indefinitely when many workflows need recovery.
        Paginates through runs to avoid loading thousands of objects at once.
        """
        resumed: list[str] = []
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _recover_one(run: Any) -> str | None:
            async with semaphore:
                try:
                    input_data: dict[str, Any] = {}
                    if run.input_data is not None:
                        raw = run.input_data
                        if isinstance(raw, (bytes, memoryview)):
                            input_data = json.loads(bytes(raw).decode("utf-8"))
                        elif isinstance(raw, str):
                            input_data = json.loads(raw)

                    definition = self._registry.get(run.workflow_name, run.workflow_version)
                    await self.execute_workflow(
                        definition=definition,
                        run_id=run.id,
                        input_data=input_data,
                        resume=True,
                    )
                    return str(run.id)
                except Exception:
                    logger.exception("Failed to recover workflow run '%s'", run.id)
                    return None

        total_found = 0
        for status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING):
            offset = 0
            while True:
                batch = await self._backend.list_workflow_runs(
                    status=status,
                    limit=page_size,
                    offset=offset,
                )
                if not batch:
                    break
                total_found += len(batch)
                results = await asyncio.gather(*[_recover_one(run) for run in batch])
                resumed.extend(r for r in results if r is not None)
                if len(batch) < page_size:
                    break
                offset += page_size

        if total_found > 0:
            logger.info(
                "Recovery complete: %d/%d workflow run(s) resumed",
                len(resumed),
                total_found,
            )
        return resumed

    @staticmethod
    def _calculate_backoff(
        retry_count: int,
        backoff: str | None,
        base: float,
        multiplier: float,
        max_delay: float,
        jitter: bool,
    ) -> float:
        """Calculate retry delay based on backoff strategy.

        Supported strategies:
          - ``"exponential"``: base * multiplier^(retry_count-1)
          - ``"linear"``: base * retry_count
          - ``"constant"``: base (fixed delay)
          - ``None``: defaults to exponential backoff

        When *jitter* is True, the delay is randomized between 50-100%
        of the calculated value to prevent thundering herd.
        """

        if backoff == "exponential" or backoff is None:
            # Default strategy when not specified
            delay = base * (multiplier ** (retry_count - 1))
        elif backoff == "linear":
            delay = base * retry_count
        elif backoff == "constant":
            delay = base
        else:
            # Unknown strategy — fall back to exponential
            delay = base * (multiplier ** (retry_count - 1))

        delay = min(delay, max_delay)

        if jitter:
            # Use secrets for thread-safe, cryptographically random jitter
            jitter_factor = 0.5 + (secrets.randbelow(1000) / 1000.0) * 0.5
            delay = delay * jitter_factor

        return delay
