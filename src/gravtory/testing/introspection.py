# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Type-safe introspection API for workflow runs.

Provides :class:`WorkflowInspection`, :class:`StepInspection`, and
:class:`ErrorInfo` — rich, typed views over a workflow execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

from gravtory.core.types import StepStatus, WorkflowStatus


@dataclass
class ErrorInfo:
    """Structured error information."""

    message: str
    traceback: str | None = None
    step_name: str | None = None
    step_order: int | None = None


@dataclass
class StepInspection:
    """Typed view of a single step execution."""

    order: int
    name: str
    status: StepStatus
    output: Any = None
    output_type: str | None = None
    duration_ms: int | None = None
    retry_count: int = 0
    error: str | None = None
    was_replayed: bool = False


@dataclass
class WorkflowInspection:
    """Rich, type-safe inspection result for a workflow run."""

    run_id: str
    workflow_name: str
    workflow_version: int
    status: WorkflowStatus
    namespace: str
    current_step: int | None
    steps: dict[int, StepInspection]
    input_data: Any = None
    output_data: Any = None
    error: ErrorInfo | None = None
    parent_run_id: str | None = None
    child_runs: list[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None

    @property
    def is_done(self) -> bool:
        """Whether the workflow has reached a terminal state."""
        return self.status in (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.COMPENSATION_FAILED,
            WorkflowStatus.CANCELLED,
        )

    @property
    def progress(self) -> float:
        """Fraction of steps completed (0.0-1.0)."""
        total = len(self.steps)
        if total == 0:
            return 0.0
        done = sum(
            1 for s in self.steps.values() if s.status in (StepStatus.COMPLETED, StepStatus.SKIPPED)
        )
        return done / total


async def inspect_workflow(
    backend: Any,
    run_id: str,
) -> WorkflowInspection | None:
    """Build a :class:`WorkflowInspection` from backend data.

    Returns ``None`` if the run does not exist.
    """
    run = await backend.get_workflow_run(run_id)
    if run is None:
        return None

    step_outputs = await backend.get_step_outputs(run_id)
    steps: dict[int, StepInspection] = {}
    for so in step_outputs:
        steps[so.step_order] = StepInspection(
            order=so.step_order,
            name=so.step_name,
            status=so.status,
            output=so.output_data,
            output_type=so.output_type,
            duration_ms=so.duration_ms,
            retry_count=so.retry_count,
            error=so.error_message,
        )

    error_info: ErrorInfo | None = None
    if run.error_message:
        error_info = ErrorInfo(
            message=run.error_message,
            traceback=run.error_traceback,
        )

    duration_ms: int | None = None
    if run.created_at and run.completed_at:
        delta = run.completed_at - run.created_at
        duration_ms = int(delta.total_seconds() * 1000)

    return WorkflowInspection(
        run_id=run.id,
        workflow_name=run.workflow_name,
        workflow_version=run.workflow_version,
        status=run.status,
        namespace=run.namespace,
        current_step=run.current_step,
        steps=steps,
        input_data=run.input_data,
        output_data=run.output_data,
        error=error_info,
        parent_run_id=run.parent_run_id,
        created_at=run.created_at,
        updated_at=run.updated_at,
        completed_at=run.completed_at,
        duration_ms=duration_ms,
    )
