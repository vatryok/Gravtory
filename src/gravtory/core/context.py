# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""StepContext — provides access to previous step outputs for conditions and input resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from gravtory.core.types import StepResult


class StepContext:
    """Context object passed to condition functions and available during step execution.

    Allows steps and conditions to access outputs of completed steps.
    """

    def __init__(
        self,
        completed_steps: Mapping[int, StepResult],
        workflow_kwargs: dict[str, Any],
        workflow_run_id: str,
    ) -> None:
        self._completed = completed_steps
        self._kwargs = workflow_kwargs
        self._run_id = workflow_run_id

    _MISSING = object()

    def output(self, step_order: int, default: Any = _MISSING) -> Any:
        """Get the output of a completed step by its order number.

        Usage in condition:
            condition=lambda ctx: ctx.output(1)["amount"] > 1000

        A *default* can be provided to avoid KeyError when the step
        has not completed yet — useful in condition lambdas::

            condition=lambda ctx: ctx.output(1, {}).get("approved")

        Raises:
            KeyError: If step_order hasn't completed yet and no default given.
        """
        if step_order not in self._completed:
            if default is not self._MISSING:
                return default
            raise KeyError(
                f"Step {step_order} has not completed yet. "
                f"Available: {list(self._completed.keys())}"
            )
        return self._completed[step_order].output

    def has_output(self, step_order: int) -> bool:
        """Check if a step has completed and has output available."""
        return step_order in self._completed

    @property
    def workflow_run_id(self) -> str:
        """The workflow run ID."""
        return self._run_id

    @property
    def kwargs(self) -> dict[str, Any]:
        """Original workflow kwargs."""
        return self._kwargs
