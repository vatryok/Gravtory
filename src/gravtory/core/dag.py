# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""DAG builder — resolves step dependencies and execution order."""

from __future__ import annotations

import warnings
from collections import deque
from typing import TYPE_CHECKING

from gravtory.core.errors import InvalidWorkflowError

if TYPE_CHECKING:
    from gravtory.core.types import StepDefinition, StepResult


class DAG:
    """Directed Acyclic Graph for workflow step dependencies.

    Computes execution order via topological sort, detects cycles,
    and determines which steps are ready to run given completed steps.
    """

    def __init__(self, steps: dict[int, StepDefinition]) -> None:
        """Build DAG from step definitions.

        Args:
            steps: Dict mapping step_order -> StepDefinition.

        Raises:
            InvalidWorkflowError: If DAG has cycles, self-deps, or missing deps.
        """
        self._steps = steps
        self._adjacency: dict[int, list[int]] = {}  # step -> dependents
        self._reverse: dict[int, list[int]] = {}  # step -> dependencies
        self._build()
        self._validate()

    def _build(self) -> None:
        """Build adjacency and reverse adjacency lists."""
        for order in self._steps:
            self._adjacency.setdefault(order, [])
            self._reverse.setdefault(order, [])

        for order, step_def in self._steps.items():
            for dep in step_def.depends_on:
                if dep in self._steps:
                    self._adjacency.setdefault(dep, []).append(order)
                    self._reverse[order].append(dep)

    def _validate(self) -> None:
        """Validate DAG structure. Collects all errors before raising."""
        errors: list[str] = []

        # 1. Self-dependency
        for order, step_def in self._steps.items():
            if order in step_def.depends_on:
                errors.append(f"Step {order} depends on itself")

        # 2. Missing dependency
        all_orders = set(self._steps.keys())
        for order, step_def in self._steps.items():
            for dep in step_def.depends_on:
                if dep not in all_orders:
                    errors.append(f"Step {order} depends on step {dep} which does not exist")

        # 3. Cycle detection (Kahn's algorithm)
        if not errors:
            in_degree: dict[int, int] = dict.fromkeys(self._steps, 0)
            for order in self._steps:
                for _dep in self._reverse[order]:
                    in_degree[order] += 1

            queue: deque[int] = deque()
            for order, deg in in_degree.items():
                if deg == 0:
                    queue.append(order)

            visited = 0
            while queue:
                node = queue.popleft()
                visited += 1
                for dependent in self._adjacency.get(node, []):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

            if visited < len(self._steps):
                # Find cycle participants
                cycle_nodes = [o for o, d in in_degree.items() if d > 0]
                errors.append(f"Circular dependency detected involving steps: {cycle_nodes}")

        # 4. Orphan detection (warning only)
        if not errors:
            roots = self.get_roots()
            reachable: set[int] = set()
            visit_queue: deque[int] = deque(roots)
            while visit_queue:
                node = visit_queue.popleft()
                if node in reachable:
                    continue
                reachable.add(node)
                for dependent in self._adjacency.get(node, []):
                    visit_queue.append(dependent)

            for order in self._steps:
                if order not in reachable:
                    warnings.warn(
                        f"Step {order} is unreachable from any root step",
                        stacklevel=2,
                    )

        if errors:
            raise InvalidWorkflowError("dag", "; ".join(errors))

    def topological_sort(self) -> list[int]:
        """Return step orders in a valid execution sequence using Kahn's algorithm."""
        in_degree: dict[int, int] = {o: len(self._reverse[o]) for o in self._steps}

        queue: deque[int] = deque(sorted(o for o, d in in_degree.items() if d == 0))
        result: list[int] = []

        while queue:
            node = queue.popleft()
            result.append(node)
            for dependent in sorted(self._adjacency.get(node, [])):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def get_ready_steps(self, completed_steps: dict[int, StepResult]) -> list[StepDefinition]:
        """Get steps whose ALL dependencies are satisfied.

        A step is "ready" if:
          - It has not been completed/skipped yet
          - ALL of its dependencies are in completed_steps
        """
        ready: list[StepDefinition] = []
        completed_orders = set(completed_steps.keys())

        for order, step_def in self._steps.items():
            if order in completed_orders:
                continue
            deps = set(self._reverse.get(order, []))
            if deps.issubset(completed_orders):
                ready.append(step_def)

        return ready

    def get_next_steps(
        self, current_step: int, completed_steps: dict[int, StepResult]
    ) -> list[StepDefinition]:
        """After completing current_step, which new steps become ready?

        Returns steps that depend on current_step and have all other deps completed.
        """
        completed_orders = set(completed_steps.keys()) | {current_step}
        next_steps: list[StepDefinition] = []

        for dependent in self._adjacency.get(current_step, []):
            if dependent in completed_orders:
                continue
            deps = set(self._reverse.get(dependent, []))
            if deps.issubset(completed_orders):
                next_steps.append(self._steps[dependent])

        return next_steps

    def all_steps_done(self, completed_steps: dict[int, StepResult]) -> bool:
        """Check if all steps in the DAG are completed or skipped."""
        return len(completed_steps) >= len(self._steps)

    def get_roots(self) -> list[int]:
        """Get step orders that have no dependencies (entry points)."""
        return sorted(o for o in self._steps if not self._reverse.get(o))

    def get_leaves(self) -> list[int]:
        """Get step orders that no other step depends on (exit points)."""
        return sorted(o for o in self._steps if not self._adjacency.get(o))

    def __repr__(self) -> str:
        """Human-readable DAG representation."""
        lines = [f"DAG({len(self._steps)} steps)"]
        for order in self.topological_sort():
            deps = self._reverse.get(order, [])
            dep_str = f" (depends on {deps})" if deps else " (root)"
            lines.append(f"  step {order}: {self._steps[order].name}{dep_str}")
        return "\n".join(lines)
