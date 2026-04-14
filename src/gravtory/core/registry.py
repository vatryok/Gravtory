# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Workflow registry — stores and retrieves workflow definitions."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from gravtory.core.dag import DAG
from gravtory.core.errors import (
    InvalidWorkflowError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from gravtory.core.types import WorkflowDefinition


class WorkflowRegistry:
    """Stores all registered workflow definitions and provides lookup at runtime."""

    def __init__(self) -> None:
        self._workflows: dict[str, dict[int, WorkflowDefinition]] = {}
        # Outer key: workflow_name, inner key: version
        self._dags: dict[str, dict[int, DAG]] = {}

    def register(self, definition: WorkflowDefinition) -> None:
        """Register a workflow definition.

        Steps:
          1. Validate definition
          2. Build DAG from steps
          3. Store definition
          4. If name+version already registered: raise WorkflowAlreadyExistsError
        """
        errors = self.validate(definition)
        if errors:
            raise InvalidWorkflowError(definition.name, "; ".join(errors))

        name = definition.name
        version = definition.version

        if name in self._workflows and version in self._workflows[name]:
            raise WorkflowAlreadyExistsError(f"{name}@v{version}")

        # Build DAG (validates structure)
        dag = DAG(definition.steps)

        self._workflows.setdefault(name, {})[version] = definition
        self._dags.setdefault(name, {})[version] = dag

    def get(self, workflow_name: str, version: int | None = None) -> WorkflowDefinition:
        """Get a workflow definition by name and optional version.

        If version is None: return latest version.
        If not found: raise WorkflowNotFoundError.
        """
        if workflow_name not in self._workflows:
            raise WorkflowNotFoundError(workflow_name)

        versions = self._workflows[workflow_name]
        if version is not None:
            if version not in versions:
                raise WorkflowNotFoundError(f"{workflow_name}@v{version}")
            return versions[version]

        # Return latest version
        latest = max(versions.keys())
        return versions[latest]

    def get_dag(self, workflow_name: str, version: int | None = None) -> DAG:
        """Get the DAG for a workflow."""
        if workflow_name not in self._dags:
            raise WorkflowNotFoundError(workflow_name)

        dags = self._dags[workflow_name]
        if version is not None:
            if version not in dags:
                raise WorkflowNotFoundError(f"{workflow_name}@v{version}")
            return dags[version]

        latest = max(dags.keys())
        return dags[latest]

    def validate(self, definition: WorkflowDefinition) -> list[str]:
        """Validate a workflow definition. Returns list of error strings."""
        errors: list[str] = []

        if not definition.name:
            errors.append("Workflow name cannot be empty")

        if not definition.steps:
            errors.append("Workflow must have at least one step")
            return errors  # Can't validate further without steps

        # Step orders must be positive integers
        for order in definition.steps:
            if order <= 0:
                errors.append(f"Step order must be positive, got {order}")

        # Step names must be unique
        names = [s.name for s in definition.steps.values()]
        seen_names: set[str] = set()
        for n in names:
            if n in seen_names:
                errors.append(f"Duplicate step name: {n}")
            seen_names.add(n)

        # If saga enabled: at least one step should have compensate
        if definition.config.saga_enabled:
            has_compensate = any(s.compensate for s in definition.steps.values())
            if not has_compensate:
                errors.append(
                    "Saga-enabled workflow must have at least one step with a compensate handler"
                )

        # Compensation handler names must reference existing methods on the class
        if definition.workflow_class is not None:
            for step_def in definition.steps.values():
                if step_def.compensate is not None:
                    handler = getattr(definition.workflow_class, step_def.compensate, None)
                    if handler is None:
                        errors.append(
                            f"Step '{step_def.name}' references compensate handler "
                            f"'{step_def.compensate}' which does not exist on "
                            f"class '{definition.workflow_class.__name__}'"
                        )

        # Step function signatures: class-based steps should have 'self' as first param
        if definition.workflow_class is not None:
            for step_def in definition.steps.values():
                if step_def.function is not None:
                    try:
                        sig = inspect.signature(step_def.function)
                        params = list(sig.parameters.keys())
                        if (
                            params
                            and params[0] != "self"
                            and hasattr(definition.workflow_class, step_def.name)
                        ):
                            errors.append(
                                f"Step '{step_def.name}' is a class method but "
                                f"does not have 'self' as first parameter"
                            )
                    except (ValueError, TypeError):
                        pass

        return errors

    def list(self) -> list[WorkflowDefinition]:
        """List all registered workflow definitions (latest versions)."""
        result: list[WorkflowDefinition] = []
        for _name, versions in self._workflows.items():
            latest = max(versions.keys())
            result.append(versions[latest])
        return result

    def get_compensation_handler(self, workflow_name: str, handler_name: str) -> Callable[..., Any]:
        """Lookup compensation handler by name on the workflow class."""
        definition = self.get(workflow_name)
        cls = definition.workflow_class
        if cls is None:
            raise WorkflowNotFoundError(
                f"Compensation handler '{handler_name}' not found: "
                f"workflow '{workflow_name}' has no class"
            )
        handler = getattr(cls, handler_name, None)
        if handler is None:
            raise WorkflowNotFoundError(
                f"Compensation handler '{handler_name}' not found "
                f"on workflow class '{cls.__name__}'"
            )
        return handler  # type: ignore[no-any-return]

    def unregister(self, workflow_name: str, version: int | None = None) -> None:
        """Remove a workflow definition."""
        if workflow_name not in self._workflows:
            raise WorkflowNotFoundError(workflow_name)

        if version is not None:
            versions = self._workflows[workflow_name]
            if version not in versions:
                raise WorkflowNotFoundError(f"{workflow_name}@v{version}")
            del versions[version]
            if workflow_name in self._dags and version in self._dags[workflow_name]:
                del self._dags[workflow_name][version]
            if not versions:
                del self._workflows[workflow_name]
                self._dags.pop(workflow_name, None)
        else:
            del self._workflows[workflow_name]
            self._dags.pop(workflow_name, None)
