# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@workflow decorator — marks a class as a Gravtory workflow."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta


class WorkflowProxy:
    """Wraps a workflow class, holding its definition and ID template.

    The proxy is what grav.run() receives. It provides:
      - definition: WorkflowDefinition
      - id_template: str (e.g., "order-{order_id}")
      - generate_id(**kwargs) -> str
      - original_class: type (for instantiation)
    """

    def __init__(self, cls: type, definition: WorkflowDefinition, id_template: str) -> None:
        self.original_class = cls
        self.definition = definition
        self.id_template = id_template
        # Preserve class metadata for debugging
        self.__name__ = cls.__name__
        self.__qualname__ = cls.__qualname__
        self.__module__ = cls.__module__
        self.__doc__ = cls.__doc__

    def generate_id(self, **kwargs: Any) -> str:
        """Generate workflow run ID from template and kwargs.

        Delegates to :func:`gravtory.core.id_template.generate_workflow_id`.
        """
        from gravtory.core.id_template import generate_workflow_id

        return generate_workflow_id(self.id_template, **kwargs)

    def __repr__(self) -> str:
        return f"WorkflowProxy({self.__name__}, id_template={self.id_template!r})"


def workflow(
    id: str,
    *,
    version: int = 1,
    deadline: timedelta | None = None,
    priority: int = 0,
    namespace: str = "default",
    saga: bool = False,
) -> Callable[..., Any]:
    """Decorator that marks a class as a Gravtory workflow.

    Usage:
        @workflow(id="order-{order_id}")
        class OrderWorkflow:
            @step(1)
            async def charge(self, order_id: str) -> dict: ...

    What it does:
      1. Scans the class for methods decorated with @step
      2. Extracts StepDefinitions from __gravtory_step__
      3. Creates a WorkflowDefinition
      4. Returns a WorkflowProxy wrapping the original class
      5. Original class is NOT modified (no monkey-patching)
    """

    def decorator(cls_or_func: type | Callable[..., Any]) -> WorkflowProxy:
        if inspect.isclass(cls_or_func):
            return _wrap_class(cls_or_func)
        # Function-based workflows: wrap in a synthetic class
        return _wrap_function(cls_or_func)

    def _wrap_class(cls: type) -> WorkflowProxy:
        # Scan class for @step-decorated methods
        unsorted_steps: dict[int, StepDefinition] = {}
        for attr_name in dir(cls):
            try:
                attr = getattr(cls, attr_name)
            except AttributeError:
                continue
            if hasattr(attr, "__gravtory_step__"):
                step_def: StepDefinition = attr.__gravtory_step__
                step_def.function = attr
                unsorted_steps[step_def.order] = step_def
        # Ensure steps dict is keyed in step-order, not dir()'s alphabetical order
        steps = dict(sorted(unsorted_steps.items()))

        # Read @saga decorator flag — if set on class, enable saga mode
        saga_flag = saga or getattr(cls, "__gravtory_saga__", False)

        # Create WorkflowDefinition
        definition = WorkflowDefinition(
            name=cls.__name__,
            version=version,
            steps=steps,
            config=WorkflowConfig(
                deadline=deadline,
                priority=priority,
                namespace=namespace,
                saga_enabled=saga_flag,
                version=version,
            ),
            workflow_class=cls,
        )

        proxy = WorkflowProxy(cls, definition, id_template=id)

        # Read @schedule decorator metadata — if set on class, attach to proxy
        sched_meta = getattr(cls, "__gravtory_schedule__", None)
        if sched_meta is not None:
            from gravtory.core.types import Schedule

            sched = Schedule(
                id=f"sched-{cls.__name__}",
                workflow_name=cls.__name__,
                schedule_type=sched_meta["type"],
                schedule_config=sched_meta["config"],
                namespace=namespace,
                enabled=sched_meta.get("enabled", True),
            )
            proxy._schedule = sched  # type: ignore[attr-defined]

        return proxy

    def _wrap_function(func: Callable[..., Any]) -> WorkflowProxy:
        # For function-based workflows, create a synthetic class
        # The function itself is treated as a single step
        step_def = StepDefinition(
            order=1,
            name=func.__name__,
            function=func,
        )

        # Create a simple class wrapper
        cls = type(func.__name__, (), {"__module__": func.__module__})

        definition = WorkflowDefinition(
            name=func.__name__,
            version=version,
            steps={1: step_def},
            config=WorkflowConfig(
                deadline=deadline,
                priority=priority,
                namespace=namespace,
                saga_enabled=saga,
                version=version,
            ),
            workflow_class=cls,
        )

        return WorkflowProxy(cls, definition, id_template=id)

    return decorator  # type: ignore[return-value,unused-ignore]
