# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Dynamic workflow creation — define workflows at runtime without decorators."""

from __future__ import annotations

import importlib
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

if TYPE_CHECKING:
    from collections.abc import Callable

    from gravtory.backends.base import Backend


def create_dynamic_workflow(
    name: str,
    steps: list[dict[str, Any]],
    *,
    version: int = 1,
    deadline: timedelta | None = None,
    priority: int = 0,
    namespace: str = "default",
    saga: bool = False,
) -> WorkflowDefinition:
    """Create a workflow definition dynamically (without decorators).

    Usage::

        workflow = create_dynamic_workflow(
            name="etl-pipeline",
            steps=[
                {"order": 1, "function": extract, "name": "extract"},
                {"order": 2, "function": transform, "name": "transform",
                 "depends_on": 1},
                {"order": 3, "function": load, "name": "load",
                 "depends_on": 2},
            ],
        )

    Each step dict supports these keys:
      - **order** (int, required): Step execution order.
      - **function** (callable, required): The step function.
      - **name** (str): Step name (defaults to function.__name__).
      - **depends_on** (int | list[int]): Dependencies.
      - **retries** (int): Max retry attempts.
      - **backoff** (str): Backoff strategy.
      - **compensate** (str): Compensation handler name.
      - **condition** (callable): Condition function.
      - **timeout** (timedelta): Step timeout.

    Returns:
        A WorkflowDefinition that can be registered and executed.
    """
    step_defs: dict[int, StepDefinition] = {}

    for s in steps:
        func: Callable[..., Any] = s["function"]
        depends_on_raw = s.get("depends_on")
        if depends_on_raw is None:
            deps: list[int] = []
        elif isinstance(depends_on_raw, int):
            deps = [depends_on_raw]
        else:
            deps = list(depends_on_raw)

        step_def = StepDefinition(
            order=s["order"],
            name=s.get("name", func.__name__),
            depends_on=deps,
            function=func,
            retries=s.get("retries", 0),
            backoff=s.get("backoff"),
            backoff_base=s.get("backoff_base", 1.0),
            backoff_max=s.get("backoff_max", 300.0),
            backoff_multiplier=s.get("backoff_multiplier", 2.0),
            jitter=s.get("jitter", False),
            compensate=s.get("compensate"),
            condition=s.get("condition"),
            timeout=s.get("timeout"),
        )
        step_defs[step_def.order] = step_def

    return WorkflowDefinition(
        name=name,
        version=version,
        steps=step_defs,
        config=WorkflowConfig(
            deadline=deadline,
            priority=priority,
            namespace=namespace,
            saga_enabled=saga,
            version=version,
        ),
    )


# ── Serialization helpers ────────────────────────────────────────────


def _func_ref(func: Any) -> str:
    """Serialize a callable to 'module:qualname' string."""
    mod = getattr(func, "__module__", None)
    qname = getattr(func, "__qualname__", None) or getattr(func, "__name__", None)
    if mod is None or qname is None:
        raise ValueError(f"Cannot serialize function {func!r}: missing __module__ or __qualname__")
    return f"{mod}:{qname}"


_ALLOWED_MODULES: set[str] | None = None


def configure_allowed_modules(modules: set[str] | None) -> None:
    """Set the allowlist of module prefixes for dynamic function resolution.

    Pass a set of module prefixes (e.g. ``{"myapp.", "mylib."}``) to restrict
    which modules can be imported via ``function_ref``.  Pass ``None`` to
    disable allowlist checking (NOT recommended in production).
    """
    global _ALLOWED_MODULES
    _ALLOWED_MODULES = modules


def _resolve_func(ref: str) -> Any:
    """Resolve a 'module:qualname' string back to a callable.

    If an allowlist has been configured via :func:`configure_allowed_modules`,
    the module portion must match at least one allowed prefix.
    """
    mod_name, qual_name = ref.rsplit(":", 1)
    if _ALLOWED_MODULES is not None and not any(
        mod_name == m or mod_name.startswith(m) for m in _ALLOWED_MODULES
    ):
        raise ImportError(
            f"Module {mod_name!r} is not in the allowed modules list for "
            f"dynamic workflow resolution. Allowed prefixes: {_ALLOWED_MODULES}"
        )
    mod = importlib.import_module(mod_name)
    obj: Any = mod
    for part in qual_name.split("."):
        obj = getattr(obj, part)
    return obj


def definition_to_json(defn: WorkflowDefinition) -> str:
    """Serialize a WorkflowDefinition to a JSON string for persistence.

    Functions are stored as ``"module:qualname"`` references so they can
    be re-imported on reload. This means the function must be importable
    at the top level of its module.
    """
    steps_data: list[dict[str, Any]] = []
    for order, step in sorted(defn.steps.items()):
        sd: dict[str, Any] = {
            "order": order,
            "name": step.name,
            "depends_on": step.depends_on,
            "retries": step.retries,
            "backoff": step.backoff,
            "backoff_base": step.backoff_base,
            "backoff_max": step.backoff_max,
            "backoff_multiplier": step.backoff_multiplier,
            "jitter": step.jitter,
            "compensate": step.compensate,
            "priority": step.priority,
            "rate_limit": step.rate_limit,
        }
        if step.function is not None:
            sd["function_ref"] = _func_ref(step.function)
        if step.timeout is not None:
            sd["timeout_seconds"] = step.timeout.total_seconds()
        steps_data.append(sd)

    data: dict[str, Any] = {
        "name": defn.name,
        "version": defn.version,
        "steps": steps_data,
        "config": {
            "priority": defn.config.priority,
            "namespace": defn.config.namespace,
            "saga_enabled": defn.config.saga_enabled,
            "max_concurrent": defn.config.max_concurrent,
        },
    }
    if defn.config.deadline is not None:
        data["config"]["deadline_seconds"] = defn.config.deadline.total_seconds()
    return json.dumps(data)


def definition_from_json(raw: str) -> WorkflowDefinition:
    """Deserialize a WorkflowDefinition from a JSON string.

    Re-imports step functions via their stored ``"module:qualname"`` references.
    """
    data = json.loads(raw)
    step_defs: dict[int, StepDefinition] = {}
    for sd in data["steps"]:
        func = None
        if "function_ref" in sd:
            func = _resolve_func(sd["function_ref"])
        timeout = None
        if "timeout_seconds" in sd:
            timeout = timedelta(seconds=sd["timeout_seconds"])
        step_defs[sd["order"]] = StepDefinition(
            order=sd["order"],
            name=sd.get("name", ""),
            depends_on=sd.get("depends_on", []),
            function=func,
            retries=sd.get("retries", 0),
            backoff=sd.get("backoff"),
            backoff_base=sd.get("backoff_base", 1.0),
            backoff_max=sd.get("backoff_max", 300.0),
            backoff_multiplier=sd.get("backoff_multiplier", 2.0),
            jitter=sd.get("jitter", False),
            compensate=sd.get("compensate"),
            timeout=timeout,
            priority=sd.get("priority", 0),
            rate_limit=sd.get("rate_limit"),
        )

    cfg = data.get("config", {})
    deadline = None
    if "deadline_seconds" in cfg:
        deadline = timedelta(seconds=cfg["deadline_seconds"])

    return WorkflowDefinition(
        name=data["name"],
        version=data.get("version", 1),
        steps=step_defs,
        config=WorkflowConfig(
            deadline=deadline,
            priority=cfg.get("priority", 0),
            namespace=cfg.get("namespace", "default"),
            saga_enabled=cfg.get("saga_enabled", False),
            max_concurrent=cfg.get("max_concurrent", 0),
            version=data.get("version", 1),
        ),
    )


# ── Persistence helpers ──────────────────────────────────────────────


async def persist_dynamic_workflow(backend: Backend, defn: WorkflowDefinition) -> None:
    """Serialize and persist a dynamic workflow definition to the backend."""
    raw = definition_to_json(defn)
    await backend.save_workflow_definition(defn.name, defn.version, raw)


async def load_persisted_workflows(backend: Backend) -> list[WorkflowDefinition]:
    """Load all persisted dynamic workflow definitions from the backend."""
    rows = await backend.load_workflow_definitions()
    results: list[WorkflowDefinition] = []
    for _name, _version, raw in rows:
        results.append(definition_from_json(raw))
    return results
