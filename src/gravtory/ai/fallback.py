# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Model routing — intelligent LLM model selection and fallback.

Provides :class:`ModelRouter` for condition-based routing,
cost-optimised selection, and quality-first selection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from gravtory.ai.tokens import MODEL_COSTS, MODEL_QUALITY

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class ModelRoute:
    """A single routing rule mapping a condition to a model.

    The *condition* receives an arbitrary context dict and returns True
    if this route should be selected.  The default condition always
    matches (acts as a catch-all).
    """

    model: str
    condition: Callable[[dict[str, Any]], bool] = field(
        default_factory=lambda: _always_true,
    )


def _always_true(_ctx: dict[str, Any]) -> bool:
    return True


class ModelRouter:
    """Route LLM calls to the optimal model based on rules.

    Usage::

        router = ModelRouter([
            ModelRoute("gpt-4", condition=lambda ctx: ctx.get("priority", 0) > 5),
            ModelRoute("gpt-3.5-turbo"),  # fallback (always True)
        ])

        model = router.select({"priority": 8})  # → "gpt-4"
    """

    def __init__(self, routes: list[ModelRoute]) -> None:
        if not routes:
            msg = "ModelRouter requires at least one route"
            raise ValueError(msg)
        self._routes = list(routes)

    @property
    def routes(self) -> list[ModelRoute]:
        """Return a copy of the configured routes."""
        return list(self._routes)

    def select(self, context: dict[str, Any] | None = None) -> str:
        """Select the best model for the given *context*.

        Evaluates routes in order; the first whose condition returns
        ``True`` wins.  If nothing matches, the last route's model is
        returned as a fallback.
        """
        ctx = context or {}
        for route in self._routes:
            if route.condition(ctx):
                return route.model
        return self._routes[-1].model

    # ── Factory helpers ───────────────────────────────────────────

    @classmethod
    def cost_optimized(cls, models: list[str]) -> ModelRouter:
        """Create a router sorted cheapest-first.

        All routes use a catch-all condition.  Pair with a fallback
        strategy that tries each model in order on failure.
        """
        sorted_models = sorted(
            models,
            key=lambda m: MODEL_COSTS.get(m, 999.0),
        )
        routes = [ModelRoute(model=m) for m in sorted_models]
        return cls(routes)

    @classmethod
    def quality_first(cls, models: list[str]) -> ModelRouter:
        """Create a router sorted best-quality-first."""
        sorted_models = sorted(
            models,
            key=lambda m: MODEL_QUALITY.get(m, 0),
            reverse=True,
        )
        routes = [ModelRoute(model=m) for m in sorted_models]
        return cls(routes)
