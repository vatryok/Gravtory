"""Tests for ModelRouter — intelligent model selection and fallback."""

from __future__ import annotations

import pytest

from gravtory.ai.fallback import ModelRoute, ModelRouter
from gravtory.ai.tokens import MODEL_COSTS, MODEL_QUALITY


class TestModelRouter:
    """ModelRouter unit tests."""

    def test_condition_based_routing(self) -> None:
        """Router selects model based on condition evaluation."""
        router = ModelRouter(
            [
                ModelRoute("gpt-4", condition=lambda ctx: ctx.get("priority", 0) > 5),
                ModelRoute("gpt-3.5-turbo"),  # fallback
            ]
        )
        assert router.select({"priority": 8}) == "gpt-4"
        assert router.select({"priority": 2}) == "gpt-3.5-turbo"

    def test_fallback_to_last_route(self) -> None:
        """When no condition matches, the last route's model is returned."""
        router = ModelRouter(
            [
                ModelRoute("gpt-4", condition=lambda ctx: False),
                ModelRoute("gpt-3.5-turbo", condition=lambda ctx: False),
                ModelRoute("claude-3-haiku"),  # default catch-all
            ]
        )
        assert router.select({"anything": True}) == "claude-3-haiku"

    def test_empty_context(self) -> None:
        """select() with no context uses default catch-all."""
        router = ModelRouter([ModelRoute("gpt-4")])
        assert router.select() == "gpt-4"
        assert router.select(None) == "gpt-4"

    def test_cost_optimized_sorts_cheapest_first(self) -> None:
        """cost_optimized creates a router sorted by ascending cost."""
        router = ModelRouter.cost_optimized(["gpt-4", "gpt-3.5-turbo", "claude-3-haiku"])
        models = [r.model for r in router.routes]
        # Verify cost ordering
        costs = [MODEL_COSTS.get(m, 999.0) for m in models]
        assert costs == sorted(costs)
        # Cheapest should be first
        assert models[0] == "claude-3-haiku"

    def test_quality_first_sorts_best_first(self) -> None:
        """quality_first creates a router sorted by descending quality."""
        router = ModelRouter.quality_first(["gpt-3.5-turbo", "gpt-4", "claude-3-haiku"])
        models = [r.model for r in router.routes]
        qualities = [MODEL_QUALITY.get(m, 0) for m in models]
        assert qualities == sorted(qualities, reverse=True)
        # Best quality should be first
        assert models[0] == "gpt-4"

    def test_routes_property(self) -> None:
        """routes returns a copy of the configured routes."""
        r1 = ModelRoute("gpt-4")
        router = ModelRouter([r1])
        routes = router.routes
        assert len(routes) == 1
        assert routes[0].model == "gpt-4"
        # Mutation doesn't affect internal state
        routes.clear()
        assert len(router.routes) == 1

    def test_empty_routes_raises(self) -> None:
        """ModelRouter with empty routes raises ValueError."""
        with pytest.raises(ValueError, match="at least one route"):
            ModelRouter([])


class TestModelRouterGapFill:
    """Gap-fill tests for model router edge cases."""

    def test_single_route_always_selected(self) -> None:
        r = ModelRoute(model="gpt-4")
        router = ModelRouter([r])
        selected = router.select()
        assert selected == "gpt-4"

    def test_many_routes_fallthrough(self) -> None:
        """First matching condition wins; catch-all at end."""
        routes = [
            ModelRoute(model="special", condition=lambda ctx: ctx.get("vip") is True),
            ModelRoute(model="default"),
        ]
        router = ModelRouter(routes)
        assert router.select({"vip": True}) == "special"
        assert router.select({}) == "default"
