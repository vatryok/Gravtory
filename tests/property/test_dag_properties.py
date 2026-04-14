"""Property-based tests for DAG computation engine using Hypothesis.

Tests invariants that must hold for ANY valid DAG:
- Topological sort respects all dependency edges
- Roots have no dependencies
- Leaves have no dependents
- Cycle detection always raises
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from gravtory.core.dag import DAG
from gravtory.core.errors import InvalidWorkflowError
from gravtory.core.types import StepDefinition

pytestmark = pytest.mark.property


# ── Strategies ───────────────────────────────────────────────────


def _make_step(order: int, deps: list[int]) -> StepDefinition:
    return StepDefinition(order=order, name=f"step_{order}", depends_on=deps)


@st.composite
def valid_dag_steps(draw: st.DrawFn) -> dict[int, StepDefinition]:
    """Generate a valid (acyclic) set of StepDefinitions.

    Strategy: assign random orders 1..n, then each step can only depend
    on steps with a LOWER order number — guarantees acyclicity.
    """
    n = draw(st.integers(min_value=1, max_value=15))
    orders = list(range(1, n + 1))
    steps: dict[int, StepDefinition] = {}
    for o in orders:
        # Can only depend on steps with lower order
        possible_deps = [x for x in orders if x < o]
        deps = (
            draw(
                st.lists(
                    st.sampled_from(possible_deps), max_size=min(3, len(possible_deps)), unique=True
                )
            )
            if possible_deps
            else []
        )
        steps[o] = _make_step(o, deps)
    return steps


@st.composite
def cyclic_dag_steps(draw: st.DrawFn) -> dict[int, StepDefinition]:
    """Generate a set of StepDefinitions that contains a cycle."""
    # Simple cycle: 1→2→3→1
    n = draw(st.integers(min_value=2, max_value=6))
    orders = list(range(1, n + 1))
    steps: dict[int, StepDefinition] = {}
    for i, o in enumerate(orders):
        # Create a cycle: last depends on first, each depends on previous
        if i == 0:
            steps[o] = _make_step(o, [orders[-1]])  # Cycle back
        else:
            steps[o] = _make_step(o, [orders[i - 1]])
    return steps


# ── Properties ───────────────────────────────────────────────────


class TestDAGTopologicalSort:
    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_topo_sort_respects_all_edges(self, steps: dict[int, StepDefinition]) -> None:
        """For every edge A→B, A appears before B in topological order."""
        dag = DAG(steps)
        order = dag.topological_sort()
        pos = {o: i for i, o in enumerate(order)}
        for step_def in steps.values():
            for dep in step_def.depends_on:
                assert pos[dep] < pos[step_def.order], (
                    f"Dependency {dep} should come before {step_def.order}"
                )

    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_topo_sort_contains_all_steps(self, steps: dict[int, StepDefinition]) -> None:
        """Topological sort contains exactly the step orders from input."""
        dag = DAG(steps)
        order = dag.topological_sort()
        assert set(order) == set(steps.keys())

    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_topo_sort_no_duplicates(self, steps: dict[int, StepDefinition]) -> None:
        """Topological sort has no duplicate entries."""
        dag = DAG(steps)
        order = dag.topological_sort()
        assert len(order) == len(set(order))


class TestDAGRoots:
    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_roots_have_no_dependencies(self, steps: dict[int, StepDefinition]) -> None:
        """Every root step has an empty depends_on list."""
        dag = DAG(steps)
        roots = dag.get_roots()
        for r in roots:
            assert steps[r].depends_on == [], f"Root {r} has deps {steps[r].depends_on}"

    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_at_least_one_root(self, steps: dict[int, StepDefinition]) -> None:
        """Every valid DAG has at least one root."""
        dag = DAG(steps)
        assert len(dag.get_roots()) >= 1


class TestDAGLeaves:
    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_leaves_have_no_dependents(self, steps: dict[int, StepDefinition]) -> None:
        """No step depends on a leaf."""
        dag = DAG(steps)
        leaves = set(dag.get_leaves())
        for step_def in steps.values():
            for dep in step_def.depends_on:
                assert dep not in leaves or step_def.order in leaves, (
                    f"Leaf {dep} has dependent {step_def.order}"
                )

    @given(steps=valid_dag_steps())
    @settings(max_examples=100)
    def test_at_least_one_leaf(self, steps: dict[int, StepDefinition]) -> None:
        """Every valid DAG has at least one leaf."""
        dag = DAG(steps)
        assert len(dag.get_leaves()) >= 1


class TestDAGCycleDetection:
    @given(steps=cyclic_dag_steps())
    @settings(max_examples=50)
    def test_cycle_always_detected(self, steps: dict[int, StepDefinition]) -> None:
        """Cyclic dependencies always raise InvalidWorkflowError."""
        with pytest.raises(InvalidWorkflowError):
            DAG(steps)
