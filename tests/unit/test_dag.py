"""Unit tests for the DAG computation engine."""

import pytest

from gravtory.core.dag import DAG
from gravtory.core.errors import InvalidWorkflowError
from gravtory.core.types import StepDefinition, StepResult, StepStatus


def _make_steps(*specs: tuple[int, list[int]]) -> dict[int, StepDefinition]:
    """Helper: specs are (order, depends_on) tuples."""
    return {
        order: StepDefinition(order=order, name=f"step_{order}", depends_on=deps)
        for order, deps in specs
    }


class TestDAGLinearChain:
    def test_linear_chain_sorts_correctly(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [2]))
        dag = DAG(steps)
        assert dag.topological_sort() == [1, 2, 3]

    def test_linear_chain_roots_and_leaves(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [2]))
        dag = DAG(steps)
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [3]


class TestDAGFanOut:
    def test_fan_out_both_ready_after_root(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [1]))
        dag = DAG(steps)
        completed = {1: StepResult(output="done", status=StepStatus.COMPLETED)}
        ready = dag.get_ready_steps(completed)
        ready_orders = sorted(s.order for s in ready)
        assert ready_orders == [2, 3]

    def test_fan_out_topological_sort(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [1]))
        dag = DAG(steps)
        result = dag.topological_sort()
        assert result[0] == 1
        assert set(result[1:]) == {2, 3}


class TestDAGFanIn:
    def test_fan_in_waits_for_all(self) -> None:
        steps = _make_steps((1, []), (2, []), (3, [1, 2]))
        dag = DAG(steps)
        # Only step 1 done — step 3 not ready
        completed = {1: StepResult(output="a")}
        ready = dag.get_ready_steps(completed)
        assert all(s.order != 3 for s in ready)

        # Both done — step 3 ready
        completed[2] = StepResult(output="b")
        ready = dag.get_ready_steps(completed)
        assert any(s.order == 3 for s in ready)

    def test_fan_in_topological_sort(self) -> None:
        steps = _make_steps((1, []), (2, []), (3, [1, 2]))
        dag = DAG(steps)
        result = dag.topological_sort()
        assert result[-1] == 3
        assert set(result[:2]) == {1, 2}


class TestDAGDiamond:
    def test_diamond_shape(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [1]), (4, [2, 3]))
        dag = DAG(steps)
        result = dag.topological_sort()
        assert result[0] == 1
        assert result[-1] == 4
        assert set(result[1:3]) == {2, 3}


class TestDAGSingleStep:
    def test_single_step(self) -> None:
        steps = _make_steps((1, []))
        dag = DAG(steps)
        assert dag.topological_sort() == [1]
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [1]


class TestDAGValidation:
    def test_cycle_detection(self) -> None:
        steps = _make_steps((1, [2]), (2, [1]))
        with pytest.raises(InvalidWorkflowError, match="Circular dependency"):
            DAG(steps)

    def test_self_dependency(self) -> None:
        steps = _make_steps((1, [1]))
        with pytest.raises(InvalidWorkflowError, match="depends on itself"):
            DAG(steps)

    def test_missing_dependency(self) -> None:
        steps = _make_steps((2, [99]))
        with pytest.raises(InvalidWorkflowError, match="does not exist"):
            DAG(steps)


class TestGetReadySteps:
    def test_initial_returns_roots(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [1]))
        dag = DAG(steps)
        ready = dag.get_ready_steps({})
        assert len(ready) == 1
        assert ready[0].order == 1

    def test_after_completion_returns_newly_ready(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [2]))
        dag = DAG(steps)
        completed = {1: StepResult(output="done")}
        ready = dag.get_ready_steps(completed)
        assert len(ready) == 1
        assert ready[0].order == 2

    def test_returns_empty_when_all_done(self) -> None:
        steps = _make_steps((1, []), (2, [1]))
        dag = DAG(steps)
        completed = {
            1: StepResult(output="a"),
            2: StepResult(output="b"),
        }
        assert dag.get_ready_steps(completed) == []


class TestGetNextSteps:
    def test_next_after_root(self) -> None:
        steps = _make_steps((1, []), (2, [1]), (3, [1]))
        dag = DAG(steps)
        completed: dict[int, StepResult] = {}
        next_steps = dag.get_next_steps(1, completed)
        assert sorted(s.order for s in next_steps) == [2, 3]


class TestAllStepsDone:
    def test_all_done_true(self) -> None:
        steps = _make_steps((1, []), (2, [1]))
        dag = DAG(steps)
        completed = {
            1: StepResult(output="a"),
            2: StepResult(output="b"),
        }
        assert dag.all_steps_done(completed) is True

    def test_all_done_false(self) -> None:
        steps = _make_steps((1, []), (2, [1]))
        dag = DAG(steps)
        completed = {1: StepResult(output="a")}
        assert dag.all_steps_done(completed) is False


class TestDAGComplexPatterns:
    """Gap-fill tests for complex DAG topologies."""

    def test_complex_diamond_dag(self) -> None:
        """4 steps in a diamond: 1 -> [2, 3] -> 4."""
        steps = _make_steps((1, []), (2, [1]), (3, [1]), (4, [2, 3]))
        dag = DAG(steps)
        order = dag.topological_sort()
        assert order[0] == 1
        assert order[-1] == 4
        assert set(order[1:3]) == {2, 3}
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [4]

    def test_wide_fan_out(self) -> None:
        """1 -> [2, 3, 4, 5, 6]."""
        specs: list[tuple[int, list[int]]] = [(1, [])] + [(i, [1]) for i in range(2, 7)]
        steps = _make_steps(*specs)
        dag = DAG(steps)
        completed = {1: StepResult(output="done", status=StepStatus.COMPLETED)}
        ready = dag.get_ready_steps(completed)
        assert sorted(s.order for s in ready) == [2, 3, 4, 5, 6]
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [2, 3, 4, 5, 6]

    def test_deep_chain(self) -> None:
        """10-step linear chain: 1->2->3->...->10."""
        specs = [(1, [])] + [(i, [i - 1]) for i in range(2, 11)]
        steps = _make_steps(*specs)
        dag = DAG(steps)
        assert dag.topological_sort() == list(range(1, 11))
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [10]

    def test_mixed_dag(self) -> None:
        """Combination: 1->[2,3], 2->4, 3->4, 4->5 (fan-out, fan-in, linear)."""
        steps = _make_steps(
            (1, []),
            (2, [1]),
            (3, [1]),
            (4, [2, 3]),
            (5, [4]),
        )
        dag = DAG(steps)
        order = dag.topological_sort()
        assert order[0] == 1
        assert order[-1] == 5
        assert order.index(4) > order.index(2)
        assert order.index(4) > order.index(3)

    def test_topological_sort_stability(self) -> None:
        """Same input -> same output (deterministic)."""
        steps = _make_steps((1, []), (2, [1]), (3, [1]), (4, [2, 3]))
        dag = DAG(steps)
        first = dag.topological_sort()
        second = dag.topological_sort()
        assert first == second

    def test_get_roots_multiple(self) -> None:
        """Multiple independent roots: {1, 2, 3} with no dependencies."""
        steps = _make_steps((1, []), (2, []), (3, []))
        dag = DAG(steps)
        assert dag.get_roots() == [1, 2, 3]

    def test_get_leaves_multiple(self) -> None:
        """Multiple leaves: 1->[2,3] — leaves are 2 and 3."""
        steps = _make_steps((1, []), (2, [1]), (3, [1]))
        dag = DAG(steps)
        assert dag.get_leaves() == [2, 3]

    def test_three_node_cycle(self) -> None:
        """Cycle among 3 nodes is detected."""
        steps = _make_steps((1, [3]), (2, [1]), (3, [2]))
        with pytest.raises(InvalidWorkflowError, match="Circular dependency"):
            DAG(steps)


class TestDAGRepr:
    def test_repr(self) -> None:
        steps = _make_steps((1, []), (2, [1]))
        dag = DAG(steps)
        r = repr(dag)
        assert "DAG(2 steps)" in r
        assert "step 1" in r
        assert "step 2" in r


class TestDAGGapFill:
    """Gap-fill tests for DAG edge cases."""

    def test_single_node_dag(self) -> None:
        steps = _make_steps((1, []))
        dag = DAG(steps)
        assert dag.topological_sort() == [1]
        assert dag.get_roots() == [1]
        assert dag.get_leaves() == [1]

    def test_wide_fan_out(self) -> None:
        """Many nodes depending on a single root."""
        specs = [(1, [])] + [(i, [1]) for i in range(2, 12)]
        steps = _make_steps(*specs)
        dag = DAG(steps)
        assert dag.get_roots() == [1]
        assert set(dag.get_leaves()) == set(range(2, 12))

    def test_long_linear_chain(self) -> None:
        specs = [(1, [])] + [(i, [i - 1]) for i in range(2, 21)]
        steps = _make_steps(*specs)
        dag = DAG(steps)
        assert dag.topological_sort() == list(range(1, 21))
