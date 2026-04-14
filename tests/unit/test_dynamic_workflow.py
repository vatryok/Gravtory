"""Unit tests for dynamic workflow creation."""

from __future__ import annotations

from datetime import timedelta

import pytest

from gravtory.core.dag import DAG
from gravtory.core.dynamic import (
    create_dynamic_workflow,
    definition_from_json,
    definition_to_json,
)
from gravtory.core.errors import InvalidWorkflowError


async def _importable_step() -> str:
    """Module-level importable step for serialization round-trip tests."""
    return "result"


class TestDynamicWorkflow:
    """Tests for create_dynamic_workflow factory."""

    def test_dynamic_workflow_creation(self) -> None:
        """Dynamic workflow with 3 steps creates a valid definition."""

        async def extract() -> list[str]:
            return ["a", "b"]

        async def transform(data: list[str]) -> list[str]:
            return [x.upper() for x in data]

        async def load(data: list[str]) -> None:
            pass

        defn = create_dynamic_workflow(
            name="etl-pipeline",
            steps=[
                {"order": 1, "function": extract, "name": "extract"},
                {"order": 2, "function": transform, "name": "transform", "depends_on": 1},
                {"order": 3, "function": load, "name": "load", "depends_on": 2},
            ],
        )

        assert defn.name == "etl-pipeline"
        assert len(defn.steps) == 3
        assert defn.steps[1].name == "extract"
        assert defn.steps[2].depends_on == [1]
        assert defn.steps[3].depends_on == [2]

    def test_dynamic_workflow_dag_validation(self) -> None:
        """DAG validation runs on a dynamic workflow and detects cycles."""

        async def noop() -> None:
            pass

        defn = create_dynamic_workflow(
            name="cyclic",
            steps=[
                {"order": 1, "function": noop, "depends_on": 2},
                {"order": 2, "function": noop, "depends_on": 1},
            ],
        )

        with pytest.raises(InvalidWorkflowError, match="Circular dependency"):
            DAG(defn.steps)

    def test_dynamic_workflow_defaults(self) -> None:
        """Default values are set correctly for optional step fields."""

        async def my_step() -> int:
            return 42

        defn = create_dynamic_workflow(
            name="simple",
            steps=[{"order": 1, "function": my_step}],
        )

        assert defn.steps[1].name == "my_step"
        assert defn.steps[1].depends_on == []
        assert defn.steps[1].retries == 0
        assert defn.steps[1].backoff is None
        assert defn.steps[1].function is my_step
        assert defn.version == 1
        assert defn.config.namespace == "default"

    def test_dynamic_workflow_with_options(self) -> None:
        """Dynamic workflow respects custom options."""
        from datetime import timedelta

        async def s1() -> None:
            pass

        defn = create_dynamic_workflow(
            name="custom",
            steps=[
                {
                    "order": 1,
                    "function": s1,
                    "retries": 3,
                    "backoff": "exponential",
                    "timeout": timedelta(seconds=30),
                },
            ],
            version=2,
            namespace="prod",
            saga=True,
        )

        assert defn.version == 2
        assert defn.config.namespace == "prod"
        assert defn.config.saga_enabled is True
        assert defn.steps[1].retries == 3
        assert defn.steps[1].backoff == "exponential"
        assert defn.steps[1].timeout == timedelta(seconds=30)

    def test_dynamic_workflow_multi_depends(self) -> None:
        """Steps can depend on multiple other steps."""

        async def noop() -> None:
            pass

        defn = create_dynamic_workflow(
            name="fan-in",
            steps=[
                {"order": 1, "function": noop},
                {"order": 2, "function": noop},
                {"order": 3, "function": noop, "depends_on": [1, 2]},
            ],
        )

        assert defn.steps[3].depends_on == [1, 2]
        # Should build a valid DAG
        dag = DAG(defn.steps)
        assert set(dag.get_roots()) == {1, 2}


class TestDynamicWorkflowGapFill:
    """Gap-fill tests for dynamic workflow edge cases."""

    def test_single_step_workflow(self) -> None:
        async def only() -> str:
            return "done"

        defn = create_dynamic_workflow(
            name="single",
            steps=[{"order": 1, "function": only}],
        )
        assert len(defn.steps) == 1
        dag = DAG(defn.steps)
        assert dag.topological_sort() == [1]

    def test_long_chain(self) -> None:
        """Dynamic workflow with a 10-step chain."""

        async def noop() -> None:
            pass

        steps = [{"order": 1, "function": noop}]
        for i in range(2, 11):
            steps.append({"order": i, "function": noop, "depends_on": i - 1})

        defn = create_dynamic_workflow(name="chain-10", steps=steps)
        assert len(defn.steps) == 10
        dag = DAG(defn.steps)
        assert dag.topological_sort() == list(range(1, 11))

    def test_compensate_option(self) -> None:
        async def s() -> None:
            pass

        defn = create_dynamic_workflow(
            name="saga-dyn",
            steps=[{"order": 1, "function": s, "compensate": "undo_s"}],
            saga=True,
        )
        assert defn.steps[1].compensate == "undo_s"
        assert defn.config.saga_enabled is True


class TestSerializationRoundTrip:
    """Verify definition_to_json / definition_from_json round-trip fidelity."""

    def test_basic_round_trip(self) -> None:
        defn = create_dynamic_workflow(
            name="rt-basic",
            steps=[{"order": 1, "function": _importable_step, "name": "step_a"}],
        )
        raw = definition_to_json(defn)
        assert isinstance(raw, str)
        restored = definition_from_json(raw)
        assert restored.name == defn.name
        assert restored.version == defn.version
        assert len(restored.steps) == len(defn.steps)
        assert restored.steps[1].name == "step_a"
        assert restored.steps[1].function is _importable_step

    def test_round_trip_preserves_config(self) -> None:
        defn = create_dynamic_workflow(
            name="rt-cfg",
            steps=[{"order": 1, "function": _importable_step}],
            version=3,
            deadline=timedelta(seconds=120),
            priority=5,
            namespace="production",
            saga=True,
        )
        restored = definition_from_json(definition_to_json(defn))
        assert restored.config.priority == 5
        assert restored.config.namespace == "production"
        assert restored.config.saga_enabled is True
        assert restored.config.deadline == timedelta(seconds=120)
        assert restored.version == 3

    def test_round_trip_preserves_step_options(self) -> None:
        defn = create_dynamic_workflow(
            name="rt-opts",
            steps=[
                {
                    "order": 1,
                    "function": _importable_step,
                    "retries": 3,
                    "backoff": "exponential",
                    "timeout": timedelta(seconds=30),
                    "compensate": "undo_step",
                },
                {
                    "order": 2,
                    "function": _importable_step,
                    "depends_on": 1,
                },
            ],
        )
        restored = definition_from_json(definition_to_json(defn))
        assert restored.steps[1].retries == 3
        assert restored.steps[1].backoff == "exponential"
        assert restored.steps[1].timeout == timedelta(seconds=30)
        assert restored.steps[1].compensate == "undo_step"
        assert restored.steps[2].depends_on == [1]

    def test_round_trip_json_is_string(self) -> None:
        defn = create_dynamic_workflow(
            name="rt-type",
            steps=[{"order": 1, "function": _importable_step}],
        )
        raw = definition_to_json(defn)
        assert isinstance(raw, str)
        assert not isinstance(raw, bytes)
