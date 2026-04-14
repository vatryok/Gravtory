"""Unit tests for the WorkflowRegistry."""

import pytest

from gravtory.core.errors import (
    InvalidWorkflowError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
)
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition


def _make_definition(
    name: str = "TestWorkflow",
    version: int = 1,
    steps: dict[int, StepDefinition] | None = None,
    saga: bool = False,
) -> WorkflowDefinition:
    if steps is None:
        steps = {
            1: StepDefinition(order=1, name="step_1"),
            2: StepDefinition(order=2, name="step_2", depends_on=[1]),
        }
    return WorkflowDefinition(
        name=name,
        version=version,
        steps=steps,
        config=WorkflowConfig(saga_enabled=saga),
    )


class TestRegisterAndGet:
    def test_register_and_get(self) -> None:
        reg = WorkflowRegistry()
        defn = _make_definition()
        reg.register(defn)
        result = reg.get("TestWorkflow")
        assert result.name == "TestWorkflow"
        assert result.version == 1

    def test_get_by_version(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition(version=1))
        reg.register(_make_definition(version=2))
        v1 = reg.get("TestWorkflow", version=1)
        v2 = reg.get("TestWorkflow", version=2)
        assert v1.version == 1
        assert v2.version == 2

    def test_get_latest_version(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition(version=1))
        reg.register(_make_definition(version=2))
        reg.register(_make_definition(version=3))
        latest = reg.get("TestWorkflow")
        assert latest.version == 3


class TestRegisterInvalid:
    def test_empty_name(self) -> None:
        reg = WorkflowRegistry()
        defn = _make_definition(name="")
        with pytest.raises(InvalidWorkflowError, match="cannot be empty"):
            reg.register(defn)

    def test_no_steps(self) -> None:
        reg = WorkflowRegistry()
        defn = _make_definition(steps={})
        with pytest.raises(InvalidWorkflowError, match="at least one step"):
            reg.register(defn)

    def test_duplicate_name_version(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition())
        with pytest.raises(WorkflowAlreadyExistsError):
            reg.register(_make_definition())

    def test_cycle_in_dag(self) -> None:
        reg = WorkflowRegistry()
        steps = {
            1: StepDefinition(order=1, name="a", depends_on=[2]),
            2: StepDefinition(order=2, name="b", depends_on=[1]),
        }
        defn = _make_definition(steps=steps)
        with pytest.raises(InvalidWorkflowError):
            reg.register(defn)


class TestListWorkflows:
    def test_list_all(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition(name="A"))
        reg.register(_make_definition(name="B"))
        result = reg.list()
        names = {d.name for d in result}
        assert names == {"A", "B"}

    def test_list_empty(self) -> None:
        reg = WorkflowRegistry()
        assert reg.list() == []


class TestGetNotFound:
    def test_unknown_name(self) -> None:
        reg = WorkflowRegistry()
        with pytest.raises(WorkflowNotFoundError):
            reg.get("DoesNotExist")

    def test_unknown_version(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition(version=1))
        with pytest.raises(WorkflowNotFoundError):
            reg.get("TestWorkflow", version=99)


class TestUnregister:
    def test_unregister_all_versions(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition())
        reg.unregister("TestWorkflow")
        with pytest.raises(WorkflowNotFoundError):
            reg.get("TestWorkflow")

    def test_unregister_specific_version(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition(version=1))
        reg.register(_make_definition(version=2))
        reg.unregister("TestWorkflow", version=1)
        assert reg.get("TestWorkflow").version == 2


class TestGetDAG:
    def test_get_dag(self) -> None:
        reg = WorkflowRegistry()
        reg.register(_make_definition())
        dag = reg.get_dag("TestWorkflow")
        assert dag.topological_sort() == [1, 2]


class TestRegistryGapFill:
    """Gap-fill tests for registry edge cases."""

    def test_register_many_versions(self) -> None:
        """Registry handles many versions of the same workflow."""
        reg = WorkflowRegistry()
        for v in range(1, 11):
            reg.register(_make_definition(version=v))
        assert reg.get("TestWorkflow").version == 10
        assert reg.get("TestWorkflow", version=5).version == 5

    def test_register_many_workflows(self) -> None:
        """Registry handles many distinct workflows."""
        reg = WorkflowRegistry()
        for i in range(20):
            reg.register(_make_definition(name=f"Workflow_{i}"))
        result = reg.list()
        assert len(result) == 20

    def test_unregister_nonexistent_raises(self) -> None:
        reg = WorkflowRegistry()
        with pytest.raises(WorkflowNotFoundError):
            reg.unregister("DoesNotExist")

    def test_get_dag_nonexistent(self) -> None:
        reg = WorkflowRegistry()
        with pytest.raises(WorkflowNotFoundError):
            reg.get_dag("NoSuchWorkflow")

    def test_register_single_step_no_deps(self) -> None:
        """Workflow with a single step and no dependencies."""
        reg = WorkflowRegistry()
        defn = _make_definition(steps={1: StepDefinition(order=1, name="only")})
        reg.register(defn)
        dag = reg.get_dag("TestWorkflow")
        assert dag.topological_sort() == [1]
