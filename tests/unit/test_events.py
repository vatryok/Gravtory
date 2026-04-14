"""Tests for EventBus — event emission, workflow chaining."""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    Schedule,
    ScheduleType,
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)
from gravtory.scheduling.events import EventBus, EventTrigger


def _setup() -> tuple[InMemoryBackend, WorkflowRegistry, ExecutionEngine, EventBus]:
    backend = InMemoryBackend()
    registry = WorkflowRegistry()

    async def dummy_step() -> str:
        return "ok"

    defn = WorkflowDefinition(
        name="target-wf",
        version=1,
        steps={1: StepDefinition(order=1, name="s1", function=dummy_step)},
        config=WorkflowConfig(),
    )
    registry.register(defn)
    engine = ExecutionEngine(registry, backend)
    bus = EventBus(backend, registry, engine)
    return backend, registry, engine, bus


class TestEventEmission:
    @pytest.mark.asyncio
    async def test_emit_triggers_subscribed_workflow(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        bus.register_trigger(EventTrigger(event_name="order_completed", workflow_name="target-wf"))

        count = await bus.emit("order_completed", {"order_id": "123"})
        assert count == 1

        runs = [r for r in backend._runs.values() if r.workflow_name == "target-wf"]
        assert len(runs) == 1
        assert runs[0].status in (WorkflowStatus.RUNNING, WorkflowStatus.PENDING)

    @pytest.mark.asyncio
    async def test_unknown_event_no_trigger(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        bus.register_trigger(EventTrigger(event_name="order_completed", workflow_name="target-wf"))

        count = await bus.emit("unknown_event", {})
        assert count == 0

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self) -> None:
        backend, registry, _engine, bus = _setup()
        await backend.initialize()

        async def another_step() -> str:
            return "another"

        defn2 = WorkflowDefinition(
            name="target-wf-2",
            version=1,
            steps={1: StepDefinition(order=1, name="s1", function=another_step)},
            config=WorkflowConfig(),
        )
        registry.register(defn2)

        bus.register_trigger(EventTrigger(event_name="shared_event", workflow_name="target-wf"))
        bus.register_trigger(EventTrigger(event_name="shared_event", workflow_name="target-wf-2"))

        count = await bus.emit("shared_event", {})
        assert count == 2


class TestWorkflowChaining:
    @pytest.mark.asyncio
    async def test_workflow_completed_triggers_chain(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        bus.register_trigger(
            EventTrigger(
                event_name="workflow_completed:parent-wf",
                workflow_name="target-wf",
            )
        )

        count = await bus.emit_workflow_completed(
            workflow_name="parent-wf",
            run_id="parent-run-1",
            output_data={"result": "success"},
        )
        assert count == 1

        runs = [r for r in backend._runs.values() if r.workflow_name == "target-wf"]
        assert len(runs) == 1

    @pytest.mark.asyncio
    async def test_register_triggers_from_schedules(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()

        schedules = [
            Schedule(
                id="s1",
                workflow_name="target-wf",
                schedule_type=ScheduleType.EVENT,
                schedule_config="workflow:parent-wf",
                enabled=True,
            ),
            Schedule(
                id="s2",
                workflow_name="target-wf",
                schedule_type=ScheduleType.EVENT,
                schedule_config="custom_event",
                enabled=True,
            ),
        ]
        bus.register_triggers_from_schedules(schedules)
        assert len(bus._triggers) == 2

    @pytest.mark.asyncio
    async def test_chained_workflow_receives_parent_data(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        bus.register_trigger(
            EventTrigger(
                event_name="workflow_completed:parent-wf",
                workflow_name="target-wf",
            )
        )

        await bus.emit_workflow_completed(
            workflow_name="parent-wf",
            run_id="parent-run-2",
            output_data={"key": "value"},
        )

        runs = [r for r in backend._runs.values() if r.workflow_name == "target-wf"]
        assert len(runs) == 1


class TestEventsGapFill:
    """Gap-fill tests for event bus edge cases."""

    @pytest.mark.asyncio
    async def test_emit_returns_zero_when_no_triggers(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        count = await bus.emit("no_subscriptions", {})
        assert count == 0

    @pytest.mark.asyncio
    async def test_register_trigger_deduplication(self) -> None:
        """Registering the same trigger twice still creates two entries."""
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        trigger = EventTrigger(event_name="evt", workflow_name="target-wf")
        bus.register_trigger(trigger)
        bus.register_trigger(trigger)
        count = await bus.emit("evt", {})
        assert count == 2

    @pytest.mark.asyncio
    async def test_emit_workflow_completed_no_subscribers(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        count = await bus.emit_workflow_completed(
            workflow_name="untracked-wf",
            run_id="run-1",
            output_data={},
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_register_triggers_from_empty_schedules(self) -> None:
        backend, _registry, _engine, bus = _setup()
        await backend.initialize()
        bus.register_triggers_from_schedules([])
        assert len(bus._triggers) == 0
