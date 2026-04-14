"""Tests for scheduling.events — EventBus, EventTrigger, workflow chaining."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import (
    Schedule,
    ScheduleType,
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
)
from gravtory.scheduling.events import EventBus, EventSubscription, EventTrigger


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


@pytest.fixture
def registry() -> MagicMock:
    step1 = StepDefinition(name="step_a", order=1, retries=0, depends_on=[])
    definition = WorkflowDefinition(
        name="target-wf",
        version=1,
        steps={1: step1},
        config=WorkflowConfig(priority=5),
    )
    reg = MagicMock()
    reg.get.return_value = definition
    return reg


@pytest.fixture
def engine() -> MagicMock:
    return MagicMock()


class TestEventTrigger:
    def test_defaults(self) -> None:
        t = EventTrigger(event_name="my_event", workflow_name="wf1")
        assert t.namespace == "default"


class TestEventSubscription:
    def test_fields(self) -> None:
        cb = lambda *a: None
        s = EventSubscription(event_name="evt", callback=cb)
        assert s.event_name == "evt"
        assert s.callback is cb


class TestEventBusRegister:
    def test_register_trigger(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        trigger = EventTrigger(event_name="order_placed", workflow_name="process-order")
        bus.register_trigger(trigger)
        assert len(bus._triggers) == 1

    def test_register_triggers_from_event_schedules(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        schedules = [
            Schedule(
                id="s1",
                workflow_name="chain-wf",
                schedule_type=ScheduleType.EVENT,
                schedule_config="workflow:parent-wf",
                namespace="default",
                enabled=True,
            ),
            Schedule(
                id="s2",
                workflow_name="custom-wf",
                schedule_type=ScheduleType.EVENT,
                schedule_config="custom_event",
                namespace="default",
                enabled=True,
            ),
            Schedule(
                id="s3",
                workflow_name="disabled-wf",
                schedule_type=ScheduleType.EVENT,
                schedule_config="some_event",
                namespace="default",
                enabled=False,
            ),
            Schedule(
                id="s4",
                workflow_name="cron-wf",
                schedule_type=ScheduleType.CRON,
                schedule_config="* * * * *",
                namespace="default",
                enabled=True,
            ),
        ]
        bus.register_triggers_from_schedules(schedules)
        # Only 2 should be registered (s1 and s2 are EVENT+enabled)
        assert len(bus._triggers) == 2
        events = [t.event_name for t in bus._triggers]
        assert "workflow_completed:parent-wf" in events
        assert "custom_event" in events

    def test_subscribe(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        called = []
        bus.subscribe("my_event", lambda name, data: called.append(name))
        assert len(bus._subscriptions) == 1


class TestEventBusEmit:
    @pytest.mark.asyncio
    async def test_emit_triggers_workflow(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        bus.register_trigger(EventTrigger(event_name="go", workflow_name="target-wf"))
        count = await bus.emit("go", {"key": "value"})
        assert count == 1
        # A workflow run should have been created
        runs = await backend.list_workflow_runs(namespace="default")
        assert len(runs) >= 1

    @pytest.mark.asyncio
    async def test_emit_no_matching_triggers(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        count = await bus.emit("unmatched_event")
        assert count == 0

    @pytest.mark.asyncio
    async def test_emit_calls_subscriptions(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        received: list[dict] = []
        bus.subscribe("my_event", lambda name, data: received.append(data))
        await bus.emit("my_event", {"val": 42})
        assert len(received) == 1
        assert received[0]["val"] == 42

    @pytest.mark.asyncio
    async def test_emit_calls_async_subscriptions(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        received: list[str] = []

        async def async_callback(name: str, data: dict) -> None:
            received.append(name)

        bus.subscribe("async_event", async_callback)
        await bus.emit("async_event")
        assert received == ["async_event"]

    @pytest.mark.asyncio
    async def test_emit_isolates_trigger_errors(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        # Register trigger that will fail (registry.get raises)
        bus.register_trigger(EventTrigger(event_name="fail_event", workflow_name="bad-wf"))
        registry.get.side_effect = KeyError("not found")

        # Should not raise
        count = await bus.emit("fail_event")
        assert count == 0

    @pytest.mark.asyncio
    async def test_emit_isolates_subscription_errors(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)

        def bad_callback(name: str, data: dict) -> None:
            raise RuntimeError("broken callback")

        bus.subscribe("evt", bad_callback)
        # Should not raise
        await bus.emit("evt", {"data": True})


class TestEventBusWorkflowCompleted:
    @pytest.mark.asyncio
    async def test_emit_workflow_completed(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        bus.register_trigger(
            EventTrigger(
                event_name="workflow_completed:parent-wf",
                workflow_name="target-wf",
            )
        )
        count = await bus.emit_workflow_completed("parent-wf", "run-parent-1", {"result": "ok"})
        assert count == 1

    @pytest.mark.asyncio
    async def test_emit_workflow_completed_no_output(
        self, backend: InMemoryBackend, registry: MagicMock, engine: MagicMock
    ) -> None:
        bus = EventBus(backend, registry, engine)
        bus.register_trigger(
            EventTrigger(
                event_name="workflow_completed:wf-a",
                workflow_name="target-wf",
            )
        )
        count = await bus.emit_workflow_completed("wf-a", "run-1")
        assert count == 1
