"""Unit tests for SagaCoordinator."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.core.saga import SagaCoordinator
from gravtory.core.types import (
    StepDefinition,
    StepResult,
    StepStatus,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowStatus,
)


def _make_definition(
    steps: dict[int, StepDefinition],
    *,
    saga_enabled: bool = True,
    workflow_class: type | None = None,
) -> WorkflowDefinition:
    return WorkflowDefinition(
        name="TestWorkflow",
        version=1,
        steps=steps,
        config=WorkflowConfig(saga_enabled=saga_enabled),
        workflow_class=workflow_class,
    )


def _make_backend() -> AsyncMock:
    backend = AsyncMock()
    backend.update_workflow_status = AsyncMock()
    backend.validated_update_workflow_status = AsyncMock()
    backend.add_to_dlq = AsyncMock()
    backend.save_compensation = AsyncMock()
    backend.get_compensations = AsyncMock(return_value=[])
    return backend


def _make_registry(handlers: dict[str, Any] | None = None) -> MagicMock:
    registry = MagicMock()
    handlers = handlers or {}

    def get_comp_handler(wf_name: str, handler_name: str) -> Any:
        if handler_name not in handlers:
            raise KeyError(f"Handler '{handler_name}' not found")
        return handlers[handler_name]

    registry.get_compensation_handler = MagicMock(side_effect=get_comp_handler)
    return registry


class TestSagaCoordinator:
    """Tests for saga compensation logic."""

    @pytest.mark.asyncio
    async def test_compensation_runs_in_reverse_order(self) -> None:
        """Compensations execute in reverse step order (3, 2, 1)."""
        call_order: list[int] = []

        async def undo_1(output: Any) -> None:
            call_order.append(1)

        async def undo_2(output: Any) -> None:
            call_order.append(2)

        async def undo_3(output: Any) -> None:
            call_order.append(3)

        handlers = {"undo_1": undo_1, "undo_2": undo_2, "undo_3": undo_3}
        registry = _make_registry(handlers)
        backend = _make_backend()

        steps = {
            1: StepDefinition(order=1, name="s1", compensate="undo_1"),
            2: StepDefinition(order=2, name="s2", compensate="undo_2"),
            3: StepDefinition(order=3, name="s3", compensate="undo_3"),
        }
        definition = _make_definition(steps)
        completed = {
            1: StepResult(output={"a": 1}, status=StepStatus.COMPLETED),
            2: StepResult(output={"b": 2}, status=StepStatus.COMPLETED),
            3: StepResult(output={"c": 3}, status=StepStatus.COMPLETED),
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=4, definition=definition, completed_steps=completed
        )

        assert call_order == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_compensation_receives_step_output(self) -> None:
        """Each compensation handler receives its step's original output."""
        received: list[Any] = []

        async def undo_1(output: Any) -> None:
            received.append(output)

        registry = _make_registry({"undo_1": undo_1})
        backend = _make_backend()

        steps = {1: StepDefinition(order=1, name="s1", compensate="undo_1")}
        definition = _make_definition(steps)
        completed = {1: StepResult(output={"order_id": "X"}, status=StepStatus.COMPLETED)}

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=2, definition=definition, completed_steps=completed
        )

        assert received == [{"order_id": "X"}]

    @pytest.mark.asyncio
    async def test_all_success_status_compensated(self) -> None:
        """If all compensations succeed, final status is COMPENSATED."""

        async def undo(output: Any) -> None:
            pass

        registry = _make_registry({"undo": undo})
        backend = _make_backend()

        steps = {1: StepDefinition(order=1, name="s1", compensate="undo")}
        definition = _make_definition(steps)
        completed = {1: StepResult(output=None, status=StepStatus.COMPLETED)}

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=2, definition=definition, completed_steps=completed
        )

        # Last call should be COMPENSATED
        calls = [c.args for c in backend.validated_update_workflow_status.call_args_list]
        assert calls[-1] == ("run-1", WorkflowStatus.COMPENSATED)

    @pytest.mark.asyncio
    async def test_any_failure_status_compensation_failed(self) -> None:
        """If any compensation fails, final status is COMPENSATION_FAILED."""

        async def undo_fail(output: Any) -> None:
            raise RuntimeError("refund failed")

        registry = _make_registry({"undo_fail": undo_fail})
        backend = _make_backend()

        steps = {1: StepDefinition(order=1, name="s1", compensate="undo_fail")}
        definition = _make_definition(steps)
        completed = {1: StepResult(output=None, status=StepStatus.COMPLETED)}

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=2, definition=definition, completed_steps=completed
        )

        calls = [c.args for c in backend.validated_update_workflow_status.call_args_list]
        assert calls[-1] == ("run-1", WorkflowStatus.COMPENSATION_FAILED)

    @pytest.mark.asyncio
    async def test_partial_compensation_best_effort(self) -> None:
        """If comp 2 fails, comp 1 still runs (best-effort)."""
        call_order: list[int] = []

        async def undo_1(output: Any) -> None:
            call_order.append(1)

        async def undo_2(output: Any) -> None:
            call_order.append(2)
            raise RuntimeError("undo_2 failed")

        registry = _make_registry({"undo_1": undo_1, "undo_2": undo_2})
        backend = _make_backend()

        steps = {
            1: StepDefinition(order=1, name="s1", compensate="undo_1"),
            2: StepDefinition(order=2, name="s2", compensate="undo_2"),
        }
        definition = _make_definition(steps)
        completed = {
            1: StepResult(output=None, status=StepStatus.COMPLETED),
            2: StepResult(output=None, status=StepStatus.COMPLETED),
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=3, definition=definition, completed_steps=completed
        )

        # Both ran: undo_2 first (reverse order), then undo_1
        assert call_order == [2, 1]

    @pytest.mark.asyncio
    async def test_skipped_steps_not_compensated(self) -> None:
        """Steps with status != COMPLETED are not compensated."""
        call_order: list[int] = []

        async def undo_1(output: Any) -> None:
            call_order.append(1)

        registry = _make_registry({"undo_1": undo_1})
        backend = _make_backend()

        steps = {
            1: StepDefinition(order=1, name="s1", compensate="undo_1"),
        }
        definition = _make_definition(steps)
        completed = {
            1: StepResult(output=None, status=StepStatus.SKIPPED),
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=2, definition=definition, completed_steps=completed
        )

        assert call_order == []

    @pytest.mark.asyncio
    async def test_failed_compensation_added_to_dlq(self) -> None:
        """Failed compensations are added to the DLQ."""

        async def undo_fail(output: Any) -> None:
            raise RuntimeError("boom")

        registry = _make_registry({"undo_fail": undo_fail})
        backend = _make_backend()

        steps = {1: StepDefinition(order=1, name="s1", compensate="undo_fail")}
        definition = _make_definition(steps)
        completed = {1: StepResult(output=None, status=StepStatus.COMPLETED)}

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=2, definition=definition, completed_steps=completed
        )

        backend.add_to_dlq.assert_called_once()
        dlq_entry = backend.add_to_dlq.call_args.args[0]
        assert dlq_entry.workflow_run_id == "run-1"
        assert dlq_entry.step_order == 1
        assert "boom" in (dlq_entry.error_message or "")

    @pytest.mark.asyncio
    async def test_steps_without_compensate_skipped(self) -> None:
        """Steps without a compensate handler are simply skipped."""
        call_order: list[int] = []

        async def undo_2(output: Any) -> None:
            call_order.append(2)

        registry = _make_registry({"undo_2": undo_2})
        backend = _make_backend()

        steps = {
            1: StepDefinition(order=1, name="s1"),  # no compensate
            2: StepDefinition(order=2, name="s2", compensate="undo_2"),
        }
        definition = _make_definition(steps)
        completed = {
            1: StepResult(output=None, status=StepStatus.COMPLETED),
            2: StepResult(output=None, status=StepStatus.COMPLETED),
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-1", failed_step=3, definition=definition, completed_steps=completed
        )

        assert call_order == [2]

    @pytest.mark.asyncio
    async def test_register_saves_compensation(self) -> None:
        """register() persists a Compensation record via the backend."""
        backend = _make_backend()
        registry = _make_registry()
        coordinator = SagaCoordinator(backend, registry)

        await coordinator.register(
            "run-1", step_order=1, step_name="s1", handler_name="undo_1", step_output=b"data"
        )

        backend.save_compensation.assert_called_once()
        comp = backend.save_compensation.call_args.args[0]
        assert comp.workflow_run_id == "run-1"
        assert comp.step_order == 1
        assert comp.handler_name == "undo_1"
        assert comp.status == StepStatus.PENDING


class TestSagaGapFill:
    """Gap-fill tests for saga edge cases."""

    @pytest.mark.asyncio
    async def test_compensation_with_5_steps(self) -> None:
        """Larger saga with 5 steps compensated in reverse order."""
        call_order: list[int] = []

        handlers: dict[str, Any] = {}
        for i in range(1, 6):

            async def make_undo(order: int = i) -> Any:
                async def undo(output: Any) -> None:
                    call_order.append(order)

                return undo

            # We need to create closures properly
            pass

        # Build handlers with proper closures
        async def undo_1(output: Any) -> None:
            call_order.append(1)

        async def undo_2(output: Any) -> None:
            call_order.append(2)

        async def undo_3(output: Any) -> None:
            call_order.append(3)

        async def undo_4(output: Any) -> None:
            call_order.append(4)

        async def undo_5(output: Any) -> None:
            call_order.append(5)

        handlers = {
            "undo_1": undo_1,
            "undo_2": undo_2,
            "undo_3": undo_3,
            "undo_4": undo_4,
            "undo_5": undo_5,
        }
        registry = _make_registry(handlers)
        backend = _make_backend()

        steps = {
            i: StepDefinition(order=i, name=f"s{i}", compensate=f"undo_{i}") for i in range(1, 6)
        }
        definition = _make_definition(steps)
        completed = {
            i: StepResult(output={"step": i}, status=StepStatus.COMPLETED) for i in range(1, 6)
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-5", failed_step=6, definition=definition, completed_steps=completed
        )

        assert call_order == [5, 4, 3, 2, 1]

    @pytest.mark.asyncio
    async def test_compensation_handler_receives_correct_type(self) -> None:
        """Compensation handler receives the exact output type from the step."""
        received_outputs: list[Any] = []

        async def undo_dict(output: Any) -> None:
            received_outputs.append(output)

        async def undo_str(output: Any) -> None:
            received_outputs.append(output)

        registry = _make_registry({"undo_dict": undo_dict, "undo_str": undo_str})
        backend = _make_backend()

        steps = {
            1: StepDefinition(order=1, name="s1", compensate="undo_dict"),
            2: StepDefinition(order=2, name="s2", compensate="undo_str"),
        }
        definition = _make_definition(steps)
        completed = {
            1: StepResult(output={"key": "val"}, status=StepStatus.COMPLETED),
            2: StepResult(output="simple_string", status=StepStatus.COMPLETED),
        }

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-type", failed_step=3, definition=definition, completed_steps=completed
        )

        # Reverse order: step 2's output first, then step 1's
        assert received_outputs == ["simple_string", {"key": "val"}]

    @pytest.mark.asyncio
    async def test_sync_compensation_handler(self) -> None:
        """Sync (non-async) compensation handlers also work."""
        called = False

        def sync_undo(output: Any) -> None:
            nonlocal called
            called = True

        registry = _make_registry({"sync_undo": sync_undo})
        backend = _make_backend()

        steps = {1: StepDefinition(order=1, name="s1", compensate="sync_undo")}
        definition = _make_definition(steps)
        completed = {1: StepResult(output=None, status=StepStatus.COMPLETED)}

        coordinator = SagaCoordinator(backend, registry)
        await coordinator.trigger(
            "run-sync", failed_step=2, definition=definition, completed_steps=completed
        )

        assert called

    @pytest.mark.asyncio
    async def test_get_status_empty(self) -> None:
        """get_status returns zero counts for a run with no compensations."""
        backend = _make_backend()
        registry = _make_registry()
        coordinator = SagaCoordinator(backend, registry)

        status = await coordinator.get_status("run-none")
        assert status == {"total": 0, "completed": 0, "failed": 0, "pending": 0}


class TestSagaDecorator:
    """Tests for the @saga decorator."""

    def test_saga_decorator_sets_flag(self) -> None:
        from gravtory.decorators.saga import saga

        @saga
        class MyWorkflow:
            pass

        assert getattr(MyWorkflow, "__gravtory_saga__", False) is True

    def test_saga_decorator_callable_style(self) -> None:
        from gravtory.decorators.saga import saga

        @saga()
        class MyWorkflow:
            pass

        assert getattr(MyWorkflow, "__gravtory_saga__", False) is True
