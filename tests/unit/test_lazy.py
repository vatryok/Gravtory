"""Unit tests for LazyStepOutput proxy."""

from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.core.lazy import LazyStepOutput
from gravtory.core.types import StepOutput, StepStatus


@dataclasses.dataclass
class Order:
    item: str
    qty: int


class TestLazyLoadsOnFirstGet:
    @pytest.mark.asyncio
    async def test_loads_from_db(self) -> None:
        """First .get() loads from the backend."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        blob = engine.process({"item": "widget", "qty": 5})

        backend = MagicMock()
        backend.get_step_output = AsyncMock(
            return_value=StepOutput(
                workflow_run_id="run-1",
                step_order=1,
                step_name="s1",
                output_data=blob,
                status=StepStatus.COMPLETED,
            )
        )

        lazy = LazyStepOutput("run-1", 1, backend, engine)
        assert lazy.loaded is False
        result = await lazy.get()
        assert lazy.loaded is True
        assert result == {"item": "widget", "qty": 5}
        backend.get_step_output.assert_awaited_once_with("run-1", 1)

    @pytest.mark.asyncio
    async def test_caches_result(self) -> None:
        """Second .get() returns cached value without DB call."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        blob = engine.process(42)

        backend = MagicMock()
        backend.get_step_output = AsyncMock(
            return_value=StepOutput(
                workflow_run_id="run-1",
                step_order=1,
                step_name="s1",
                output_data=blob,
                status=StepStatus.COMPLETED,
            )
        )

        lazy = LazyStepOutput("run-1", 1, backend, engine)
        first = await lazy.get()
        second = await lazy.get()
        assert first == second == 42
        # Only one DB call
        assert backend.get_step_output.await_count == 1

    @pytest.mark.asyncio
    async def test_none_output(self) -> None:
        """Step with no output_data returns None."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        backend = MagicMock()
        backend.get_step_output = AsyncMock(return_value=None)

        lazy = LazyStepOutput("run-1", 1, backend, engine)
        result = await lazy.get()
        assert result is None
        assert lazy.loaded is True

    @pytest.mark.asyncio
    async def test_typed_restoration(self) -> None:
        """LazyStepOutput with output_type reconstructs dataclass."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        blob = engine.process({"item": "gear", "qty": 3})

        backend = MagicMock()
        backend.get_step_output = AsyncMock(
            return_value=StepOutput(
                workflow_run_id="run-1",
                step_order=1,
                step_name="s1",
                output_data=blob,
                status=StepStatus.COMPLETED,
            )
        )

        lazy = LazyStepOutput("run-1", 1, backend, engine, output_type=Order)
        result = await lazy.get()
        assert isinstance(result, Order)
        assert result.item == "gear"
        assert result.qty == 3


class TestLazyGapFill:
    """Gap-fill tests for LazyStepOutput edge cases."""

    @pytest.mark.asyncio
    async def test_loaded_flag_after_none(self) -> None:
        """loaded is True even when result is None."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        backend = MagicMock()
        backend.get_step_output = AsyncMock(return_value=None)

        lazy = LazyStepOutput("run-1", 1, backend, engine)
        await lazy.get()
        assert lazy.loaded is True

    @pytest.mark.asyncio
    async def test_multiple_lazy_independent(self) -> None:
        """Two LazyStepOutputs for different steps are independent."""
        from gravtory.core.checkpoint import CheckpointEngine

        engine = CheckpointEngine(serializer="json")
        blob1 = engine.process("first")
        blob2 = engine.process("second")

        backend = MagicMock()
        backend.get_step_output = AsyncMock(
            side_effect=[
                StepOutput(
                    workflow_run_id="r",
                    step_order=1,
                    step_name="s1",
                    output_data=blob1,
                    status=StepStatus.COMPLETED,
                ),
                StepOutput(
                    workflow_run_id="r",
                    step_order=2,
                    step_name="s2",
                    output_data=blob2,
                    status=StepStatus.COMPLETED,
                ),
            ]
        )

        lazy1 = LazyStepOutput("r", 1, backend, engine)
        lazy2 = LazyStepOutput("r", 2, backend, engine)
        assert await lazy1.get() == "first"
        assert await lazy2.get() == "second"
