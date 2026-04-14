"""Tests for @wait_for_signal decorator."""

from __future__ import annotations

from datetime import timedelta

from gravtory.core.types import SignalConfig
from gravtory.decorators.signal import wait_for_signal
from gravtory.decorators.step import step


class TestWaitForSignalDecorator:
    def test_sets_signal_config(self) -> None:
        @step(2, depends_on=1)
        @wait_for_signal("approval", timeout=timedelta(hours=24))
        async def wait_approval(self: object, signal_data: dict[str, object]) -> bool:
            return bool(signal_data.get("approved"))

        step_def = wait_approval.__gravtory_step__
        assert step_def.signal_config is not None
        assert step_def.signal_config.name == "approval"
        assert step_def.signal_config.timeout == timedelta(hours=24)

    def test_default_timeout(self) -> None:
        @step(3, depends_on=1)
        @wait_for_signal("ready")
        async def wait_ready(self: object) -> None:
            pass

        step_def = wait_ready.__gravtory_step__
        assert step_def.signal_config is not None
        assert step_def.signal_config.timeout == timedelta(days=7)

    def test_decorator_stores_pending_config(self) -> None:
        """@wait_for_signal on bare function stores config for @step to pick up."""

        @wait_for_signal("approval")
        async def bare_func(self: object) -> None:
            pass

        assert hasattr(bare_func, "__gravtory_signal_config__")
        cfg = bare_func.__gravtory_signal_config__
        assert cfg.name == "approval"
        assert cfg.timeout == timedelta(days=7)

    def test_preserves_function_identity(self) -> None:
        @step(4, depends_on=1)
        @wait_for_signal("go")
        async def my_step(self: object) -> str:
            return "done"

        assert my_step.__name__ == "my_step"

    def test_signal_config_is_correct_type(self) -> None:
        @step(5)
        @wait_for_signal("check", timeout=timedelta(minutes=30))
        async def check_step(self: object) -> None:
            pass

        step_def = check_step.__gravtory_step__
        assert isinstance(step_def.signal_config, SignalConfig)


class TestSignalDecoratorGapFill:
    """Gap-fill tests for @wait_for_signal decorator edge cases."""

    def test_custom_timeout_value(self) -> None:
        @step(6)
        @wait_for_signal("urgent", timeout=timedelta(minutes=5))
        async def urgent_step(self: object) -> None:
            pass

        cfg = urgent_step.__gravtory_step__.signal_config
        assert cfg.timeout == timedelta(minutes=5)

    def test_signal_name_preserved(self) -> None:
        @step(7)
        @wait_for_signal("payment_confirmation")
        async def pay_step(self: object) -> None:
            pass

        cfg = pay_step.__gravtory_step__.signal_config
        assert cfg.name == "payment_confirmation"
