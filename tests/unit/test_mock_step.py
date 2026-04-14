"""Tests for MockStep, FailNTimes, DelayedMock (Section 11.2)."""

from __future__ import annotations

import pytest

from gravtory.testing.mocks import DelayedMock, FailNTimes, MockStep


class TestMockStep:
    @pytest.mark.asyncio
    async def test_mock_step_tracks_calls(self) -> None:
        mock = MockStep(return_value={"id": "123"})
        result = await mock(order_id="abc", amount=100)
        assert result == {"id": "123"}
        assert mock.call_count == 1
        assert mock.last_call_args == {"order_id": "abc", "amount": 100}
        assert mock.call_history == [{"order_id": "abc", "amount": 100}]

        # Second call
        await mock(order_id="def")
        assert mock.call_count == 2
        assert mock.last_call_args == {"order_id": "def"}

    @pytest.mark.asyncio
    async def test_mock_step_raises(self) -> None:
        mock = MockStep(raises=ValueError)
        with pytest.raises(ValueError):
            await mock(x=1)
        assert mock.call_count == 1

    @pytest.mark.asyncio
    async def test_mock_step_side_effect(self) -> None:
        mock = MockStep(side_effect=lambda **kw: kw["x"] * 2)
        result = await mock(x=5)
        assert result == 10
        assert mock.call_count == 1

    @pytest.mark.asyncio
    async def test_mock_step_reset(self) -> None:
        mock = MockStep(return_value="ok")
        await mock(a=1)
        await mock(b=2)
        assert mock.call_count == 2
        mock.reset()
        assert mock.call_count == 0
        assert mock.call_history == []
        assert mock.last_call_args is None


class TestFailNTimes:
    @pytest.mark.asyncio
    async def test_fail_n_times(self) -> None:
        fnt = FailNTimes(failures=2, exception=ConnectionError, success_value="ok")
        with pytest.raises(ConnectionError):
            await fnt(x=1)
        with pytest.raises(ConnectionError):
            await fnt(x=2)
        result = await fnt(x=3)
        assert result == "ok"
        assert fnt.attempt == 3

    @pytest.mark.asyncio
    async def test_fail_n_times_reset(self) -> None:
        fnt = FailNTimes(failures=1, success_value="done")
        with pytest.raises(RuntimeError):
            await fnt()
        fnt.reset()
        assert fnt.attempt == 0
        with pytest.raises(RuntimeError):
            await fnt()


class TestDelayedMock:
    @pytest.mark.asyncio
    async def test_delayed_mock(self) -> None:
        dm = DelayedMock(delay=0.01, return_value="done")
        result = await dm(x=1)
        assert result == "done"


class TestMockStepGapFill:
    """Gap-fill tests for testing mocks."""

    @pytest.mark.asyncio
    async def test_mock_step_no_args(self) -> None:
        """MockStep handles calls with no arguments."""
        mock = MockStep(return_value="ok")
        result = await mock()
        assert result == "ok"
        assert mock.call_count == 1
        assert mock.last_call_args == {}

    @pytest.mark.asyncio
    async def test_fail_n_times_zero_failures(self) -> None:
        """FailNTimes with 0 failures always succeeds."""
        fnt = FailNTimes(failures=0, success_value="instant")
        result = await fnt()
        assert result == "instant"
        assert fnt.attempt == 1

    @pytest.mark.asyncio
    async def test_mock_step_multiple_side_effects(self) -> None:
        """Side effect can produce different results based on input."""
        mock = MockStep(side_effect=lambda **kw: kw.get("x", 0) ** 2)
        assert await mock(x=3) == 9
        assert await mock(x=5) == 25
        assert mock.call_count == 2

    @pytest.mark.asyncio
    async def test_delayed_mock_returns_correct_value(self) -> None:
        """DelayedMock returns value after delay on multiple calls."""
        dm = DelayedMock(delay=0.01, return_value="tracked")
        r1 = await dm(a=1)
        r2 = await dm(b=2)
        assert r1 == "tracked"
        assert r2 == "tracked"
