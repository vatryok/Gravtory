"""Unit tests for CircuitBreaker."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from gravtory.core.errors import CircuitOpenError
from gravtory.retry.circuit_breaker import CircuitBreaker, _CircuitState


class TestCircuitBreaker:
    """Tests for circuit breaker state transitions."""

    @pytest.mark.asyncio
    async def test_closed_passes_through(self) -> None:
        """In CLOSED state, calls pass through normally."""
        cb = CircuitBreaker("test", failure_threshold=5)

        async def ok() -> str:
            return "success"

        result = await cb.call(ok)
        assert result == "success"
        assert cb.state == _CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_threshold_opens_circuit(self) -> None:
        """After failure_threshold failures, circuit transitions to OPEN."""
        cb = CircuitBreaker("test", failure_threshold=3)

        async def fail() -> None:
            raise RuntimeError("boom")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await cb.call(fail)

        assert cb.state == _CircuitState.OPEN
        assert cb.failure_count == 3

    @pytest.mark.asyncio
    async def test_open_rejects_calls(self) -> None:
        """In OPEN state, calls are rejected with CircuitOpenError."""
        cb = CircuitBreaker("test", failure_threshold=1)

        async def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

        async def ok() -> str:
            return "should not reach"

        with pytest.raises(CircuitOpenError, match="test"):
            await cb.call(ok)

    @pytest.mark.asyncio
    async def test_recovery_timeout_half_open(self) -> None:
        """After recovery_timeout, OPEN transitions to HALF_OPEN."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.5)

        async def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

        # Patch time.monotonic to simulate elapsed time
        original_opened = cb._state.opened_at
        assert original_opened is not None
        with patch(
            "gravtory.retry.circuit_breaker.time.monotonic", return_value=original_opened + 1.0
        ):
            cb._maybe_transition_to_half_open()

        assert cb.state == _CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_success_closes(self) -> None:
        """A successful call in HALF_OPEN transitions to CLOSED."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.0)

        async def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

        # Force transition to HALF_OPEN by simulating elapsed time
        opened_at = cb._state.opened_at
        assert opened_at is not None
        with patch("gravtory.retry.circuit_breaker.time.monotonic", return_value=opened_at + 1.0):

            async def ok() -> str:
                return "recovered"

            result = await cb.call(ok)

        assert result == "recovered"
        assert cb.state == _CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens(self) -> None:
        """A failed call in HALF_OPEN transitions back to OPEN."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.0)

        async def fail() -> None:
            raise RuntimeError("boom")

        # Open the circuit
        with pytest.raises(RuntimeError):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

        # Force transition to HALF_OPEN
        opened_at = cb._state.opened_at
        assert opened_at is not None
        with (
            patch("gravtory.retry.circuit_breaker.time.monotonic", return_value=opened_at + 1.0),
            pytest.raises(RuntimeError),
        ):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_manual_reset(self) -> None:
        """Manual reset returns circuit to CLOSED state."""
        cb = CircuitBreaker("test", failure_threshold=1)

        async def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await cb.call(fail)

        assert cb.state == _CircuitState.OPEN

        await cb.reset()

        assert cb.state == _CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_failures_below_threshold_stay_closed(self) -> None:
        """Failures below threshold keep circuit CLOSED."""
        cb = CircuitBreaker("test", failure_threshold=5)

        async def fail() -> None:
            raise RuntimeError("boom")

        for _ in range(4):
            with pytest.raises(RuntimeError):
                await cb.call(fail)

        assert cb.state == _CircuitState.CLOSED
        assert cb.failure_count == 4

    @pytest.mark.asyncio
    async def test_half_open_max_limits_probe_calls(self) -> None:
        """In HALF_OPEN, only half_open_max probe calls are allowed."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.0, half_open_max=1)

        async def fail() -> None:
            raise RuntimeError("boom")

        # Open the circuit
        with pytest.raises(RuntimeError):
            await cb.call(fail)
        assert cb.state == _CircuitState.OPEN

        # Force transition to HALF_OPEN
        opened_at = cb._state.opened_at
        assert opened_at is not None

        call_count = 0

        async def slow_ok() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        # First probe call should be allowed (half_open_max=1)
        with patch("gravtory.retry.circuit_breaker.time.monotonic", return_value=opened_at + 1.0):
            result = await cb.call(slow_ok)
        assert result == "ok"
        assert call_count == 1


class TestCircuitBreakerGapFill:
    """Gap-fill tests for circuit breaker edge cases."""

    @pytest.mark.asyncio
    async def test_concurrent_requests_through_circuit(self) -> None:
        """Multiple concurrent calls pass through CLOSED circuit."""
        import asyncio

        cb = CircuitBreaker("concurrent", failure_threshold=10)
        results: list[str] = []

        async def ok(idx: int) -> str:
            await asyncio.sleep(0.01)
            return f"result-{idx}"

        tasks = [cb.call(ok, i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        assert len(results) == 5
        assert cb.state == _CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_in_closed_does_not_reset_failures(self) -> None:
        """In CLOSED state, success does not reset failure count (only HALF_OPEN does)."""
        cb = CircuitBreaker("reset", failure_threshold=5)

        async def fail() -> None:
            raise RuntimeError("boom")

        async def ok() -> str:
            return "good"

        # Accumulate 3 failures (still below threshold=5, so CLOSED)
        for _ in range(3):
            with pytest.raises(RuntimeError):
                await cb.call(fail)
        assert cb.failure_count == 3
        assert cb.state == _CircuitState.CLOSED

        # Success in CLOSED state — failure count stays
        await cb.call(ok)
        assert cb.failure_count == 3

    @pytest.mark.asyncio
    async def test_circuit_name_property(self) -> None:
        """Circuit breaker exposes its name."""
        cb = CircuitBreaker("my-service", failure_threshold=3)
        assert cb.name == "my-service"
