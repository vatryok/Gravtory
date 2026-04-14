"""Tests for RateLimiter (token bucket)."""

from __future__ import annotations

import time

import pytest

from gravtory.workers.rate_limit import RateLimiter


class TestRateLimiterBasic:
    @pytest.mark.asyncio
    async def test_acquire_succeeds_when_tokens_available(self) -> None:
        rl = RateLimiter("test", max_tokens=5.0, refill_rate=1.0)
        wait = await rl.acquire(1.0)
        assert wait == 0.0
        assert rl.available_tokens == pytest.approx(4.0, abs=0.5)

    @pytest.mark.asyncio
    async def test_acquire_returns_wait_when_empty(self) -> None:
        rl = RateLimiter("test", max_tokens=2.0, refill_rate=1.0)
        await rl.acquire(2.0)
        wait = await rl.acquire(1.0)
        assert wait > 0.0

    @pytest.mark.asyncio
    async def test_refill_adds_tokens_over_time(self) -> None:
        rl = RateLimiter("test", max_tokens=10.0, refill_rate=100.0)
        await rl.acquire(10.0)
        time.sleep(0.05)
        available = rl.available_tokens
        assert available > 0.0

    @pytest.mark.asyncio
    async def test_refill_does_not_exceed_max(self) -> None:
        rl = RateLimiter("test", max_tokens=5.0, refill_rate=100.0)
        time.sleep(0.1)
        assert rl.available_tokens <= 5.0

    @pytest.mark.asyncio
    async def test_name_property(self) -> None:
        rl = RateLimiter("my-bucket", max_tokens=10.0, refill_rate=1.0)
        assert rl.name == "my-bucket"

    @pytest.mark.asyncio
    async def test_multiple_acquires_drain_tokens(self) -> None:
        rl = RateLimiter("test", max_tokens=3.0, refill_rate=0.0)
        assert await rl.acquire(1.0) == 0.0
        assert await rl.acquire(1.0) == 0.0
        assert await rl.acquire(1.0) == 0.0
        wait = await rl.acquire(1.0)
        assert wait > 0.0

    @pytest.mark.asyncio
    async def test_partial_acquire(self) -> None:
        rl = RateLimiter("test", max_tokens=5.0, refill_rate=0.0)
        assert await rl.acquire(3.0) == 0.0
        assert await rl.acquire(3.0) > 0.0

    @pytest.mark.asyncio
    async def test_zero_refill_rate_never_refills(self) -> None:
        rl = RateLimiter("test", max_tokens=1.0, refill_rate=0.0)
        await rl.acquire(1.0)
        time.sleep(0.01)
        assert rl.available_tokens == pytest.approx(0.0, abs=0.01)


class TestRateLimiterGapFill:
    """Gap-fill tests for rate limiter edge cases."""

    @pytest.mark.asyncio
    async def test_acquire_exact_remaining(self) -> None:
        """Acquiring exactly available tokens succeeds."""
        rl = RateLimiter("test", max_tokens=5.0, refill_rate=0.0)
        wait = await rl.acquire(5.0)
        assert wait == 0.0

    @pytest.mark.asyncio
    async def test_fractional_acquire(self) -> None:
        rl = RateLimiter("test", max_tokens=1.0, refill_rate=0.0)
        wait = await rl.acquire(0.5)
        assert wait == 0.0
        assert rl.available_tokens == pytest.approx(0.5, abs=0.1)

    @pytest.mark.asyncio
    async def test_high_refill_rate(self) -> None:
        rl = RateLimiter("test", max_tokens=100.0, refill_rate=10000.0)
        await rl.acquire(100.0)
        time.sleep(0.02)
        assert rl.available_tokens > 0.0

    @pytest.mark.asyncio
    async def test_zero_refill_returns_inf_wait(self) -> None:
        rl = RateLimiter("test", max_tokens=1.0, refill_rate=0.0)
        await rl.acquire(1.0)  # drain
        wait = await rl.acquire(1.0)
        assert wait == float("inf")

    @pytest.mark.asyncio
    async def test_backend_rate_limit_acquire(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        mock_backend = MagicMock()
        mock_backend.rate_limit_acquire = AsyncMock(return_value=0.0)
        rl = RateLimiter("test", max_tokens=10.0, refill_rate=1.0, backend=mock_backend)
        wait = await rl.acquire(1.0)
        assert wait == 0.0
        mock_backend.rate_limit_acquire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_backend_without_rate_limit_method(self) -> None:
        from unittest.mock import MagicMock

        mock_backend = MagicMock(spec=[])  # no rate_limit_acquire
        rl = RateLimiter("test", max_tokens=10.0, refill_rate=1.0, backend=mock_backend)
        wait = await rl.acquire(1.0)
        assert wait == 0.0  # falls back to local
