"""Unit tests for RetryManager, BackoffPolicy, and RetryPolicy."""

from __future__ import annotations

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.retry.policies import BackoffPolicy, RetryManager, RetryPolicy


class TestBackoffPolicy:
    """Tests for the BackoffPolicy dataclass."""

    def test_valid_strategies_accepted(self) -> None:
        for s in ("constant", "linear", "exponential"):
            BackoffPolicy(strategy=s)  # Should not raise

    def test_invalid_strategy_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="Invalid backoff strategy"):
            BackoffPolicy(strategy="invalid")

    def test_callable_strategy_accepted(self) -> None:
        BackoffPolicy(strategy=lambda a: a * 0.5)  # Should not raise


class TestRetryManager:
    """Tests for RetryManager.calculate_delay."""

    def setup_method(self) -> None:
        self.mgr = RetryManager()

    def test_constant_backoff(self) -> None:
        policy = BackoffPolicy(strategy="constant", base_delay=5.0)
        assert self.mgr.calculate_delay(policy, 1) == 5.0
        assert self.mgr.calculate_delay(policy, 2) == 5.0
        assert self.mgr.calculate_delay(policy, 10) == 5.0

    def test_linear_backoff(self) -> None:
        policy = BackoffPolicy(strategy="linear", base_delay=1.0)
        assert self.mgr.calculate_delay(policy, 1) == 1.0
        assert self.mgr.calculate_delay(policy, 2) == 2.0
        assert self.mgr.calculate_delay(policy, 3) == 3.0

    def test_exponential_backoff(self) -> None:
        policy = BackoffPolicy(strategy="exponential", base_delay=1.0, multiplier=2.0)
        assert self.mgr.calculate_delay(policy, 1) == 1.0
        assert self.mgr.calculate_delay(policy, 2) == 2.0
        assert self.mgr.calculate_delay(policy, 3) == 4.0
        assert self.mgr.calculate_delay(policy, 4) == 8.0

    def test_cap_enforcement(self) -> None:
        policy = BackoffPolicy(
            strategy="exponential", base_delay=1.0, multiplier=2.0, max_delay=10.0
        )
        # attempt 5 would be 16.0 uncapped
        assert self.mgr.calculate_delay(policy, 5) == 10.0
        # attempt 10 would be 512.0 uncapped
        assert self.mgr.calculate_delay(policy, 10) == 10.0

    def test_jitter_values_in_range(self) -> None:
        policy = BackoffPolicy(strategy="constant", base_delay=10.0, jitter=True)
        values = [self.mgr.calculate_delay(policy, 1) for _ in range(100)]
        assert all(0 <= v <= 10.0 for v in values)

    def test_jitter_not_all_same(self) -> None:
        policy = BackoffPolicy(strategy="constant", base_delay=10.0, jitter=True)
        values = [self.mgr.calculate_delay(policy, 1) for _ in range(100)]
        assert len(set(values)) > 1, "Jitter should produce varying values"

    def test_custom_callable_strategy(self) -> None:
        policy = BackoffPolicy(strategy=lambda a: a * 0.5, max_delay=100.0)
        assert self.mgr.calculate_delay(policy, 1) == 0.5
        assert self.mgr.calculate_delay(policy, 4) == 2.0
        assert self.mgr.calculate_delay(policy, 10) == 5.0


class TestRetryPolicy:
    """Tests for the RetryPolicy convenience wrapper."""

    def test_to_backoff_policy_from_string(self) -> None:
        rp = RetryPolicy(retries=5, backoff="linear", backoff_base=2.0, backoff_max=60.0)
        bp = rp.to_backoff_policy()
        assert isinstance(bp, BackoffPolicy)
        assert bp.strategy == "linear"
        assert bp.base_delay == 2.0
        assert bp.max_delay == 60.0
        assert bp.multiplier == 2.0  # default
        assert bp.jitter is True  # RetryPolicy default

    def test_to_backoff_policy_from_instance(self) -> None:
        custom = BackoffPolicy(strategy="constant", base_delay=3.0)
        rp = RetryPolicy(backoff=custom)
        assert rp.to_backoff_policy() is custom

    def test_defaults(self) -> None:
        rp = RetryPolicy()
        assert rp.retries == 3
        assert rp.backoff == "exponential"
        assert rp.multiplier == 2.0
        assert rp.jitter is True

    def test_multiplier_passthrough(self) -> None:
        rp = RetryPolicy(backoff="exponential", multiplier=3.0, jitter=False)
        bp = rp.to_backoff_policy()
        assert bp.multiplier == 3.0
        assert bp.jitter is False
        mgr = RetryManager()
        assert mgr.calculate_delay(bp, 2) == 3.0  # 1.0 * 3.0^(2-1)


class TestRetryGapFill:
    """Gap-fill tests for retry edge cases."""

    def test_zero_base_delay(self) -> None:
        """Zero base delay always produces zero delay."""
        mgr = RetryManager()
        for strategy in ("constant", "linear", "exponential"):
            policy = BackoffPolicy(strategy=strategy, base_delay=0.0)
            assert mgr.calculate_delay(policy, 1) == 0.0
            assert mgr.calculate_delay(policy, 10) == 0.0

    def test_very_large_attempt_number(self) -> None:
        """Attempt 1000 still respects max_delay cap."""
        mgr = RetryManager()
        policy = BackoffPolicy(
            strategy="exponential",
            base_delay=1.0,
            multiplier=2.0,
            max_delay=30.0,
        )
        delay = mgr.calculate_delay(policy, 1000)
        assert delay == 30.0

    def test_negative_delay_impossible(self) -> None:
        """Delay is never negative regardless of input."""
        mgr = RetryManager()
        for strategy in ("constant", "linear", "exponential"):
            policy = BackoffPolicy(strategy=strategy, base_delay=1.0, max_delay=60.0)
            for attempt in range(1, 20):
                assert mgr.calculate_delay(policy, attempt) >= 0.0

    def test_multiplier_less_than_one(self) -> None:
        """Multiplier < 1 produces decaying backoff."""
        mgr = RetryManager()
        policy = BackoffPolicy(
            strategy="exponential",
            base_delay=10.0,
            multiplier=0.5,
            max_delay=100.0,
        )
        d1 = mgr.calculate_delay(policy, 1)
        d2 = mgr.calculate_delay(policy, 2)
        d3 = mgr.calculate_delay(policy, 3)
        assert d1 == 10.0
        assert d2 == 5.0
        assert d3 == 2.5
