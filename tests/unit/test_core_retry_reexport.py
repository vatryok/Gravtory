"""Tests for core.retry re-export module."""

from __future__ import annotations


class TestCoreRetryReExports:
    def test_all_exports_present(self) -> None:
        from gravtory.core.retry import __all__

        assert "BackoffPolicy" in __all__
        assert "CircuitBreaker" in __all__
        assert "CircuitBreakerState" in __all__
        assert "RetryManager" in __all__
        assert "RetryPolicy" in __all__

    def test_imports_work(self) -> None:
        from gravtory.core.retry import (
            BackoffPolicy,
            CircuitBreaker,
            CircuitBreakerState,
            RetryManager,
            RetryPolicy,
        )

        assert BackoffPolicy is not None
        assert CircuitBreaker is not None
        assert CircuitBreakerState is not None
        assert RetryManager is not None
        assert RetryPolicy is not None

    def test_same_as_retry_package(self) -> None:
        from gravtory.core.retry import RetryPolicy as CoreRP
        from gravtory.retry.policies import RetryPolicy as PoliciesRP

        assert CoreRP is PoliciesRP
