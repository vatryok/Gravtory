# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Retry policies — BackoffPolicy, RetryPolicy, and RetryManager."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gravtory.core.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable

_VALID_STRATEGIES = {"constant", "linear", "exponential"}


@dataclass
class BackoffPolicy:
    """Configures how retry delays are calculated.

    Attributes:
        strategy: One of "constant", "linear", "exponential", or a
            callable(attempt: int) -> float.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay cap in seconds.
        multiplier: Multiplier for exponential backoff.
        jitter: If True, apply full jitter (random.uniform(0, delay)).
    """

    strategy: str | Callable[[int], float] = "exponential"
    base_delay: float = 1.0
    max_delay: float = 300.0
    multiplier: float = 2.0
    jitter: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.strategy, str) and self.strategy not in _VALID_STRATEGIES:
            raise ConfigurationError(
                f"Invalid backoff strategy: {self.strategy!r}. "
                f"Must be one of {_VALID_STRATEGIES} or a callable."
            )


@dataclass
class RetryPolicy:
    """User-facing convenience wrapper around BackoffPolicy.

    Attributes:
        retries: Maximum number of retry attempts.
        backoff: Strategy name or a full BackoffPolicy instance.
        backoff_base: Base delay in seconds.
        backoff_max: Maximum delay cap in seconds.
        jitter: Whether to apply jitter.
        retry_on: Exception types that should trigger a retry.
        abort_on: Exception types that should abort immediately (no retry).
    """

    retries: int = 3
    backoff: str | BackoffPolicy = "exponential"
    backoff_base: float = 1.0
    backoff_max: float = 300.0
    multiplier: float = 2.0
    jitter: bool = True
    retry_on: list[type[Exception]] = field(default_factory=list)
    abort_on: list[type[Exception]] = field(default_factory=list)

    def to_backoff_policy(self) -> BackoffPolicy:
        """Convert to a BackoffPolicy instance."""
        if isinstance(self.backoff, BackoffPolicy):
            return self.backoff
        return BackoffPolicy(
            strategy=self.backoff,
            base_delay=self.backoff_base,
            max_delay=self.backoff_max,
            multiplier=self.multiplier,
            jitter=self.jitter,
        )


class RetryManager:
    """Calculates retry delays based on backoff policies."""

    def calculate_delay(self, policy: BackoffPolicy, attempt: int) -> float:
        """Calculate delay for the given retry attempt.

        Args:
            policy: The backoff policy to use.
            attempt: The retry attempt number (1-indexed).

        Returns:
            Delay in seconds.
        """
        if isinstance(policy.strategy, str):
            if policy.strategy == "constant":
                base = policy.base_delay
            elif policy.strategy == "linear":
                base = policy.base_delay * attempt
            elif policy.strategy == "exponential":
                base = policy.base_delay * (policy.multiplier ** (attempt - 1))
            else:
                raise ConfigurationError(f"Unknown strategy: {policy.strategy!r}")
        elif callable(policy.strategy):
            base = policy.strategy(attempt)
        else:
            raise ConfigurationError(f"Unknown strategy: {policy.strategy!r}")

        delay = min(base, policy.max_delay)

        if policy.jitter:
            delay = random.uniform(0, delay)

        return delay
