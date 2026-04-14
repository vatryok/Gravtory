# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""DB-backed token bucket rate limiter for step execution throttling."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gravtory.backends.base import Backend


@dataclass
class _TokenBucket:
    """In-memory state for a single rate limit bucket."""

    name: str
    tokens: float
    max_tokens: float
    refill_rate: float  # tokens per second
    last_refill_at: float = field(default_factory=time.monotonic)


class RateLimiter:
    """Token bucket rate limiter.

    For single-process use, state is held in memory (fast).
    For multi-worker scenarios, a DB-backed implementation can be
    plugged in via the backend's ``rate_limit_acquire`` method.

    Token bucket algorithm:
      - Bucket starts full at *max_tokens*.
      - Each ``acquire()`` consumes *tokens* from the bucket.
      - Tokens refill at *refill_rate* per second, up to *max_tokens*.
      - If not enough tokens, returns the wait time needed.
    """

    def __init__(
        self,
        name: str,
        *,
        max_tokens: float = 10.0,
        refill_rate: float = 1.0,
        backend: Backend | None = None,
    ) -> None:
        self._name = name
        self._max_tokens = max_tokens
        self._refill_rate = refill_rate
        self._backend = backend
        # In-memory bucket (used when no DB-backed method is available)
        self._bucket = _TokenBucket(
            name=name,
            tokens=max_tokens,
            max_tokens=max_tokens,
            refill_rate=refill_rate,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def available_tokens(self) -> float:
        """Current tokens after refill (in-memory only)."""
        self._refill()
        return self._bucket.tokens

    async def acquire(self, tokens: float = 1.0) -> float:
        """Acquire tokens. Returns wait time (0 if acquired immediately).

        If a DB-backed backend with ``rate_limit_acquire`` is available,
        delegates to it for cross-worker atomicity. Otherwise uses the
        in-memory bucket.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            Seconds to wait before retrying (0.0 = success, acquired now).
        """
        # Try DB-backed atomic acquire
        if self._backend is not None:
            fn = getattr(self._backend, "rate_limit_acquire", None)
            if fn is not None:
                wait: float = await fn(self._name, tokens, self._max_tokens, self._refill_rate)
                return wait

        # In-memory acquire
        return self._acquire_local(tokens)

    def _refill(self) -> None:
        """Add tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._bucket.last_refill_at
        if elapsed > 0:
            added = elapsed * self._bucket.refill_rate
            self._bucket.tokens = min(self._bucket.tokens + added, self._bucket.max_tokens)
            self._bucket.last_refill_at = now

    def _acquire_local(self, tokens: float) -> float:
        """Acquire tokens from the in-memory bucket.

        Returns 0.0 on success, or the number of seconds to wait.
        """
        self._refill()

        if self._bucket.tokens >= tokens:
            self._bucket.tokens -= tokens
            return 0.0

        # Not enough tokens — calculate wait time
        deficit = tokens - self._bucket.tokens
        if self._bucket.refill_rate <= 0:
            return float("inf")
        wait_time = deficit / self._bucket.refill_rate
        return wait_time
