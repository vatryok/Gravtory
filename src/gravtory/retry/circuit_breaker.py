# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Circuit breaker — DB-backed, shared across workers.

States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED

CLOSED: calls pass through normally.
OPEN: calls rejected with CircuitOpenError.
HALF_OPEN: limited calls allowed to probe recovery.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gravtory.backends.base import Backend

from gravtory.core.errors import CircuitOpenError


class _CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerState:
    """In-memory representation of circuit breaker state.

    For the DB-backed variant, this is read/written via the backend.
    For unit-testable standalone usage, this is kept in memory.
    The ``version`` field enables optimistic concurrency control.
    """

    name: str = ""
    state: str = field(default=_CircuitState.CLOSED)
    failure_count: int = 0
    last_failure_at: float | None = None
    last_success_at: float | None = None
    opened_at: float | None = None
    version: int = 0


class CircuitBreaker:
    """Circuit breaker that prevents hammering a failing service.

    This is an in-memory implementation suitable for single-process use
    and unit testing. The DB-backed variant (for multi-worker sharing)
    will extend this with backend read/write in a future section.

    Args:
        name: Identifier for this circuit breaker.
        failure_threshold: Number of failures before opening.
        recovery_timeout: Seconds to wait before transitioning OPEN → HALF_OPEN.
        half_open_max: Number of probe calls allowed in HALF_OPEN state.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max: int = 1,
    ) -> None:
        self.name = name
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max = half_open_max
        self._state = CircuitBreakerState(name=name)
        self._half_open_calls = 0

    @property
    def state(self) -> str:
        """Current circuit state."""
        return self._state.state

    @property
    def failure_count(self) -> int:
        """Current failure count."""
        return self._state.failure_count

    async def call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute *func* through the circuit breaker.

        Algorithm:
          1. If OPEN and recovery_timeout elapsed → transition to HALF_OPEN.
          2. If OPEN and timeout not elapsed → raise CircuitOpenError.
          3. Try func(*args, **kwargs).
          4. On success: if HALF_OPEN → transition to CLOSED.
          5. On failure: increment failure_count; if >= threshold → OPEN.

        Raises:
            CircuitOpenError: If circuit is OPEN.
        """
        self._maybe_transition_to_half_open()

        if self._state.state == _CircuitState.OPEN:
            raise CircuitOpenError(self.name)

        if self._state.state == _CircuitState.HALF_OPEN:
            if self._half_open_calls >= self._half_open_max:
                raise CircuitOpenError(self.name)
            self._half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise

        self._record_success()
        return result

    async def reset(self) -> None:
        """Manually reset the circuit to CLOSED state."""
        self._state.state = _CircuitState.CLOSED
        self._state.failure_count = 0
        self._state.opened_at = None
        self._half_open_calls = 0

    def _maybe_transition_to_half_open(self) -> None:
        """If OPEN and recovery timeout has elapsed, move to HALF_OPEN."""
        if self._state.state != _CircuitState.OPEN:
            return
        if self._state.opened_at is None:
            return
        elapsed = time.monotonic() - self._state.opened_at
        if elapsed >= self._recovery_timeout:
            self._state.state = _CircuitState.HALF_OPEN
            self._half_open_calls = 0

    def _record_failure(self) -> None:
        """Record a failure. Open the circuit if threshold exceeded."""
        self._state.failure_count += 1
        self._state.last_failure_at = time.monotonic()

        if self._state.state == _CircuitState.HALF_OPEN:
            # Probe failed → back to OPEN
            self._state.state = _CircuitState.OPEN
            self._state.opened_at = time.monotonic()
            self._half_open_calls = 0
        elif self._state.failure_count >= self._threshold:
            self._state.state = _CircuitState.OPEN
            self._state.opened_at = time.monotonic()

    def _record_success(self) -> None:
        """Record a success. Close the circuit if in HALF_OPEN."""
        self._state.last_success_at = time.monotonic()

        if self._state.state == _CircuitState.HALF_OPEN:
            self._state.state = _CircuitState.CLOSED
            self._state.failure_count = 0
            self._state.opened_at = None
            self._half_open_calls = 0


class DBCircuitBreaker(CircuitBreaker):
    """DB-backed circuit breaker — shares state across workers.

    Extends the in-memory :class:`CircuitBreaker` by reading state from
    the backend before each call and writing it back after each state
    transition. This ensures that multiple workers see the same circuit
    state and prevents them from independently hammering a failing service.

    Args:
        name: Identifier for this circuit breaker.
        backend: A :class:`Backend` instance with ``save_circuit_state``
            and ``load_circuit_state`` support.
        failure_threshold: Number of failures before opening.
        recovery_timeout: Seconds to wait before transitioning OPEN -> HALF_OPEN.
        half_open_max: Number of probe calls allowed in HALF_OPEN state.
    """

    def __init__(
        self,
        name: str,
        backend: Backend,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max: int = 1,
    ) -> None:
        super().__init__(
            name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max=half_open_max,
        )
        self._backend = backend

    async def _load_state(self) -> None:
        """Load circuit breaker state from the backend."""
        raw = await self._backend.load_circuit_state(self.name)
        if raw is not None:
            data = json.loads(raw)
            self._state = CircuitBreakerState(
                name=data.get("name", self.name),
                state=data.get("state", _CircuitState.CLOSED),
                failure_count=data.get("failure_count", 0),
                last_failure_at=data.get("last_failure_at"),
                last_success_at=data.get("last_success_at"),
                opened_at=data.get("opened_at"),
                version=data.get("version", 0),
            )

    async def _save_state(self, expected_version: int | None = None) -> bool:
        """Persist circuit breaker state with optimistic locking.

        When *expected_version* is provided, re-loads the state first and
        returns False if the stored version has advanced (another worker
        wrote in between). This detects — but cannot fully prevent — the
        TOCTOU race under concurrent workers.

        .. note::

            True atomicity would require backend-level atomic increment
            (e.g. ``UPDATE SET failure_count = failure_count + 1 WHERE
            name = ? AND version = ?``). The current implementation
            provides *best-effort* optimistic concurrency: under high
            contention the failure_count may be slightly behind, causing
            the circuit breaker to open a few calls later than configured.
        """
        if expected_version is not None:
            current = await self._backend.load_circuit_state(self.name)
            if current is not None:
                stored = json.loads(current)
                if stored.get("version", 0) != expected_version:
                    return False
        self._state.version += 1
        data = asdict(self._state)
        await self._backend.save_circuit_state(self.name, json.dumps(data))
        return True

    _MAX_OPTIMISTIC_RETRIES = 3

    async def _load_mutate_save(self, mutate_fn: Any) -> None:
        """Load state, apply a mutation, save with optimistic retry.

        If another worker modifies the state between our load and save,
        the version will have advanced and we retry the full cycle.
        """
        for _attempt in range(self._MAX_OPTIMISTIC_RETRIES):
            await self._load_state()
            pre_version = self._state.version
            mutate_fn()
            if await self._save_state(expected_version=pre_version):
                return
        # Last attempt without version guard — best effort
        await self._load_state()
        mutate_fn()
        await self._save_state()

    async def call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute *func* through the DB-backed circuit breaker.

        Uses optimistic concurrency: load→evaluate→save with version checks
        to prevent TOCTOU races across workers.
        """
        # ── Pre-call: load, evaluate, persist any transition ──
        await self._load_state()
        prev_state = self._state.state
        self._maybe_transition_to_half_open()

        if self._state.state == _CircuitState.OPEN:
            if prev_state != self._state.state:
                await self._save_state()
            raise CircuitOpenError(self.name)

        if self._state.state == _CircuitState.HALF_OPEN:
            if self._half_open_calls >= self._half_open_max:
                raise CircuitOpenError(self.name)
            self._half_open_calls += 1

        if prev_state != self._state.state:
            await self._save_state()

        # ── Execute ──
        try:
            result = await func(*args, **kwargs)
        except Exception:
            await self._load_mutate_save(self._record_failure)
            raise

        # ── Post-call: atomic load→record→save ──
        await self._load_mutate_save(self._record_success)
        return result

    async def reset(self) -> None:
        """Manually reset the circuit to CLOSED state and persist."""
        await super().reset()
        await self._save_state()
