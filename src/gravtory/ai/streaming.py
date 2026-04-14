# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@stream_step decorator — streaming LLM outputs with checkpointing.

Provides :class:`StreamingCheckpointer` for incremental checkpointing
of streaming responses, and the :func:`stream_step` decorator that
wires it into the Gravtory step lifecycle.
"""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from gravtory.core.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

logger = logging.getLogger("gravtory.ai.streaming")


# ── Streaming checkpointer ───────────────────────────────────────


@dataclass
class _Snapshot:
    """Internal: a single incremental checkpoint."""

    index: int
    content: str


class StreamingCheckpointer:
    """Manages incremental checkpointing of a streaming LLM response.

    Accumulates text chunks and writes a checkpoint every
    *interval* estimated tokens.

    Usage::

        cp = StreamingCheckpointer(interval=100)
        async for chunk in llm_stream:
            snapshot = cp.feed(chunk)
            if snapshot is not None:
                # persist snapshot.content somewhere
                ...
        final = cp.finalize()
    """

    def __init__(self, *, interval: int = 100) -> None:
        self._interval = max(1, interval)
        self._buffer: str = ""
        self._token_count: int = 0
        self._checkpoint_count: int = 0
        self._snapshots: list[_Snapshot] = []

    @property
    def buffer(self) -> str:
        """Current accumulated text."""
        return self._buffer

    @property
    def token_count(self) -> int:
        """Rough token count so far."""
        return self._token_count

    @property
    def snapshots(self) -> list[_Snapshot]:
        """All snapshots taken so far."""
        return list(self._snapshots)

    def feed(self, chunk: str) -> _Snapshot | None:
        """Feed a streaming chunk.

        Returns a :class:`_Snapshot` when an interval boundary is
        crossed, otherwise ``None``.
        """
        if not chunk:
            return None
        self._buffer += chunk
        # Rough token estimate: 1 token ≈ 4 characters
        self._token_count = max(1, len(self._buffer) // 4)

        threshold = (self._checkpoint_count + 1) * self._interval
        if self._token_count >= threshold:
            return self._take_snapshot()
        return None

    def _take_snapshot(self) -> _Snapshot:
        snap = _Snapshot(index=self._checkpoint_count, content=self._buffer)
        self._snapshots.append(snap)
        self._checkpoint_count += 1
        return snap

    def get_resume_point(self) -> str | None:
        """Return the content of the latest snapshot, or ``None``."""
        if not self._snapshots:
            return None
        return self._snapshots[-1].content

    def finalize(self) -> str:
        """Take a final snapshot and return the complete output."""
        if not self._snapshots or self._snapshots[-1].content != self._buffer:
            self._take_snapshot()
        return self._buffer


# ── Decorator ─────────────────────────────────────────────────────


def stream_step(
    checkpoint_interval: int = 100,
) -> Callable[..., Any]:
    """Decorator for steps that produce streaming output.

    Must be applied **outside** ``@step`` (i.e. ``@stream_step`` on top)::

        @stream_step(checkpoint_interval=50)
        @step(2, depends_on=1)
        async def generate(self, prompt: str) -> AsyncIterator[str]:
            async for chunk in llm_stream(prompt):
                yield chunk

    The decorator:
      1. Collects all yielded chunks into a buffer.
      2. Every *checkpoint_interval* tokens: stores an incremental
         checkpoint via :class:`StreamingCheckpointer`.
      3. On crash: the last checkpoint can be used to skip
         already-generated tokens.
      4. Final output: complete concatenated string.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        step_def = getattr(func, "__gravtory_step__", None)
        if step_def is None:
            raise ConfigurationError(
                "@stream_step must be applied outside @step "
                "(place @step directly above the function, "
                "then @stream_step above @step)",
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> str:
            cp = StreamingCheckpointer(interval=checkpoint_interval)
            gen: AsyncIterator[str] = func(*args, **kwargs)
            async for chunk in gen:
                cp.feed(chunk)
            return cp.finalize()

        step_def.function = wrapper
        return func

    return decorator
