# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@parallel decorator — marks a step for parallel fan-out execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gravtory.core.errors import ConfigurationError
from gravtory.core.types import ParallelConfig

if TYPE_CHECKING:
    from collections.abc import Callable


def parallel(
    max_concurrency: int = 10,
    batch_checkpoint: int | None = None,
) -> Callable[..., Any]:
    """Decorator that marks a step for parallel fan-out execution.

    Usage::

        @step(2, depends_on=1)
        @parallel(max_concurrency=5)
        async def process_item(self, item: dict) -> dict:
            return {"processed": item["id"]}

    How it works:
      1. Step 1 returns a ``list[T]``.
      2. Step 2 (parallel) receives each ``T`` individually.
      3. Step 2 runs for each item concurrently (up to *max_concurrency*).
      4. Each result is checkpointed individually.
      5. Step 3 receives ``list[U]`` (all results).

    What it does:
      Sets ``step_def.parallel_config = ParallelConfig(max_concurrency=...)``.

    Raises:
        ConfigurationError: If applied before ``@step``.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not hasattr(func, "__gravtory_step__"):
            raise ConfigurationError("@parallel must be applied AFTER @step")
        func.__gravtory_step__.parallel_config = ParallelConfig(
            max_concurrency=max_concurrency,
            batch_checkpoint=batch_checkpoint,
        )
        return func

    return decorator
