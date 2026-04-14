# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Lazy step output loading — loads from DB on first access, then caches."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.checkpoint import CheckpointEngine


class LazyStepOutput:
    """Proxy that loads step output from DB on first access.

    This avoids loading all step outputs when only some are needed
    (e.g., step 5 only needs step 4's output, not steps 1-3).
    """

    def __init__(
        self,
        run_id: str,
        step_order: int,
        backend: Backend,
        checkpoint_engine: CheckpointEngine,
        output_type: type | None = None,
    ) -> None:
        self._run_id = run_id
        self._step_order = step_order
        self._backend = backend
        self._checkpoint = checkpoint_engine
        self._output_type = output_type
        self._loaded = False
        self._value: Any = None

    async def get(self) -> Any:
        """Load and return the step output.

        First call: loads from DB, deserializes, caches.
        Subsequent calls: return cached value.
        """
        if not self._loaded:
            step_output = await self._backend.get_step_output(self._run_id, self._step_order)
            if step_output is not None and step_output.output_data is not None:
                self._value = self._checkpoint.restore_typed(
                    step_output.output_data, self._output_type
                )
            self._loaded = True
        return self._value

    @property
    def loaded(self) -> bool:
        """Whether the value has been loaded from the DB."""
        return self._loaded
