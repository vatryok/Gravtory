# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@saga decorator — enables saga mode on a workflow class."""

from __future__ import annotations

from typing import Any


def saga(cls_or_func: Any = None) -> Any:
    """Decorator that enables saga mode on a workflow.

    Usage::

        @grav.workflow(id="order-{order_id}")
        @saga
        class OrderWorkflow:
            @step(1, compensate="undo_charge")
            async def charge(self, order_id): ...

            async def undo_charge(self, output: dict):
                ...

    What it does:
      1. Sets ``__gravtory_saga__ = True`` on the class.
      2. The ``@workflow`` decorator reads this flag and sets
         ``config.saga_enabled = True`` on the WorkflowDefinition.
    """

    def decorator(cls: Any) -> Any:
        cls.__gravtory_saga__ = True
        return cls

    if cls_or_func is not None:
        return decorator(cls_or_func)
    return decorator
