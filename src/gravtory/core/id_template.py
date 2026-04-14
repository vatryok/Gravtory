# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Workflow ID templating — generate run IDs from templates and kwargs."""

from __future__ import annotations

from typing import Any

from gravtory.core.errors import ConfigurationError


def generate_workflow_id(template: str, **kwargs: Any) -> str:
    """Generate a workflow run ID from a template.

    Template format: "order-{order_id}"
    kwargs: {"order_id": "abc123"}
    Result: "order-abc123"

    Rules:
      1. All template variables MUST be provided in kwargs
      2. Values are converted to strings
      3. Special characters are preserved (no sanitization)
      4. Empty values are allowed
      5. Missing variables raise ConfigurationError
    """
    try:
        return template.format(**{k: str(v) for k, v in kwargs.items()})
    except KeyError as e:
        raise ConfigurationError(
            f"Workflow ID template '{template}' requires parameter {e} "
            f"which was not provided. Available: {list(kwargs.keys())}"
        ) from e
