# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Versioned prompt templates for LLM steps.

Provides :class:`PromptTemplate` — a version-controlled, renderable
prompt that produces OpenAI-compatible message lists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PromptTemplate:
    """A versioned prompt template.

    Usage::

        ANALYZE = PromptTemplate(
            name="analyze",
            template="Analyze the following data:\\n\\n{data}",
            version=2,
            model="gpt-4",
            max_tokens=1000,
            system_prompt="You are a data analyst.",
        )

        rendered = ANALYZE.render(data="revenue numbers …")
        messages = ANALYZE.to_messages(data="revenue numbers …")
    """

    name: str
    template: str
    version: int = 1
    model: str | None = None
    max_tokens: int | None = None
    system_prompt: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def render(self, **kwargs: str) -> str:
        """Render the template with the given variables.

        Raises :class:`KeyError` if a required variable is missing.
        """
        return self.template.format(**kwargs)

    def to_messages(self, **kwargs: str) -> list[dict[str, str]]:
        """Render as an OpenAI-compatible ``messages`` list.

        Returns a list of ``{"role": …, "content": …}`` dicts.
        If :attr:`system_prompt` is set it is prepended as a system message.
        """
        messages: list[dict[str, str]] = []
        if self.system_prompt is not None:
            messages.append({"role": "system", "content": self.system_prompt})
        messages.append({"role": "user", "content": self.render(**kwargs)})
        return messages
