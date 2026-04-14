# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""AI/ML native features for Gravtory.

Provides first-class support for building durable AI pipelines:
LLM steps, streaming outputs, model routing, token tracking,
prompt versioning, and agent orchestration patterns.
"""

from __future__ import annotations

from gravtory.ai.agents import AgentTool, LLMMapReduce, ReActAgent, ToolCallingLoop
from gravtory.ai.fallback import ModelRoute, ModelRouter
from gravtory.ai.llm_step import LLMConfig, llm_step
from gravtory.ai.prompts import PromptTemplate
from gravtory.ai.streaming import StreamingCheckpointer, stream_step
from gravtory.ai.tokens import LLMUsage, TokenCounter, UsageTracker

__all__ = [
    "AgentTool",
    "LLMConfig",
    "LLMMapReduce",
    "LLMUsage",
    "ModelRoute",
    "ModelRouter",
    "PromptTemplate",
    "ReActAgent",
    "StreamingCheckpointer",
    "TokenCounter",
    "ToolCallingLoop",
    "UsageTracker",
    "llm_step",
    "stream_step",
]
