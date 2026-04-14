# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Token counting and LLM usage tracking.

Provides :class:`TokenCounter` for counting tokens (with tiktoken
fallback), :class:`LLMUsage` for recording per-call usage, and
:class:`UsageTracker` for aggregating costs across workflows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger("gravtory.ai.tokens")

# ── Model cost table (approximate $/1K tokens, input) ────────────
MODEL_COSTS: dict[str, float] = {
    "gpt-4": 0.03,
    "gpt-4-turbo": 0.01,
    "gpt-4o": 0.005,
    "gpt-4o-mini": 0.00015,
    "gpt-3.5-turbo": 0.0005,
    "claude-3-opus": 0.015,
    "claude-3-sonnet": 0.003,
    "claude-3-haiku": 0.00025,
    "claude-3.5-sonnet": 0.003,
}

# ── Quality ranking (higher = better) ────────────────────────────
MODEL_QUALITY: dict[str, int] = {
    "gpt-4": 90,
    "gpt-4-turbo": 88,
    "gpt-4o": 92,
    "gpt-4o-mini": 75,
    "gpt-3.5-turbo": 60,
    "claude-3-opus": 95,
    "claude-3-sonnet": 80,
    "claude-3-haiku": 55,
    "claude-3.5-sonnet": 85,
}


# ── Token counter ─────────────────────────────────────────────────


class TokenCounter:
    """Count tokens for LLM inputs/outputs.

    Uses ``tiktoken`` (OpenAI's tokeniser) when available.
    Falls back to a character-based estimation when not installed.
    """

    def __init__(self) -> None:
        self._tiktoken: Any = None
        self._available: bool = False
        try:
            import tiktoken

            self._tiktoken = tiktoken
            self._available = True
        except ImportError:
            logger.debug("tiktoken not installed — using estimation fallback")

    @property
    def tiktoken_available(self) -> bool:
        """Whether tiktoken is installed."""
        return self._available

    def count(self, text: str, model: str = "gpt-4") -> int:
        """Count tokens in *text* for the given model.

        Returns an accurate count when tiktoken is installed, otherwise
        a rough estimate (1 token ≈ 4 characters).
        """
        if not text:
            return 0
        if self._available:
            try:
                enc = self._tiktoken.encoding_for_model(model)
                return len(enc.encode(text))
            except KeyError:
                pass
        # Fallback: ≈ 4 chars per token
        return max(1, len(text) // 4)

    def estimate_cost(
        self,
        input_tokens: int,
        output_tokens: int,
        model: str,
    ) -> float:
        """Estimate cost in USD for a single LLM call.

        Uses ``MODEL_COSTS`` table.  Unknown models default to 0.0.
        Output tokens are typically more expensive; we apply a 2x factor.
        """
        cost_per_1k = MODEL_COSTS.get(model, 0.0)
        input_cost = (input_tokens / 1000) * cost_per_1k
        output_cost = (output_tokens / 1000) * cost_per_1k * 2
        return round(input_cost + output_cost, 8)


# ── Usage record ──────────────────────────────────────────────────


@dataclass
class LLMUsage:
    """A single LLM usage record."""

    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    step_name: str = ""
    workflow_run_id: str = ""
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )


# ── Usage tracker ─────────────────────────────────────────────────


class UsageTracker:
    """Track LLM token usage and costs across all workflows.

    Stores records in memory.  For production, records should be
    persisted via a backend or external store.
    """

    def __init__(self) -> None:
        self._records: list[LLMUsage] = []

    async def record(self, usage: LLMUsage) -> None:
        """Store a usage record."""
        self._records.append(usage)

    async def get_workflow_usage(self, run_id: str) -> dict[str, Any]:
        """Get total token usage and cost for a workflow run.

        Returns::

            {
                "total_input_tokens": int,
                "total_output_tokens": int,
                "total_cost_usd": float,
                "calls": int,
                "by_step": {step_name: {"tokens": int, "cost": float}},
                "by_model": {model: {"tokens": int, "cost": float}},
            }
        """
        matched = [r for r in self._records if r.workflow_run_id == run_id]
        by_step: dict[str, dict[str, float]] = {}
        by_model: dict[str, dict[str, float]] = {}
        total_in = 0
        total_out = 0
        total_cost = 0.0

        for r in matched:
            total_in += r.input_tokens
            total_out += r.output_tokens
            total_cost += r.cost_usd

            step = by_step.setdefault(r.step_name, {"tokens": 0.0, "cost": 0.0})
            step["tokens"] += r.input_tokens + r.output_tokens
            step["cost"] += r.cost_usd

            mdl = by_model.setdefault(r.model, {"tokens": 0.0, "cost": 0.0})
            mdl["tokens"] += r.input_tokens + r.output_tokens
            mdl["cost"] += r.cost_usd

        return {
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cost_usd": round(total_cost, 8),
            "calls": len(matched),
            "by_step": by_step,
            "by_model": by_model,
        }

    async def get_daily_usage(self, day: date) -> dict[str, Any]:
        """Get aggregate usage for a specific calendar day (UTC)."""
        matched = [r for r in self._records if r.timestamp.date() == day]
        total_in = sum(r.input_tokens for r in matched)
        total_out = sum(r.output_tokens for r in matched)
        total_cost = sum(r.cost_usd for r in matched)
        return {
            "date": day.isoformat(),
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cost_usd": round(total_cost, 8),
            "calls": len(matched),
        }

    async def get_cost_report(
        self,
        since: datetime,
        until: datetime,
    ) -> dict[str, Any]:
        """Generate a cost report for a time range.

        Returns aggregate stats and per-model breakdown.
        """
        matched = [r for r in self._records if since <= r.timestamp <= until]
        by_model: dict[str, dict[str, float]] = {}
        total_cost = 0.0
        total_tokens = 0

        for r in matched:
            total_cost += r.cost_usd
            total_tokens += r.input_tokens + r.output_tokens
            mdl = by_model.setdefault(r.model, {"tokens": 0.0, "cost": 0.0})
            mdl["tokens"] += r.input_tokens + r.output_tokens
            mdl["cost"] += r.cost_usd

        return {
            "since": since.isoformat(),
            "until": until.isoformat(),
            "total_tokens": total_tokens,
            "total_cost_usd": round(total_cost, 8),
            "calls": len(matched),
            "by_model": by_model,
        }
