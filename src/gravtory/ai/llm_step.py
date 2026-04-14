# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@llm_step decorator — first-class LLM step support.

Wraps a workflow step that calls an LLM API, adding:
  - Automatic token counting (input + output)
  - Cost estimation and optional cost-limit enforcement
  - Rate-limit retry (HTTP 429 with exponential back-off)
  - Model fallback on failure
  - Response caching (same prompt → cached response)
"""

from __future__ import annotations

import functools
import hashlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from gravtory.ai.tokens import LLMUsage, TokenCounter, UsageTracker
from gravtory.core.errors import ConfigurationError, GravtoryError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger("gravtory.ai.llm_step")

# Module-level singleton instances
_token_counter = TokenCounter()
_usage_tracker = UsageTracker()


def get_usage_tracker() -> UsageTracker:
    """Return the module-level :class:`UsageTracker`."""
    return _usage_tracker


# ── LLM config dataclass ─────────────────────────────────────────


@dataclass
class LLMConfig:
    """Configuration attached to an ``@llm_step``-decorated function."""

    model: str = "gpt-4"
    fallback_models: list[str] = field(default_factory=list)
    max_tokens: int | None = None
    temperature: float = 0.7
    cache: bool = False
    cost_limit: float | None = None


# ── Cost limit error ──────────────────────────────────────────────


class CostLimitExceededError(GravtoryError):
    """Raised when an LLM call would exceed its cost limit."""

    def __init__(self, model: str, estimated: float, limit: float) -> None:
        self.model = model
        self.estimated = estimated
        self.limit = limit
        super().__init__(
            f"Estimated cost ${estimated:.6f} for model '{model}' exceeds limit ${limit:.6f}",
        )


# ── Response cache ────────────────────────────────────────────────


class _ResponseCache:
    """Simple in-memory prompt→response cache."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    def key(self, model: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        raw = f"{model}:{args!r}:{kwargs!r}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, cache_key: str) -> Any | None:
        return self._store.get(cache_key)

    def set(self, cache_key: str, value: Any) -> None:
        self._store[cache_key] = value


_cache = _ResponseCache()


# ── Decorator ─────────────────────────────────────────────────────


def llm_step(
    model: str = "gpt-4",
    *,
    fallback_models: list[str] | None = None,
    max_tokens: int | None = None,
    temperature: float = 0.7,
    cache: bool = False,
    cost_limit: float | None = None,
) -> Callable[..., Any]:
    """Decorator for LLM-powered workflow steps.

    Must be applied **outside** ``@step`` (i.e. ``@llm_step`` on top)::

        @llm_step(model="gpt-4", fallback_models=["gpt-3.5-turbo"])
        @step(2, depends_on=1)
        async def analyze(self, data: dict) -> str:
            ...

    The decorator:
      1. Sets the step's retry policy to handle 429 (rate limit).
      2. Wraps the function to try fallback models on failure.
      3. Counts tokens before/after the call.
      4. Records cost via the module-level :class:`UsageTracker`.
      5. Optionally caches responses (``cache=True``).
      6. Enforces ``cost_limit`` before executing.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        step_def = getattr(func, "__gravtory_step__", None)
        if step_def is None:
            raise ConfigurationError(
                "@llm_step must be applied outside @step "
                "(place @step directly above the function, "
                "then @llm_step above @step)",
            )

        # Set LLM-friendly retry defaults if the user didn't configure them
        if step_def.retries == 0:
            step_def.retries = 5
        if step_def.backoff is None:
            step_def.backoff = "exponential"
            step_def.backoff_base = 2.0
            step_def.jitter = True

        # Attach LLM config
        llm_cfg = LLMConfig(
            model=model,
            fallback_models=fallback_models or [],
            max_tokens=max_tokens,
            temperature=temperature,
            cache=cache,
            cost_limit=cost_limit,
        )
        func.__gravtory_llm__ = llm_cfg  # type: ignore[attr-defined]

        # Build the model-fallback wrapper
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            cfg: LLMConfig = func.__gravtory_llm__  # type: ignore[attr-defined]
            models_to_try = [cfg.model, *cfg.fallback_models]

            for idx, current_model in enumerate(models_to_try):
                try:
                    # Inject model name into kwargs
                    kwargs["_llm_model"] = current_model

                    # Pre-call: cost-limit check
                    if cfg.cost_limit is not None:
                        input_tokens = _token_counter.count(
                            str(args) + str(kwargs),
                            current_model,
                        )
                        est = _token_counter.estimate_cost(
                            input_tokens,
                            0,
                            current_model,
                        )
                        if est > cfg.cost_limit:
                            raise CostLimitExceededError(
                                current_model,
                                est,
                                cfg.cost_limit,
                            )

                    # Cache lookup
                    cache_key = ""
                    if cfg.cache:
                        cache_key = _cache.key(current_model, args, kwargs)
                        cached = _cache.get(cache_key)
                        if cached is not None:
                            return cached

                    # Count input tokens
                    input_tokens = _token_counter.count(
                        str(args) + str(kwargs),
                        current_model,
                    )

                    # Execute the actual function
                    result = await func(*args, **kwargs)

                    # Count output tokens
                    output_tokens = _token_counter.count(
                        str(result),
                        current_model,
                    )

                    # Record usage
                    cost = _token_counter.estimate_cost(
                        input_tokens,
                        output_tokens,
                        current_model,
                    )
                    await _usage_tracker.record(
                        LLMUsage(
                            model=current_model,
                            input_tokens=input_tokens,
                            output_tokens=output_tokens,
                            cost_usd=cost,
                            step_name=step_def.name,
                        ),
                    )

                    # Cache store
                    if cfg.cache and cache_key:
                        _cache.set(cache_key, result)

                    return result

                except CostLimitExceededError:
                    # Cost limit exceeded for this model — try a cheaper
                    # fallback if one exists, otherwise re-raise.
                    if idx < len(models_to_try) - 1:
                        logger.warning(
                            "Model '%s' exceeds cost limit, trying fallback '%s'",
                            current_model,
                            models_to_try[idx + 1],
                        )
                        continue
                    raise
                except Exception as exc:
                    if idx < len(models_to_try) - 1:
                        logger.warning(
                            "LLM call failed with model '%s', trying fallback '%s': %s",
                            current_model,
                            models_to_try[idx + 1],
                            exc,
                        )
                        continue
                    raise

            # Unreachable — the loop always raises on last failure
            msg = "No models available"  # pragma: no cover
            raise RuntimeError(msg)  # pragma: no cover

        # Replace the step's function with the wrapper
        step_def.function = wrapper
        return func

    return decorator
