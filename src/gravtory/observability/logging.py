# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Structured logging configuration.

Provides :func:`configure_logging` to set up structured JSON or console
logging via ``structlog``.  Falls back to stdlib :mod:`logging` with basic
formatting when ``structlog`` is not installed.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

logger = logging.getLogger("gravtory.observability.logging")

# ---------------------------------------------------------------------------
# Optional structlog import
# ---------------------------------------------------------------------------
try:
    import structlog as _structlog_mod

    _HAS_STRUCTLOG = True
except ImportError:  # pragma: no cover
    _HAS_STRUCTLOG = False
    _structlog_mod = None  # type: ignore[assignment]  # pragma: no cover


def configure_logging(
    level: str = "INFO",
    fmt: str = "json",
    add_timestamp: bool = True,
) -> None:
    """Configure Gravtory logging.

    Args:
        level: Log level name (``DEBUG``, ``INFO``, ``WARNING``, etc.).
        fmt: Output format — ``"json"`` for production or ``"console"``
            for human-readable development output.
        add_timestamp: Whether to include ISO-8601 timestamps.

    When ``structlog`` is installed, configures it with appropriate
    processors.  Otherwise sets up stdlib ``logging`` with a basic
    formatter.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    if _HAS_STRUCTLOG and _structlog_mod is not None:
        _configure_structlog(numeric_level, fmt, add_timestamp)
    else:
        _configure_stdlib(numeric_level, fmt, add_timestamp)


def get_logger(name: str = "gravtory") -> Any:
    """Return a logger instance.

    Returns a ``structlog`` bound logger when available, otherwise a
    stdlib ``logging.Logger``.
    """
    if _HAS_STRUCTLOG and _structlog_mod is not None:
        return _structlog_mod.get_logger(name)
    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _configure_structlog(
    level: int,
    fmt: str,
    add_timestamp: bool,
) -> None:
    """Set up structlog processors and renderer."""
    assert _structlog_mod is not None  # guarded by caller

    processors: list[Any] = [
        _structlog_mod.contextvars.merge_contextvars,
        _structlog_mod.processors.add_log_level,
    ]

    if add_timestamp:
        processors.append(_structlog_mod.processors.TimeStamper(fmt="iso"))

    processors.extend(
        [
            _structlog_mod.processors.StackInfoRenderer(),
            _structlog_mod.processors.format_exc_info,
        ]
    )

    if fmt == "json":
        processors.append(_structlog_mod.processors.JSONRenderer())
    else:
        processors.append(_structlog_mod.dev.ConsoleRenderer())

    _structlog_mod.configure(
        processors=processors,
        wrapper_class=_structlog_mod.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=_structlog_mod.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def _configure_stdlib(
    level: int,
    fmt: str,
    add_timestamp: bool,
) -> None:
    """Fallback: configure stdlib logging."""
    if fmt == "json":
        if add_timestamp:
            log_fmt = (
                '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
                '"logger":"%(name)s","event":"%(message)s"}'
            )
        else:
            log_fmt = '{"level":"%(levelname)s","logger":"%(name)s","event":"%(message)s"}'
    else:
        if add_timestamp:
            log_fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        else:
            log_fmt = "[%(levelname)s] %(name)s: %(message)s"

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(log_fmt))

    root = logging.getLogger("gravtory")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
