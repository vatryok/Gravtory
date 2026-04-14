# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@schedule decorator — attaches a schedule to a workflow class or proxy."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from gravtory.core.errors import ConfigurationError
from gravtory.core.types import Schedule, ScheduleType
from gravtory.scheduling.interval import parse_interval

if TYPE_CHECKING:
    from collections.abc import Callable


def schedule(
    cron: str | None = None,
    interval: timedelta | float | None = None,
    every: str | None = None,
    on_event: str | None = None,
    after: str | None = None,
    at: datetime | None = None,
    enabled: bool = True,
) -> Callable[..., Any]:
    """Decorator that attaches a schedule to a workflow.

    Exactly one scheduling parameter must be provided:
      - ``cron``: Cron expression (5 or 6 fields)
      - ``interval``: Fixed interval as timedelta or float seconds
      - ``every``: Human-readable interval string ("30s", "5m", "2h", "1d")
      - ``on_event``: Custom event name that triggers the workflow
      - ``after``: Workflow name — triggers when that workflow completes
      - ``at``: One-time execution at a specific datetime

    Usage::

        @schedule(cron="0 9 * * 1-5")
        @workflow(id="daily-report-{date}")
        class DailyReport: ...

    At ``grav.start()`` time the schedule metadata is persisted to the DB
    and the scheduler loop picks it up.
    """

    def decorator(cls_or_proxy: Any) -> Any:
        stype, config = _resolve_schedule_params(
            cron=cron,
            interval=interval,
            every=every,
            on_event=on_event,
            after=after,
            at=at,
        )

        # Determine the workflow name
        from gravtory.decorators.workflow import WorkflowProxy

        if isinstance(cls_or_proxy, WorkflowProxy):
            wf_name = cls_or_proxy.definition.name
            ns = cls_or_proxy.definition.config.namespace
            sched = Schedule(
                id=f"sched-{wf_name}",
                workflow_name=wf_name,
                schedule_type=stype,
                schedule_config=config,
                namespace=ns,
                enabled=enabled,
            )
            cls_or_proxy._schedule = sched  # type: ignore[attr-defined]
        else:
            # Class not yet wrapped by @workflow — store metadata on the class
            cls_or_proxy.__gravtory_schedule__ = {
                "type": stype,
                "config": config,
                "enabled": enabled,
            }

        return cls_or_proxy

    return decorator


def _resolve_schedule_params(
    *,
    cron: str | None,
    interval: timedelta | float | None,
    every: str | None,
    on_event: str | None,
    after: str | None,
    at: datetime | None,
) -> tuple[ScheduleType, str]:
    """Resolve decorator parameters into (ScheduleType, config_string).

    Raises ConfigurationError if zero or multiple params are given.
    """
    provided = sum(x is not None for x in (cron, interval, every, on_event, after, at))
    if provided == 0:
        raise ConfigurationError(
            "@schedule requires one of: cron, interval, every, on_event, after, at"
        )
    if provided > 1:
        raise ConfigurationError("@schedule accepts only one scheduling parameter at a time")

    if cron is not None:
        return ScheduleType.CRON, cron

    if interval is not None:
        secs = interval.total_seconds() if isinstance(interval, timedelta) else float(interval)
        return ScheduleType.INTERVAL, str(secs)

    if every is not None:
        td = parse_interval(every)
        return ScheduleType.INTERVAL, str(td.total_seconds())

    if on_event is not None:
        return ScheduleType.EVENT, on_event

    if after is not None:
        return ScheduleType.EVENT, f"workflow:{after}"

    if at is not None:
        return ScheduleType.ONE_TIME, at.isoformat()

    # Unreachable, but keeps mypy happy
    raise ConfigurationError("@schedule: no scheduling parameter provided")  # pragma: no cover
