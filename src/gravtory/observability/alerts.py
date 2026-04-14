# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Alert system — fire notifications on critical workflow events.

Provides :class:`AlertManager` which dispatches alerts to one or more
:class:`AlertHandler` implementations (webhook, Slack, log).

Alert handler failures are isolated — a broken handler never crashes the
engine or prevents other handlers from receiving the alert.
"""

from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("gravtory.observability.alerts")


class AlertHandler(abc.ABC):
    """Base class for alert delivery handlers."""

    @abc.abstractmethod
    async def send(self, event: str, data: dict[str, Any]) -> None:
        """Deliver an alert for the given event."""
        ...


class LogAlertHandler(AlertHandler):
    """Default handler — logs alerts via stdlib logging."""

    async def send(self, event: str, data: dict[str, Any]) -> None:
        logger.warning("ALERT [%s] %s", event, data)


class WebhookAlertHandler(AlertHandler):
    """Sends alerts as JSON POST requests to a webhook URL.

    Requires ``aiohttp`` (included in ``gravtory[dashboard]`` extra).
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = url
        self._headers = headers or {}
        self._timeout = timeout
        self._session: Any = None

    async def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            import aiohttp

            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the reusable HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def send(self, event: str, data: dict[str, Any]) -> None:
        try:
            import aiohttp  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "aiohttp is required for WebhookAlertHandler — "
                "install with: pip install gravtory[dashboard]"
            ) from exc

        payload = {
            "event": event,
            "data": data,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "source": "gravtory",
        }
        session = await self._get_session()
        async with session.post(self._url, json=payload, headers=self._headers) as resp:
            if resp.status >= 400:
                logger.error(
                    "Webhook alert failed: HTTP %d from %s",
                    resp.status,
                    self._url,
                )


class SlackAlertHandler(AlertHandler):
    """Sends alerts as formatted Slack messages via incoming webhook.

    Requires ``aiohttp``.
    """

    def __init__(
        self,
        webhook_url: str,
        channel: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = webhook_url
        self._channel = channel
        self._timeout = timeout
        self._session: Any = None

    async def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            import aiohttp

            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the reusable HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def send(self, event: str, data: dict[str, Any]) -> None:
        try:
            import aiohttp  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "aiohttp is required for SlackAlertHandler — "
                "install with: pip install gravtory[dashboard]"
            ) from exc

        message = self._format_message(event, data)
        session = await self._get_session()
        async with session.post(self._url, json=message) as resp:
            if resp.status >= 400:
                logger.error(
                    "Slack alert failed: HTTP %d",
                    resp.status,
                )

    def _format_message(self, event: str, data: dict[str, Any]) -> dict[str, Any]:
        color = "#dc3545" if "fail" in event.lower() else "#ffc107"
        text_lines = [f"*{event}*"]
        for key, value in data.items():
            text_lines.append(f"• {key}: `{value}`")

        payload: dict[str, Any] = {
            "attachments": [
                {
                    "color": color,
                    "title": f"Gravtory Alert: {event}",
                    "text": "\n".join(text_lines),
                    "ts": int(datetime.now(tz=timezone.utc).timestamp()),
                }
            ],
        }
        if self._channel:
            payload["channel"] = self._channel
        return payload


class PagerDutyAlertHandler(AlertHandler):
    """Sends alerts to PagerDuty Events API v2.

    Requires ``aiohttp``.
    """

    EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"

    def __init__(
        self,
        routing_key: str,
        severity: str = "critical",
        timeout: float = 10.0,
    ) -> None:
        self._routing_key = routing_key
        self._severity = severity
        self._timeout = timeout
        self._session: Any = None

    async def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            import aiohttp

            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the reusable HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def send(self, event: str, data: dict[str, Any]) -> None:
        payload = {
            "routing_key": self._routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"Gravtory: {event}",
                "severity": self._severity,
                "source": "gravtory",
                "custom_details": data,
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            },
        }
        session = await self._get_session()
        async with session.post(self.EVENTS_URL, json=payload) as resp:
            if resp.status >= 400:
                logger.error(
                    "PagerDuty alert failed: HTTP %d",
                    resp.status,
                )


class EmailAlertHandler(AlertHandler):
    """Sends alerts via SMTP email.

    Uses stdlib ``smtplib`` — no extra dependency required.
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        sender: str = "gravtory@localhost",
        recipients: list[str] | None = None,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
    ) -> None:
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._sender = sender
        self._recipients = recipients or []
        self._username = username
        self._password = password
        self._use_tls = use_tls

    async def send(self, event: str, data: dict[str, Any]) -> None:
        import asyncio
        import smtplib
        from email.mime.text import MIMEText

        if not self._recipients:
            logger.warning("EmailAlertHandler: no recipients configured")
            return

        body_lines = [f"Event: {event}", f"Time: {datetime.now(tz=timezone.utc).isoformat()}", ""]
        for key, value in data.items():
            body_lines.append(f"  {key}: {value}")

        msg = MIMEText("\n".join(body_lines))
        msg["Subject"] = f"[Gravtory Alert] {event}"
        msg["From"] = self._sender
        msg["To"] = ", ".join(self._recipients)

        def _send_sync() -> None:
            with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
                if self._use_tls:
                    server.starttls()
                if self._username and self._password:
                    server.login(self._username, self._password)
                server.sendmail(self._sender, self._recipients, msg.as_string())

        try:
            await asyncio.get_event_loop().run_in_executor(None, _send_sync)
        except Exception:
            logger.exception("Email alert failed for event '%s'", event)


def create_handler_from_config(config: dict[str, Any]) -> AlertHandler:
    """Factory: create an AlertHandler from a configuration dict.

    Supported ``type`` values: ``log``, ``webhook``, ``slack``,
    ``pagerduty``, ``email``.

    Example configs::

        {"type": "webhook", "url": "https://hooks.example.com/grav"}
        {"type": "slack", "webhook_url": "https://hooks.slack.com/..."}
        {"type": "pagerduty", "routing_key": "abc123"}
        {"type": "email", "smtp_host": "smtp.gmail.com", "recipients": ["ops@co.com"]}
    """
    handler_type = config.get("type", "log")
    if handler_type == "log":
        return LogAlertHandler()
    if handler_type == "webhook":
        return WebhookAlertHandler(
            url=config["url"],
            headers=config.get("headers"),
            timeout=config.get("timeout", 10.0),
        )
    if handler_type == "slack":
        return SlackAlertHandler(
            webhook_url=config["webhook_url"],
            channel=config.get("channel"),
            timeout=config.get("timeout", 10.0),
        )
    if handler_type == "pagerduty":
        return PagerDutyAlertHandler(
            routing_key=config["routing_key"],
            severity=config.get("severity", "critical"),
            timeout=config.get("timeout", 10.0),
        )
    if handler_type == "email":
        return EmailAlertHandler(
            smtp_host=config["smtp_host"],
            smtp_port=config.get("smtp_port", 587),
            sender=config.get("sender", "gravtory@localhost"),
            recipients=config.get("recipients", []),
            username=config.get("username"),
            password=config.get("password"),
            use_tls=config.get("use_tls", True),
        )
    raise ValueError(f"Unknown alert handler type: {handler_type!r}")


class AlertManager:
    """Dispatches alerts to registered handlers.

    Usage::

        manager = AlertManager(handlers=[
            LogAlertHandler(),
            WebhookAlertHandler("https://hooks.example.com/gravtory"),
        ])
        await manager.fire("workflow_failed", {"workflow": "Order", "error": "timeout"})

    Supported events:
      - ``workflow_failed``
      - ``compensation_failed``
      - ``dlq_threshold``
      - ``worker_crashed``
      - ``scheduler_failover``
    """

    def __init__(
        self,
        handlers: list[AlertHandler] | None = None,
    ) -> None:
        self._handlers: list[AlertHandler] = handlers or [LogAlertHandler()]

    @property
    def handlers(self) -> list[AlertHandler]:
        return list(self._handlers)

    def add_handler(self, handler: AlertHandler) -> None:
        self._handlers.append(handler)

    async def fire(self, event: str, data: dict[str, Any]) -> None:
        """Fire an alert to all registered handlers.

        Handler failures are logged but never propagated.
        """
        for handler in self._handlers:
            try:
                await handler.send(event, data)
            except Exception:
                logger.exception(
                    "Alert handler %s failed for event '%s'",
                    type(handler).__name__,
                    event,
                )
