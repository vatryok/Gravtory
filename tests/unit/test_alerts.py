"""Tests for AlertManager and alert handlers."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.observability.alerts import (
    AlertHandler,
    AlertManager,
    LogAlertHandler,
    SlackAlertHandler,
    WebhookAlertHandler,
)


class _MockHandler(AlertHandler):
    """Test-only handler that records calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def send(self, event: str, data: dict[str, Any]) -> None:
        self.calls.append((event, data))


class _FailingHandler(AlertHandler):
    """Handler that always raises."""

    async def send(self, event: str, data: dict[str, Any]) -> None:
        raise RuntimeError("handler exploded")


class TestLogAlertHandler:
    @pytest.mark.asyncio
    async def test_log_handler_always_works(self) -> None:
        handler = LogAlertHandler()
        await handler.send("workflow_failed", {"workflow": "Order"})


class TestWebhookAlertHandler:
    def test_webhook_handler_stores_config(self) -> None:
        handler = WebhookAlertHandler(
            url="https://hooks.example.com/test",
            headers={"Authorization": "Bearer token"},
        )
        assert handler._url == "https://hooks.example.com/test"
        assert handler._headers == {"Authorization": "Bearer token"}


class TestSlackAlertHandler:
    def test_slack_handler_format_message(self) -> None:
        handler = SlackAlertHandler(
            webhook_url="https://hooks.slack.com/test",
            channel="#alerts",
        )
        msg = handler._format_message("workflow_failed", {"wf": "Order"})
        assert "attachments" in msg
        assert msg["channel"] == "#alerts"
        assert msg["attachments"][0]["color"] == "#dc3545"  # red for failure

    def test_slack_handler_warning_color(self) -> None:
        handler = SlackAlertHandler(webhook_url="https://hooks.slack.com/test")
        msg = handler._format_message("dlq_threshold", {"size": 100})
        assert msg["attachments"][0]["color"] == "#ffc107"  # yellow for warning


class TestAlertManager:
    @pytest.mark.asyncio
    async def test_fire_sends_to_all_handlers(self) -> None:
        h1 = _MockHandler()
        h2 = _MockHandler()
        manager = AlertManager(handlers=[h1, h2])

        await manager.fire("workflow_failed", {"wf": "Order"})
        assert len(h1.calls) == 1
        assert len(h2.calls) == 1
        assert h1.calls[0] == ("workflow_failed", {"wf": "Order"})

    @pytest.mark.asyncio
    async def test_handler_failure_isolated(self) -> None:
        """A broken handler must not prevent other handlers from receiving the alert."""
        failing = _FailingHandler()
        good = _MockHandler()
        manager = AlertManager(handlers=[failing, good])

        await manager.fire("worker_crashed", {"node": "n1"})
        # Good handler still received the alert
        assert len(good.calls) == 1

    @pytest.mark.asyncio
    async def test_default_handler_is_log(self) -> None:
        manager = AlertManager()
        assert len(manager.handlers) == 1
        assert isinstance(manager.handlers[0], LogAlertHandler)

    @pytest.mark.asyncio
    async def test_add_handler(self) -> None:
        manager = AlertManager()
        mock = _MockHandler()
        manager.add_handler(mock)
        assert len(manager.handlers) == 2

        await manager.fire("test_event", {})
        assert len(mock.calls) == 1

    @pytest.mark.asyncio
    async def test_multiple_events(self) -> None:
        handler = _MockHandler()
        manager = AlertManager(handlers=[handler])

        await manager.fire("workflow_failed", {"a": 1})
        await manager.fire("dlq_threshold", {"b": 2})
        await manager.fire("scheduler_failover", {"c": 3})

        assert len(handler.calls) == 3


class TestAlertsGapFill:
    """Gap-fill tests for alert system edge cases."""

    @pytest.mark.asyncio
    async def test_fire_with_no_handlers(self) -> None:
        """Firing with no handlers doesn't raise."""
        manager = AlertManager(handlers=[])
        await manager.fire("test_event", {"key": "val"})

    @pytest.mark.asyncio
    async def test_handler_receives_event_name(self) -> None:
        handler = _MockHandler()
        manager = AlertManager(handlers=[handler])
        await manager.fire("custom_alert", {"detail": 42})
        assert handler.calls[0] == ("custom_alert", {"detail": 42})

    @pytest.mark.asyncio
    async def test_many_handlers_all_receive(self) -> None:
        handlers = [_MockHandler() for _ in range(5)]
        manager = AlertManager(handlers=handlers)
        await manager.fire("event", {})
        for h in handlers:
            assert len(h.calls) == 1
