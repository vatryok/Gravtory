"""Tests for observability.alerts — AlertManager, handlers, and edge cases."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from gravtory.observability.alerts import (
    AlertHandler,
    AlertManager,
    LogAlertHandler,
    SlackAlertHandler,
    WebhookAlertHandler,
)


class TestLogAlertHandler:
    @pytest.mark.asyncio
    async def test_send_logs(self) -> None:
        handler = LogAlertHandler()
        await handler.send("workflow_failed", {"workflow": "Order", "error": "timeout"})


class TestWebhookAlertHandler:
    def test_init_with_defaults(self) -> None:
        handler = WebhookAlertHandler("https://example.com/hook")
        assert handler._url == "https://example.com/hook"
        assert handler._headers == {}

    def test_init_with_headers(self) -> None:
        handler = WebhookAlertHandler(
            "https://example.com/hook",
            headers={"X-Auth": "token123"},
        )
        assert handler._headers == {"X-Auth": "token123"}


class TestSlackAlertHandler:
    def test_init(self) -> None:
        handler = SlackAlertHandler("https://hooks.slack.com/xxx")
        assert handler._url == "https://hooks.slack.com/xxx"
        assert handler._channel is None

    def test_init_with_channel(self) -> None:
        handler = SlackAlertHandler("https://hooks.slack.com/xxx", channel="#alerts")
        assert handler._channel == "#alerts"

    def test_format_message_fail_event(self) -> None:
        handler = SlackAlertHandler("https://hooks.slack.com/xxx", channel="#alerts")
        msg = handler._format_message("workflow_failed", {"workflow": "Order"})
        assert "attachments" in msg
        assert msg["channel"] == "#alerts"
        assert msg["attachments"][0]["color"] == "#dc3545"  # red for fail

    def test_format_message_non_fail_event(self) -> None:
        handler = SlackAlertHandler("https://hooks.slack.com/xxx")
        msg = handler._format_message("workflow_started", {"workflow": "Order"})
        assert msg["attachments"][0]["color"] == "#ffc107"  # yellow for non-fail
        assert "channel" not in msg


class TestAlertManager:
    def test_default_handler(self) -> None:
        mgr = AlertManager()
        assert len(mgr.handlers) == 1
        assert isinstance(mgr.handlers[0], LogAlertHandler)

    def test_custom_handlers(self) -> None:
        h1 = LogAlertHandler()
        h2 = LogAlertHandler()
        mgr = AlertManager(handlers=[h1, h2])
        assert len(mgr.handlers) == 2

    def test_add_handler(self) -> None:
        mgr = AlertManager()
        h = LogAlertHandler()
        mgr.add_handler(h)
        assert len(mgr.handlers) == 2

    @pytest.mark.asyncio
    async def test_fire_calls_all_handlers(self) -> None:
        h1 = AsyncMock(spec=AlertHandler)
        h2 = AsyncMock(spec=AlertHandler)
        mgr = AlertManager(handlers=[h1, h2])

        await mgr.fire("workflow_failed", {"wf": "Order"})
        h1.send.assert_awaited_once_with("workflow_failed", {"wf": "Order"})
        h2.send.assert_awaited_once_with("workflow_failed", {"wf": "Order"})

    @pytest.mark.asyncio
    async def test_fire_isolates_handler_failures(self) -> None:
        failing = AsyncMock(spec=AlertHandler)
        failing.send.side_effect = RuntimeError("handler broken")
        working = AsyncMock(spec=AlertHandler)

        mgr = AlertManager(handlers=[failing, working])
        await mgr.fire("test_event", {"data": 1})
        # Working handler should still be called despite the first one failing
        working.send.assert_awaited_once()


class TestWebhookAlertHandlerSend:
    @pytest.mark.asyncio
    async def test_send_success(self) -> None:
        from unittest.mock import MagicMock, patch

        handler = WebhookAlertHandler("https://example.com/hook", headers={"X-Key": "abc"})

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await handler.send("workflow_failed", {"wf": "Order"})
        mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_http_error_logged(self) -> None:
        from unittest.mock import MagicMock, patch

        handler = WebhookAlertHandler("https://example.com/hook")

        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await handler.send("test_event", {"data": 1})


class TestSlackAlertHandlerSend:
    @pytest.mark.asyncio
    async def test_send_success(self) -> None:
        from unittest.mock import MagicMock, patch

        handler = SlackAlertHandler("https://hooks.slack.com/xxx", channel="#alerts")

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await handler.send("workflow_failed", {"wf": "Order"})
        mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_http_error_logged(self) -> None:
        from unittest.mock import MagicMock, patch

        handler = SlackAlertHandler("https://hooks.slack.com/xxx")

        mock_resp = AsyncMock()
        mock_resp.status = 400
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await handler.send("dlq_threshold", {"count": 100})
