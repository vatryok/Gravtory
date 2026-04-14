"""Tests for Dashboard server — start, stop, HTML serving."""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.registry import WorkflowRegistry
from gravtory.dashboard.server import Dashboard


def _make_dashboard(
    auth_token: str | None = None,
    insecure: bool = False,
) -> Dashboard:
    backend = InMemoryBackend()
    registry = WorkflowRegistry()
    return Dashboard(backend, registry, auth_token=auth_token, insecure=insecure)


class TestDashboardServer:
    def test_dashboard_constructor(self) -> None:
        dash = _make_dashboard()
        assert dash._host == "127.0.0.1"
        assert dash._port == 7777

    def test_dashboard_custom_port(self) -> None:
        backend = InMemoryBackend()
        registry = WorkflowRegistry()
        dash = Dashboard(backend, registry, port=9999, host="127.0.0.1")
        assert dash._port == 9999
        assert dash._host == "127.0.0.1"

    @pytest.mark.asyncio
    async def test_stop_without_start(self) -> None:
        """Stopping without starting should not raise."""
        dash = _make_dashboard()
        await dash.stop()


class TestDashboardServerGapFill:
    """Gap-fill tests for dashboard server edge cases."""

    def test_dashboard_with_auth_token(self) -> None:
        dash = _make_dashboard(auth_token="secret-123")
        assert dash._auth_token == "secret-123"

    def test_dashboard_default_no_auth(self) -> None:
        dash = _make_dashboard(insecure=True)
        assert dash._auth_token is None

    def test_make_auth_middleware_returns_callable(self) -> None:
        mw = Dashboard._make_auth_middleware("token-abc")
        assert callable(mw)
