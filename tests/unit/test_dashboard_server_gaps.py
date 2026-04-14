"""Tests for dashboard.server — Dashboard lifecycle and auth middleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gravtory.dashboard.server import Dashboard

pytestmark = pytest.mark.filterwarnings("ignore::ResourceWarning")


@pytest.fixture
def mock_backend() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_registry() -> MagicMock:
    return MagicMock()


class TestDashboardInit:
    def test_defaults(self, mock_backend: AsyncMock, mock_registry: MagicMock) -> None:
        d = Dashboard(mock_backend, mock_registry, insecure=True)
        assert d._host == "127.0.0.1"
        assert d._port == 7777
        assert d._auth_token is None

    def test_custom_settings(self, mock_backend: AsyncMock, mock_registry: MagicMock) -> None:
        d = Dashboard(
            mock_backend,
            mock_registry,
            host="127.0.0.1",
            port=9999,
            auth_token="secret-token",
        )
        assert d._host == "127.0.0.1"
        assert d._port == 9999
        assert d._auth_token == "secret-token"


class TestDashboardStart:
    @pytest.mark.asyncio
    async def test_start_creates_app(
        self,
        mock_backend: AsyncMock,
        mock_registry: MagicMock,
    ) -> None:
        d = Dashboard(mock_backend, mock_registry, port=18777)

        mock_runner = MagicMock()
        mock_runner.setup = AsyncMock()
        mock_runner.cleanup = AsyncMock()
        mock_site = MagicMock()
        mock_site.start = AsyncMock()

        with (
            patch("gravtory.dashboard.server.web.Application") as mock_app_cls,
            patch("gravtory.dashboard.server.web.AppRunner", return_value=mock_runner),
            patch("gravtory.dashboard.server.web.TCPSite", return_value=mock_site),
            patch.object(d, "_setup_routes"),
        ):
            await d.start()
            assert d._app is not None
            assert d._runner is mock_runner
            mock_runner.setup.assert_awaited_once()
            mock_site.start.assert_awaited_once()

        # Cleanup
        await d.stop()

    @pytest.mark.asyncio
    async def test_start_with_auth_token(
        self,
        mock_backend: AsyncMock,
        mock_registry: MagicMock,
    ) -> None:
        d = Dashboard(mock_backend, mock_registry, port=18778, auth_token="tok123")

        mock_runner = MagicMock()
        mock_runner.setup = AsyncMock()
        mock_runner.cleanup = AsyncMock()
        mock_site = MagicMock()
        mock_site.start = AsyncMock()

        with (
            patch("gravtory.dashboard.server.web.Application") as mock_app_cls,
            patch("gravtory.dashboard.server.web.AppRunner", return_value=mock_runner),
            patch("gravtory.dashboard.server.web.TCPSite", return_value=mock_site),
            patch.object(d, "_setup_routes"),
        ):
            await d.start()
            # Application should have been called with middlewares
            mock_app_cls.assert_called_once()
            call_kwargs = mock_app_cls.call_args
            middlewares = call_kwargs.kwargs.get("middlewares") or call_kwargs[1].get(
                "middlewares", []
            )
            assert len(middlewares) == 3  # rate limiter + CORS + auth middleware

        await d.stop()


class TestDashboardStop:
    @pytest.mark.asyncio
    async def test_stop_without_start(
        self,
        mock_backend: AsyncMock,
        mock_registry: MagicMock,
    ) -> None:
        d = Dashboard(mock_backend, mock_registry)
        await d.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_stop_cleans_runner(
        self,
        mock_backend: AsyncMock,
        mock_registry: MagicMock,
    ) -> None:
        d = Dashboard(mock_backend, mock_registry)
        mock_runner = MagicMock()
        mock_runner.cleanup = AsyncMock()
        d._runner = mock_runner

        await d.stop()
        mock_runner.cleanup.assert_awaited_once()
        assert d._runner is None


class TestAuthMiddleware:
    @pytest.mark.asyncio
    async def test_api_route_blocked_without_token(self) -> None:
        middleware_fn = Dashboard._make_auth_middleware("my-secret")

        mock_request = MagicMock()
        mock_request.path = "/api/workflows"
        mock_request.headers = {}

        mock_handler = AsyncMock()
        response = await middleware_fn(mock_request, mock_handler)
        # Should return 401
        assert response.status == 401
        mock_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_api_route_allowed_with_valid_token(self) -> None:
        middleware_fn = Dashboard._make_auth_middleware("my-secret")

        mock_request = MagicMock()
        mock_request.path = "/api/workflows"
        mock_request.headers = {"Authorization": "Bearer my-secret"}

        mock_response = MagicMock()
        mock_handler = AsyncMock(return_value=mock_response)
        response = await middleware_fn(mock_request, mock_handler)
        assert response is mock_response

    @pytest.mark.asyncio
    async def test_non_api_route_requires_auth(self) -> None:
        middleware_fn = Dashboard._make_auth_middleware("my-secret")

        mock_request = MagicMock()
        mock_request.path = "/dashboard"
        mock_request.headers = {}

        mock_handler = AsyncMock()
        response = await middleware_fn(mock_request, mock_handler)
        assert response.status == 401

    @pytest.mark.asyncio
    async def test_health_endpoint_passes_without_auth(self) -> None:
        middleware_fn = Dashboard._make_auth_middleware("my-secret")

        mock_request = MagicMock()
        mock_request.path = "/api/health"
        mock_request.headers = {}

        mock_response = MagicMock()
        mock_handler = AsyncMock(return_value=mock_response)
        response = await middleware_fn(mock_request, mock_handler)
        assert response is mock_response

    @pytest.mark.asyncio
    async def test_api_route_wrong_token(self) -> None:
        middleware_fn = Dashboard._make_auth_middleware("correct-token")

        mock_request = MagicMock()
        mock_request.path = "/api/runs"
        mock_request.headers = {"Authorization": "Bearer wrong-token"}

        mock_handler = AsyncMock()
        response = await middleware_fn(mock_request, mock_handler)
        assert response.status == 401
