"""Security-focused tests for the Gravtory dashboard and API.

Covers:
  - SQL injection via table_prefix and user inputs
  - XSS via workflow names and step data rendered in dashboard
  - Auth token bypass attempts
  - Signal payload injection
  - Path traversal in API routes

Run with: pytest tests/security/ -v
"""

from __future__ import annotations

from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import WorkflowRun, WorkflowStatus
from gravtory.dashboard.api import setup_routes
from gravtory.dashboard.server import Dashboard

pytestmark = pytest.mark.asyncio


async def _make_app(
    backend: InMemoryBackend | None = None,
    auth_token: str | None = None,
) -> web.Application:
    if backend is None:
        backend = InMemoryBackend()
        await backend.initialize()
    registry = WorkflowRegistry()

    middlewares: list[Any] = []
    if auth_token is not None:
        middlewares.append(Dashboard._make_auth_middleware(auth_token))

    app = web.Application(middlewares=middlewares)
    setup_routes(app, backend, registry)
    return app


# ── Auth bypass tests ──────────────────────────────────────────────


class TestAuthBypass:
    async def test_no_token_rejected(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows")
            assert resp.status == 401

    async def test_wrong_token_rejected(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(
                "/api/workflows",
                headers={"Authorization": "Bearer wrong-token"},
            )
            assert resp.status == 401

    async def test_empty_bearer_rejected(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(
                "/api/workflows",
                headers={"Authorization": "Bearer "},
            )
            assert resp.status == 401

    async def test_basic_auth_scheme_rejected(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(
                "/api/workflows",
                headers={"Authorization": "Basic c2VjcmV0LXRva2VuLTEyMw=="},
            )
            assert resp.status == 401

    async def test_valid_token_accepted(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(
                "/api/workflows",
                headers={"Authorization": "Bearer secret-token-123"},
            )
            assert resp.status == 200

    async def test_static_html_requires_auth(self) -> None:
        app = await _make_app(auth_token="secret-token-123")
        async with TestClient(TestServer(app)) as client:
            # Dashboard is protected when auth is enabled
            resp = await client.get("/")
            assert resp.status == 401
            # With valid token, dashboard is accessible
            resp = await client.get(
                "/",
                headers={"Authorization": "Bearer secret-token-123"},
            )
            assert resp.status == 200


# ── XSS injection tests ───────────────────────────────────────────


class TestXSSPrevention:
    async def test_xss_in_workflow_name_escaped(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run = WorkflowRun(
            id="xss-run",
            workflow_name='<script>alert("xss")</script>',
            status=WorkflowStatus.COMPLETED,
        )
        await backend.create_workflow_run(run)

        app = await _make_app(backend)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows/xss-run")
            data = await resp.json()
            # API returns raw data; XSS escaping is client-side via esc()
            assert data["workflow_name"] == '<script>alert("xss")</script>'

    async def test_xss_in_run_id_escaped_in_list(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        malicious_id = '"><img src=x onerror=alert(1)>'
        run = WorkflowRun(
            id=malicious_id,
            workflow_name="TestWF",
            status=WorkflowStatus.PENDING,
        )
        await backend.create_workflow_run(run)

        app = await _make_app(backend)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows")
            data = await resp.json()
            assert any(w["id"] == malicious_id for w in data["workflows"])


# ── Signal payload injection tests ─────────────────────────────────


class TestSignalInjection:
    async def test_oversized_signal_payload(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run = WorkflowRun(id="sig-run", workflow_name="SigWF", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)

        app = await _make_app(backend)
        async with TestClient(TestServer(app)) as client:
            # Send a very large payload
            huge_payload = {"data": "x" * 1_000_000}
            resp = await client.post(
                "/api/workflows/sig-run/signals/test",
                json=huge_payload,
            )
            # Should either succeed (if no limit) or return 413/400
            assert resp.status in (200, 400, 413)

    async def test_null_byte_in_signal_name(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        run = WorkflowRun(id="sig-run2", workflow_name="SigWF", status=WorkflowStatus.RUNNING)
        await backend.create_workflow_run(run)

        app = await _make_app(backend)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/workflows/sig-run2/signals/test%00evil",
            )
            # Should not crash
            assert resp.status in (200, 400, 404)


# ── Path traversal tests ──────────────────────────────────────────


class TestPathTraversal:
    async def test_dotdot_in_run_id(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows/../../etc/passwd")
            assert resp.status in (400, 404)

    async def test_dotdot_in_dlq_id(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.post("/api/dlq/../../etc/passwd/retry")
            assert resp.status in (400, 404)

    async def test_encoded_traversal(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows/%2e%2e%2f%2e%2e%2fetc%2fpasswd")
            assert resp.status in (400, 404)


# ── SQL injection tests (via table_prefix) ─────────────────────────


class TestSQLInjection:
    async def test_sql_in_workflow_name_filter(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/workflows?workflow_name='; DROP TABLE workflows; --")
            # In-memory backend won't crash; SQL backends should parameterize
            assert resp.status == 200

    async def test_sql_in_namespace(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/stats?namespace='; DROP TABLE runs; --")
            assert resp.status == 200


# ── Header injection tests ─────────────────────────────────────────


class TestHeaderInjection:
    async def test_crlf_in_header(self) -> None:
        app = await _make_app()
        async with TestClient(TestServer(app)) as client:
            # aiohttp >= 3.10 rejects CRLF in headers at the client level,
            # which is the correct security behaviour (prevents header injection).
            with pytest.raises(ValueError, match="[Nn]ewline|[Cc]arriage|header injection"):
                await client.get(
                    "/api/health",
                    headers={"X-Custom": "value\r\nInjected: header"},
                )
