# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Embedded web dashboard server.

Provides :class:`Dashboard` — a lightweight aiohttp web server for
monitoring and managing Gravtory workflows.  Requires the ``dashboard``
extra (``pip install gravtory[dashboard]``).

When ``aiohttp`` is not installed, importing this module still works but
:meth:`Dashboard.start` raises :class:`RuntimeError`.
"""

from __future__ import annotations

import collections
import hmac
import logging
import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gravtory.backends.base import Backend
    from gravtory.core.registry import WorkflowRegistry

logger = logging.getLogger("gravtory.dashboard.server")

try:
    from aiohttp import web

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover
    _HAS_AIOHTTP = False


class Dashboard:
    """Embedded web dashboard for Gravtory.

    Usage::

        dashboard = Dashboard(backend, registry, port=7777)
        await dashboard.start()
        # ... later ...
        await dashboard.stop()
    """

    def __init__(
        self,
        backend: Backend,
        registry: WorkflowRegistry,
        *,
        host: str = "127.0.0.1",
        port: int = 7777,
        auth_token: str | None = None,
        allowed_origins: list[str] | None = None,
        insecure: bool = False,
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._host = host
        self._port = port
        if auth_token is None and not insecure:
            import secrets
            import tempfile

            auth_token = secrets.token_urlsafe(32)
            token_hint = auth_token[:8] + "..."
            # Write full token to a restricted file so the operator can retrieve it
            token_file = os.path.join(tempfile.gettempdir(), "gravtory-dashboard-token")
            with open(token_file, "w") as f:
                f.write(auth_token)
            os.chmod(token_file, 0o600)
            logger.warning(
                "No auth_token provided — generated random token: %s (full token written to %s). "
                "Pass this token in the Authorization header as 'Bearer <token>'. "
                "To disable authentication, pass insecure=True.",
                token_hint,
                token_file,
            )
        self._auth_token = auth_token
        self._allowed_origins: set[str] = set(allowed_origins) if allowed_origins else set()
        self._app: Any = None
        self._runner: Any = None

    async def start(self) -> None:
        """Start the dashboard web server."""
        if not _HAS_AIOHTTP:
            raise RuntimeError(
                "aiohttp is required for the dashboard — "
                "install with: pip install gravtory[dashboard]"
            )

        middlewares: list[Any] = []
        middlewares.append(self._make_rate_limit_middleware())
        middlewares.append(self._make_cors_middleware(self._allowed_origins))
        if self._auth_token is not None:
            middlewares.append(self._make_auth_middleware(self._auth_token))

        self._app = web.Application(middlewares=middlewares)
        self._setup_routes(self._app)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        logger.info(
            "Dashboard available at http://%s:%d",
            self._host,
            self._port,
        )

    async def stop(self) -> None:
        """Stop the web server gracefully."""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            logger.info("Dashboard stopped")

    def _setup_routes(self, app: web.Application) -> None:
        """Register all API and static routes."""
        from gravtory.dashboard.api import setup_routes

        setup_routes(app, self._backend, self._registry)

    @staticmethod
    def _make_cors_middleware(allowed_origins: set[str]) -> Any:
        """Create CORS middleware with explicit origin allowlist.

        If *allowed_origins* is empty, no ``Access-Control-Allow-Origin``
        header is set (same-origin only).  Only explicitly listed origins
        are reflected back.
        """

        @web.middleware
        async def cors_middleware(
            request: web.Request,
            handler: Any,
        ) -> web.StreamResponse:
            origin = request.headers.get("Origin", "")
            if request.method == "OPTIONS":
                resp = web.Response(status=204)
            else:
                resp = await handler(request)
            if origin and origin in allowed_origins:
                resp.headers["Access-Control-Allow-Origin"] = origin
                resp.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
                resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
                resp.headers["Access-Control-Max-Age"] = "3600"
            return resp

        return cors_middleware

    @staticmethod
    def _make_rate_limit_middleware(
        max_requests: int = 60,
        window_seconds: float = 60.0,
    ) -> Any:
        """Simple per-IP token bucket rate limiter."""
        buckets: dict[str, collections.deque[float]] = {}

        @web.middleware
        async def rate_limit_middleware(
            request: web.Request,
            handler: Any,
        ) -> web.StreamResponse:
            ip = request.remote or "unknown"
            now = time.monotonic()
            if ip not in buckets:
                buckets[ip] = collections.deque()
            bucket = buckets[ip]
            # Purge expired entries
            while bucket and bucket[0] <= now - window_seconds:
                bucket.popleft()
            if len(bucket) >= max_requests:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers={"Retry-After": str(int(window_seconds))},
                )
            bucket.append(now)
            result: web.StreamResponse = await handler(request)
            return result

        return rate_limit_middleware

    @staticmethod
    def _make_auth_middleware(token: str) -> Any:
        """Create bearer-token authentication middleware."""

        @web.middleware
        async def auth_middleware(
            request: web.Request,
            handler: Any,
        ) -> web.StreamResponse:
            # Protect all routes when auth is enabled
            if request.path == "/api/health":
                # Allow health checks without auth
                result: web.StreamResponse = await handler(request)
                return result
            header = request.headers.get("Authorization", "")
            provided = header.removeprefix("Bearer ").strip()
            # Support token as query param for SSE (EventSource can't send headers)
            if not provided and request.path == "/api/events":
                provided = request.query.get("token", "")
            if not hmac.compare_digest(provided, token):
                return web.json_response(
                    {"error": "Unauthorized"},
                    status=401,
                )
            result = await handler(request)
            return result

        return auth_middleware
