# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""FastAPI integration — lifespan management and dependency injection.

Usage::

    from fastapi import FastAPI, Depends
    from gravtory.contrib.fastapi import GravtoryIntegration

    grav_integration = GravtoryIntegration("postgresql://localhost/mydb")

    app = FastAPI(lifespan=grav_integration.lifespan)

    @app.post("/orders")
    async def create_order(
        payload: OrderPayload,
        grav: Gravtory = Depends(grav_integration.dependency),
    ):
        run = await grav.run("process_order", input_data=payload.dict())
        return {"run_id": run.id}

    @app.get("/orders/{run_id}")
    async def get_order_status(
        run_id: str,
        grav: Gravtory = Depends(grav_integration.dependency),
    ):
        run = await grav.get_run(run_id)
        return {"status": run.status.value if run else "not_found"}
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from gravtory.core.engine import Gravtory


class GravtoryIntegration:
    """FastAPI integration for Gravtory lifecycle and dependency injection.

    Manages the Gravtory engine lifecycle through FastAPI's lifespan protocol
    and provides a dependency callable for route injection.

    Args:
        dsn: Database connection string (e.g. ``"postgresql://..."``).
        **kwargs: Additional keyword arguments passed to the ``Gravtory`` constructor
            (e.g. ``namespace``, ``serializer``, ``encryption_key``).
    """

    def __init__(self, dsn: str, **kwargs: Any) -> None:
        self._dsn = dsn
        self._kwargs = kwargs
        self._engine: Gravtory | None = None

    @asynccontextmanager
    async def lifespan(self, app: Any) -> AsyncIterator[None]:
        """FastAPI lifespan context manager.

        Starts the Gravtory engine on application startup and shuts it down
        on application shutdown.  Use as ``FastAPI(lifespan=integration.lifespan)``.
        """
        from gravtory.core.engine import Gravtory

        self._engine = Gravtory(self._dsn, **self._kwargs)
        await self._engine.start()
        try:
            yield
        finally:
            await self._engine.shutdown()
            self._engine = None

    def dependency(self) -> Gravtory:
        """FastAPI dependency that returns the active Gravtory engine.

        Usage::

            @app.post("/run")
            async def trigger(grav: Gravtory = Depends(integration.dependency)):
                ...

        Raises:
            RuntimeError: If the engine has not been started via lifespan.
        """
        if self._engine is None:
            raise RuntimeError(
                "Gravtory engine is not running. "
                "Ensure GravtoryIntegration.lifespan is set as the FastAPI lifespan."
            )
        return self._engine

    @property
    def engine(self) -> Gravtory | None:
        """The underlying Gravtory engine, or None if not started."""
        return self._engine
