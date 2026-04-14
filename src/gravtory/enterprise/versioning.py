# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Workflow versioning — migrate in-progress workflows between versions.

Provides :class:`VersionMigrator` that transforms running workflows
from one version to another using user-defined migration functions.

The migration function receives the run and its step outputs and is
expected to modify them **in place**.  After the function returns,
the migrator updates ``run.workflow_version`` to ``to_version`` so
that the run is not re-migrated on subsequent calls.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from gravtory.core.types import StepOutput, WorkflowRun, WorkflowStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from gravtory.backends.base import Backend
    from gravtory.core.registry import WorkflowRegistry

    # async (WorkflowRun, list[StepOutput]) -> list[StepOutput]
    MigrateFn = Callable[[WorkflowRun, list[StepOutput]], Awaitable[list[StepOutput]]]

logger = logging.getLogger("gravtory.enterprise.versioning")


@dataclass
class MigrationRule:
    """A registered migration from one version to another."""

    workflow: str
    from_version: int
    to_version: int
    migrate_fn: MigrateFn


class VersionMigrator:
    """Migrate running workflows from one version to another.

    Usage::

        migrator = VersionMigrator(backend, registry)
        migrator.register_migration(
            workflow="OrderWorkflow",
            from_version=1,
            to_version=2,
            migrate_fn=migrate_order_v1_to_v2,
        )

        count = await migrator.migrate_all()
    """

    def __init__(
        self,
        backend: Backend,
        registry: WorkflowRegistry,
    ) -> None:
        self._backend = backend
        self._registry = registry
        self._migrations: list[MigrationRule] = []

    def register_migration(
        self,
        workflow: str,
        from_version: int,
        to_version: int,
        migrate_fn: MigrateFn,
    ) -> None:
        """Register a migration function for a workflow version upgrade.

        Args:
            workflow: Workflow name.
            from_version: Source version.
            to_version: Target version.
            migrate_fn: Async function ``(WorkflowRun, list[StepOutput]) -> list[StepOutput]``
                that transforms step outputs **in place**.
        """
        self._migrations.append(
            MigrationRule(
                workflow=workflow,
                from_version=from_version,
                to_version=to_version,
                migrate_fn=migrate_fn,
            )
        )

    def get_migration(self, workflow: str, from_version: int) -> MigrationRule | None:
        """Find a migration rule for a specific workflow and version."""
        for m in self._migrations:
            if m.workflow == workflow and m.from_version == from_version:
                return m
        return None

    async def migrate_all(self, namespace: str = "default") -> int:
        """Migrate all eligible in-progress workflows.

        Finds all running/pending workflows whose version matches a
        registered ``from_version`` and applies the migration function.

        Returns:
            Count of migrated runs.
        """
        count = 0
        for migration in self._migrations:
            migrated = await self._apply_migration(migration, namespace)
            count += migrated
        return count

    async def _apply_migration(self, migration: MigrationRule, namespace: str) -> int:
        """Apply a single migration rule to all matching workflows."""
        active_statuses = (WorkflowStatus.RUNNING, WorkflowStatus.PENDING)
        count = 0

        for status in active_statuses:
            runs = await self._backend.list_workflow_runs(
                namespace=namespace,
                status=status,
                workflow_name=migration.workflow,
                limit=10000,
            )
            for run in runs:
                if run.workflow_version != migration.from_version:
                    continue

                try:
                    step_outputs = list(await self._backend.get_step_outputs(run.id))
                    # The migration fn modifies run & outputs in place.
                    await migration.migrate_fn(run, step_outputs)

                    # Bump the version so this run won't be re-migrated.
                    run.workflow_version = migration.to_version
                    await self._backend.update_workflow_status(
                        run.id,
                        run.status,
                    )

                    count += 1
                    logger.info(
                        "Migrated %s from v%d to v%d",
                        run.id,
                        migration.from_version,
                        migration.to_version,
                    )
                except Exception:
                    logger.exception(
                        "Failed to migrate %s from v%d to v%d",
                        run.id,
                        migration.from_version,
                        migration.to_version,
                    )
        return count

    async def migrate_single(self, run_id: str) -> bool:
        """Migrate a single workflow run if a matching migration exists.

        Returns True if migration was applied, False otherwise.
        """
        run = await self._backend.get_workflow_run(run_id)
        if run is None:
            return False

        migration = self.get_migration(run.workflow_name, run.workflow_version)
        if migration is None:
            return False

        step_outputs = list(await self._backend.get_step_outputs(run_id))
        await migration.migrate_fn(run, step_outputs)

        # Bump the version so this run won't be re-migrated.
        run.workflow_version = migration.to_version
        await self._backend.update_workflow_status(run_id, run.status)
        logger.info(
            "Migrated %s from v%d to v%d",
            run_id,
            migration.from_version,
            migration.to_version,
        )
        return True
