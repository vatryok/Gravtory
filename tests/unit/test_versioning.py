"""Tests for workflow versioning — VersionMigrator."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.registry import WorkflowRegistry
from gravtory.core.types import (
    StepDefinition,
    StepOutput,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.enterprise.versioning import MigrationRule, VersionMigrator


class TestVersioning:
    """Workflow versioning and migration tests."""

    @pytest.fixture()
    def backend(self) -> InMemoryBackend:
        return InMemoryBackend()

    @pytest.fixture()
    def registry(self) -> Any:
        from gravtory.core.registry import WorkflowRegistry

        reg = WorkflowRegistry()
        # Register v1
        reg.register(
            WorkflowDefinition(
                name="OrderWorkflow",
                version=1,
                steps={1: StepDefinition(order=1, name="charge")},
                config=WorkflowConfig(version=1),
            )
        )
        # Register v2
        reg.register(
            WorkflowDefinition(
                name="OrderWorkflow",
                version=2,
                steps={1: StepDefinition(order=1, name="charge_v2")},
                config=WorkflowConfig(version=2),
            )
        )
        return reg

    @pytest.mark.asyncio()
    async def test_new_workflow_uses_latest(self, registry: Any) -> None:
        """Getting a workflow without specifying version returns latest."""
        defn = registry.get("OrderWorkflow")
        assert defn.version == 2

    @pytest.mark.asyncio()
    async def test_resume_uses_original_version(self, registry: Any) -> None:
        """Getting a workflow with explicit version returns that version."""
        defn = registry.get("OrderWorkflow", version=1)
        assert defn.version == 1
        assert defn.steps[1].name == "charge"

    @pytest.mark.asyncio()
    async def test_multiple_versions_coexist(self, registry: Any) -> None:
        """Multiple versions of the same workflow coexist in registry."""
        v1 = registry.get("OrderWorkflow", version=1)
        v2 = registry.get("OrderWorkflow", version=2)
        assert v1.version == 1
        assert v2.version == 2
        assert v1.steps[1].name == "charge"
        assert v2.steps[1].name == "charge_v2"

    @pytest.mark.asyncio()
    async def test_version_migration(
        self,
        backend: InMemoryBackend,
        registry: Any,
    ) -> None:
        """VersionMigrator applies migration to eligible in-progress runs."""
        await backend.initialize()
        # Create a running v1 workflow
        await backend.create_workflow_run(
            WorkflowRun(
                id="run-1",
                workflow_name="OrderWorkflow",
                workflow_version=1,
                namespace="default",
                status=WorkflowStatus.RUNNING,
            )
        )
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="run-1",
                step_order=1,
                step_name="charge",
            )
        )

        async def migrate_v1_to_v2(
            run: WorkflowRun,
            outputs: list[StepOutput],
        ) -> list[StepOutput]:
            for o in outputs:
                o.step_name = "charge_v2"
            return outputs

        migrator = VersionMigrator(backend, registry)
        migrator.register_migration(
            workflow="OrderWorkflow",
            from_version=1,
            to_version=2,
            migrate_fn=migrate_v1_to_v2,
        )

        count = await migrator.migrate_all()
        assert count == 1

        # Verify the run's version was actually bumped to 2
        run = await backend.get_workflow_run("run-1")
        assert run is not None
        assert run.workflow_version == 2

        # Verify step output was transformed in place
        outputs = await backend.get_step_outputs("run-1")
        assert outputs[0].step_name == "charge_v2"

        # Running migrate_all again should NOT re-migrate (version is now 2)
        count2 = await migrator.migrate_all()
        assert count2 == 0

    @pytest.mark.asyncio()
    async def test_migrate_single(
        self,
        backend: InMemoryBackend,
        registry: Any,
    ) -> None:
        """migrate_single targets a specific run and bumps its version."""
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(
                id="run-x",
                workflow_name="OrderWorkflow",
                workflow_version=1,
                namespace="default",
                status=WorkflowStatus.RUNNING,
            )
        )

        async def noop_migrate(
            run: WorkflowRun,
            outputs: list[StepOutput],
        ) -> list[StepOutput]:
            return outputs

        migrator = VersionMigrator(backend, registry)
        migrator.register_migration(
            workflow="OrderWorkflow",
            from_version=1,
            to_version=2,
            migrate_fn=noop_migrate,
        )

        result = await migrator.migrate_single("run-x")
        assert result is True

        # Version should be bumped
        run = await backend.get_workflow_run("run-x")
        assert run is not None
        assert run.workflow_version == 2

        # Non-existent run
        result2 = await migrator.migrate_single("no-such-run")
        assert result2 is False

    def test_migration_rule_fields(self) -> None:
        """MigrationRule stores all fields."""

        async def dummy(
            run: WorkflowRun,
            outputs: list[StepOutput],
        ) -> list[StepOutput]:
            return outputs

        rule = MigrationRule(
            workflow="Test",
            from_version=1,
            to_version=2,
            migrate_fn=dummy,
        )
        assert rule.workflow == "Test"
        assert rule.from_version == 1
        assert rule.to_version == 2
        assert rule.migrate_fn is dummy


class TestVersioningGapFill:
    """Gap-fill tests for versioning edge cases."""

    def test_migration_rule_version_ordering(self) -> None:
        """from_version < to_version is expected usage."""

        async def noop(run: WorkflowRun, outputs: list[StepOutput]) -> list[StepOutput]:
            return outputs

        rule = MigrationRule(workflow="W", from_version=3, to_version=7, migrate_fn=noop)
        assert rule.to_version > rule.from_version

    @pytest.mark.asyncio()
    async def test_migrate_single_nonexistent(self) -> None:
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        migrator = VersionMigrator(backend, registry)
        result = await migrator.migrate_single("no-such-run")
        assert result is False

    @pytest.mark.asyncio()
    async def test_migrate_single_no_matching_rule(self) -> None:
        """Run exists but no rule matches its version."""
        backend = InMemoryBackend()
        await backend.initialize()
        registry = WorkflowRegistry()
        await backend.create_workflow_run(
            WorkflowRun(id="run-1", workflow_name="Wf", workflow_version=1)
        )
        migrator = VersionMigrator(backend, registry)
        result = await migrator.migrate_single("run-1")
        assert result is False
