"""Tests for multi-tenancy — GravtoryAdmin cross-namespace operations."""

from __future__ import annotations

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.types import WorkflowRun, WorkflowStatus
from gravtory.enterprise.admin import GravtoryAdmin, NamespaceStats


class TestMultiTenancy:
    """Multi-tenancy isolation and admin operations."""

    @pytest.fixture()
    def backend(self) -> InMemoryBackend:
        return InMemoryBackend()

    @pytest.fixture()
    def admin(self, backend: InMemoryBackend) -> GravtoryAdmin:
        return GravtoryAdmin(backend, namespaces=["default"])

    @pytest.mark.asyncio()
    async def test_namespace_isolation(self, backend: InMemoryBackend) -> None:
        """team-a cannot see team-b's workflows."""
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(id="run-a", workflow_name="wf", namespace="team-a")
        )
        await backend.create_workflow_run(
            WorkflowRun(id="run-b", workflow_name="wf", namespace="team-b")
        )
        team_a_runs = await backend.list_workflow_runs(namespace="team-a")
        team_b_runs = await backend.list_workflow_runs(namespace="team-b")

        assert len(team_a_runs) == 1
        assert team_a_runs[0].id == "run-a"
        assert len(team_b_runs) == 1
        assert team_b_runs[0].id == "run-b"

    @pytest.mark.asyncio()
    async def test_default_namespace(self, backend: InMemoryBackend) -> None:
        """Workflows without explicit namespace land in 'default'."""
        await backend.initialize()
        await backend.create_workflow_run(WorkflowRun(id="run-1", workflow_name="wf"))
        runs = await backend.list_workflow_runs(namespace="default")
        assert len(runs) == 1
        assert runs[0].namespace == "default"

    @pytest.mark.asyncio()
    async def test_admin_stats_by_namespace(
        self,
        backend: InMemoryBackend,
        admin: GravtoryAdmin,
    ) -> None:
        """Admin can get per-namespace statistics."""
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(
                id="r1", workflow_name="wf", namespace="ns1", status=WorkflowStatus.COMPLETED
            )
        )
        await backend.create_workflow_run(
            WorkflowRun(id="r2", workflow_name="wf", namespace="ns1", status=WorkflowStatus.FAILED)
        )
        await backend.create_workflow_run(
            WorkflowRun(id="r3", workflow_name="wf", namespace="ns2", status=WorkflowStatus.RUNNING)
        )

        stats = await admin.stats_by_namespace(namespaces=["ns1", "ns2"])
        assert "ns1" in stats
        assert "ns2" in stats
        assert stats["ns1"].total == 2
        assert stats["ns1"].completed == 1
        assert stats["ns1"].failed == 1
        assert stats["ns2"].running == 1

    @pytest.mark.asyncio()
    async def test_admin_cross_namespace_list(
        self,
        backend: InMemoryBackend,
        admin: GravtoryAdmin,
    ) -> None:
        """Admin can list runs across namespaces."""
        await backend.initialize()
        await backend.create_workflow_run(WorkflowRun(id="r1", workflow_name="wf", namespace="ns1"))
        await backend.create_workflow_run(WorkflowRun(id="r2", workflow_name="wf", namespace="ns2"))
        all_runs = await admin.list_runs_all_namespaces(
            namespaces=["ns1", "ns2"],
        )
        assert len(all_runs) == 2
        run_ids = {r.id for r in all_runs}
        assert run_ids == {"r1", "r2"}

    def test_namespace_stats_dataclass(self) -> None:
        """NamespaceStats has correct defaults."""
        ns = NamespaceStats(namespace="test")
        assert ns.namespace == "test"
        assert ns.total == 0
        assert ns.pending == 0
        assert ns.extra == {}

    def test_register_and_list_namespaces(self) -> None:
        """register_namespace / unregister_namespace / list_namespaces work."""
        backend = InMemoryBackend()
        admin = GravtoryAdmin(backend, namespaces=["a", "b"])
        assert admin.list_namespaces() == ["a", "b"]

        admin.register_namespace("c")
        assert admin.list_namespaces() == ["a", "b", "c"]

        admin.unregister_namespace("a")
        assert admin.list_namespaces() == ["b", "c"]

    @pytest.mark.asyncio()
    async def test_migrate_namespace_actually_moves_runs(
        self,
        backend: InMemoryBackend,
    ) -> None:
        """migrate_namespace changes run.namespace so runs appear in the new ns."""
        await backend.initialize()
        await backend.create_workflow_run(
            WorkflowRun(id="r1", workflow_name="wf", namespace="old-ns")
        )
        await backend.create_workflow_run(
            WorkflowRun(id="r2", workflow_name="wf", namespace="old-ns")
        )

        admin = GravtoryAdmin(backend, namespaces=["old-ns"])
        count = await admin.migrate_namespace("old-ns", "new-ns")
        assert count == 2

        # Runs should now be in new-ns
        old_runs = await backend.list_workflow_runs(namespace="old-ns")
        new_runs = await backend.list_workflow_runs(namespace="new-ns")
        assert len(old_runs) == 0
        assert len(new_runs) == 2

        # new-ns should be registered
        assert "new-ns" in admin.list_namespaces()


class TestMultiTenancyGapFill:
    """Gap-fill tests for multi-tenancy edge cases."""

    def test_register_duplicate_namespace(self) -> None:
        """Registering a namespace that already exists is idempotent."""
        backend = InMemoryBackend()
        admin = GravtoryAdmin(backend, namespaces=["a"])
        admin.register_namespace("a")
        assert admin.list_namespaces().count("a") == 1

    def test_unregister_nonexistent_namespace(self) -> None:
        """Unregistering a namespace that doesn't exist is a no-op."""
        backend = InMemoryBackend()
        admin = GravtoryAdmin(backend, namespaces=["a"])
        admin.unregister_namespace("nonexistent")
        assert admin.list_namespaces() == ["a"]

    @pytest.mark.asyncio()
    async def test_stats_empty_namespace(self) -> None:
        """Stats for a namespace with no runs returns zeros."""
        backend = InMemoryBackend()
        await backend.initialize()
        admin = GravtoryAdmin(backend, namespaces=["empty"])
        stats = await admin.stats_by_namespace(namespaces=["empty"])
        assert stats["empty"].total == 0
        assert stats["empty"].running == 0
