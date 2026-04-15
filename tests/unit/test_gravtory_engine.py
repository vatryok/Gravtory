"""Tests for core.engine — Gravtory main class lifecycle, run, inspect, etc."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.engine import Gravtory
from gravtory.core.errors import ConfigurationError, GravtoryError
from gravtory.core.types import (
    StepDefinition,
    WorkflowConfig,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)
from gravtory.decorators.workflow import WorkflowProxy


@pytest.fixture
async def backend() -> InMemoryBackend:
    b = InMemoryBackend()
    await b.initialize()
    return b


def _make_proxy(name: str = "test-wf") -> WorkflowProxy:
    step1 = StepDefinition(name="step_a", order=1, retries=0, depends_on=[])
    definition = WorkflowDefinition(
        name=name,
        version=1,
        steps={1: step1},
        config=WorkflowConfig(),
    )
    proxy = MagicMock(spec=WorkflowProxy)
    proxy.definition = definition
    proxy.generate_id = MagicMock(return_value=f"{name}-run-1")
    return proxy


class TestGravtoryInit:
    def test_memory_backend_string(self) -> None:
        g = Gravtory("memory://")
        assert g._backend is not None

    def test_memory_shorthand(self) -> None:
        g = Gravtory(":memory:")
        assert g._backend is not None

    def test_backend_object(self) -> None:
        b = InMemoryBackend()
        g = Gravtory(b)
        assert g._backend is b

    def test_default_properties(self) -> None:
        g = Gravtory(":memory:")
        assert g._namespace == "default"
        assert g._serializer == "json"
        assert g._compression is None
        assert g._encryption_key is None
        assert g._scheduler_enabled is False
        assert g._dashboard_enabled is False
        assert not g._started

    def test_custom_properties(self) -> None:
        pytest.importorskip("cryptography", reason="cryptography not installed")
        g = Gravtory(
            ":memory:",
            workers=4,
            namespace="prod",
            serializer="msgpack",
            compression="gzip",
            encryption_key="secret",
            scheduler=True,
            dashboard=True,
            dashboard_port=9999,
            table_prefix="gv_",
        )
        assert g._namespace == "prod"
        assert g._workers_count == 4
        assert g._serializer == "msgpack"
        assert g._compression == "gzip"
        assert g._encryption_key == "secret"
        assert g._scheduler_enabled is True
        assert g._dashboard_enabled is True
        assert g._dashboard_port == 9999
        assert g._table_prefix == "gv_"

    def test_registry_property(self) -> None:
        g = Gravtory(":memory:")
        assert g.registry is not None

    def test_backend_property(self) -> None:
        g = Gravtory(":memory:")
        assert g.backend is not None


class TestGravtoryLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_shutdown(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        assert g._started is True
        await g.shutdown()
        assert g._started is False

    @pytest.mark.asyncio
    async def test_start_registers_pending_workflows(self) -> None:
        g = Gravtory(":memory:")
        proxy = _make_proxy("my-wf")
        g._pending_workflows.append(proxy)
        await g.start()
        # Workflow should be registered
        defn = g.registry.get("my-wf")
        assert defn.name == "my-wf"
        await g.shutdown()


class TestGravtoryRun:
    @pytest.mark.asyncio
    async def test_run_foreground(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        proxy = _make_proxy("fg-wf")

        # Mock the execution engine
        mock_run = WorkflowRun(
            id="fg-wf-run-1",
            workflow_name="fg-wf",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
        )
        with patch.object(
            g._engine, "execute_workflow", new_callable=AsyncMock, return_value=mock_run
        ):
            result = await g.run(proxy)
            assert isinstance(result, WorkflowRun)
            assert result.status == WorkflowStatus.COMPLETED
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_run_background(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        proxy = _make_proxy("bg-wf")

        result = await g.run(proxy, background=True)
        assert isinstance(result, str)
        assert result == "bg-wf-run-1"
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_run_existing_completed(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        proxy = _make_proxy("existing-wf")

        # Pre-create a completed run
        run = WorkflowRun(
            id="existing-wf-run-1",
            workflow_name="existing-wf",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
        )
        await g.backend.create_workflow_run(run)
        await g.backend.update_workflow_status("existing-wf-run-1", WorkflowStatus.COMPLETED)

        result = await g.run(proxy)
        assert isinstance(result, WorkflowRun)
        assert result.status == WorkflowStatus.COMPLETED
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_run_existing_running_resumes(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        proxy = _make_proxy("resume-wf")

        # Pre-create a running run
        run = WorkflowRun(
            id="resume-wf-run-1",
            workflow_name="resume-wf",
            workflow_version=1,
            status=WorkflowStatus.RUNNING,
        )
        await g.backend.create_workflow_run(run)
        await g.backend.update_workflow_status("resume-wf-run-1", WorkflowStatus.RUNNING)

        mock_run = WorkflowRun(
            id="resume-wf-run-1",
            workflow_name="resume-wf",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
        )
        with patch.object(
            g._engine, "execute_workflow", new_callable=AsyncMock, return_value=mock_run
        ):
            result = await g.run(proxy)
            assert isinstance(result, WorkflowRun)
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_run_auto_registers_workflow(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        proxy = _make_proxy("auto-reg-wf")

        mock_run = WorkflowRun(
            id="auto-reg-wf-run-1",
            workflow_name="auto-reg-wf",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
        )
        with patch.object(
            g._engine, "execute_workflow", new_callable=AsyncMock, return_value=mock_run
        ):
            result = await g.run(proxy)
            assert isinstance(result, WorkflowRun)
        # Should now be in registry
        defn = g.registry.get("auto-reg-wf")
        assert defn.name == "auto-reg-wf"
        await g.shutdown()


class TestGravtoryRunSync:
    def test_run_sync_not_supported_background(self) -> None:
        g = Gravtory(":memory:")
        proxy = _make_proxy("sync-wf")

        with patch.object(Gravtory, "run", new_callable=AsyncMock, return_value="run-id-str"):
            with pytest.raises(GravtoryError, match="background"):
                g.run_sync(proxy)


class TestGravtoryInspect:
    @pytest.mark.asyncio
    async def test_inspect_found(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        run = WorkflowRun(
            id="inspect-1",
            workflow_name="wf",
            workflow_version=1,
            status=WorkflowStatus.COMPLETED,
        )
        await g.backend.create_workflow_run(run)
        result = await g.inspect("inspect-1")
        assert result.id == "inspect-1"
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_inspect_not_found(self) -> None:
        from gravtory.core.errors import WorkflowRunNotFoundError

        g = Gravtory(":memory:")
        await g.start()
        with pytest.raises(WorkflowRunNotFoundError):
            await g.inspect("nonexistent")
        await g.shutdown()


class TestGravtoryList:
    @pytest.mark.asyncio
    async def test_list_all(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        run = WorkflowRun(
            id="list-1",
            workflow_name="wf",
            workflow_version=1,
            namespace="default",
            status=WorkflowStatus.COMPLETED,
        )
        await g.backend.create_workflow_run(run)
        runs = await g.list()
        assert len(runs) >= 1
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_list_with_status_filter(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        run = WorkflowRun(
            id="list-2",
            workflow_name="wf",
            workflow_version=1,
            namespace="default",
            status=WorkflowStatus.FAILED,
        )
        await g.backend.create_workflow_run(run)
        await g.backend.update_workflow_status("list-2", WorkflowStatus.FAILED)
        runs = await g.list(status="failed")
        assert any(r.id == "list-2" for r in runs)
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_count(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        c = await g.count()
        assert c >= 0
        await g.shutdown()


class TestGravtorySignal:
    @pytest.mark.asyncio
    async def test_send_signal(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        await g.signal("run-1", "approval", b"data")
        await g.shutdown()

    @pytest.mark.asyncio
    async def test_send_signal_non_bytes(self) -> None:
        g = Gravtory(":memory:")
        await g.start()
        await g.signal("run-1", "approval", "string-data")
        await g.shutdown()


class TestGravtoryResolveProxy:
    def test_resolve_workflow_proxy(self) -> None:
        g = Gravtory(":memory:")
        proxy = _make_proxy()
        result = g._resolve_proxy(proxy)
        assert result is proxy

    def test_resolve_object_with_definition(self) -> None:
        g = Gravtory(":memory:")
        obj = MagicMock()
        obj.definition = MagicMock()
        result = g._resolve_proxy(obj)
        assert result is obj

    def test_resolve_invalid_raises(self) -> None:
        g = Gravtory(":memory:")
        with pytest.raises(ConfigurationError, match="WorkflowProxy"):
            g._resolve_proxy(42)  # type: ignore[arg-type]


class TestGravtoryDecorators:
    def test_workflow_decorator(self) -> None:
        g = Gravtory(":memory:")

        @g.workflow(id="my-wf")
        class MyWorkflow:
            pass

        assert len(g._pending_workflows) == 1

    def test_step_decorator(self) -> None:
        g = Gravtory(":memory:")
        decorated = g.step(name="my_step", order=1)
        assert callable(decorated)
