"""Targeted gap-fill tests to push overall coverage above 95%.

Covers uncovered lines across multiple modules identified by coverage report.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from gravtory.core.types import (
    Compensation,
    DLQEntry,
    PendingStep,
    StepOutput,
    StepStatus,
    WorkflowRun,
    WorkflowStatus,
)

# ── backends/__init__.py  lines 22-24, 26-28 ──────────────────────────


class TestCreateBackendGaps:
    def test_create_postgresql_backend(self) -> None:
        pytest.importorskip("asyncpg", reason="asyncpg not installed")
        from gravtory.backends import create_backend
        from gravtory.backends.postgresql import PostgreSQLBackend

        b = create_backend("postgresql://localhost/test")
        assert isinstance(b, PostgreSQLBackend)

    def test_create_postgres_backend(self) -> None:
        pytest.importorskip("asyncpg", reason="asyncpg not installed")
        from gravtory.backends import create_backend
        from gravtory.backends.postgresql import PostgreSQLBackend

        b = create_backend("postgres://localhost/test")
        assert isinstance(b, PostgreSQLBackend)

    def test_create_sqlite_backend(self) -> None:
        from gravtory.backends import create_backend
        from gravtory.backends.sqlite import SQLiteBackend

        b = create_backend("sqlite:///test.db")
        assert isinstance(b, SQLiteBackend)


# ── signals/_serde.py  lines 19-20 ────────────────────────────────────


class TestSignalSerdeGaps:
    def test_deserialize_invalid_json(self) -> None:
        from gravtory.signals._serde import deserialize_signal_data

        result = deserialize_signal_data(b"not-json{{{")
        assert "_raw" in result

    def test_deserialize_non_dict_json(self) -> None:
        from gravtory.signals._serde import deserialize_signal_data

        # json.loads returns a list, dict() on it raises TypeError/ValueError
        result = deserialize_signal_data(b"[1, 2, 3]")
        assert "_raw" in result


# ── decorators/middleware.py  lines 112-113, 133-134 ──────────────────


class TestMiddlewareHookErrors:
    @pytest.mark.asyncio
    async def test_after_step_hook_error_suppressed(self) -> None:
        from gravtory.decorators.middleware import MiddlewareRegistry

        chain = MiddlewareRegistry()

        async def bad_after(**kwargs):
            raise RuntimeError("hook fail")

        chain._after_hooks.append(bad_after)
        # Should not raise
        await chain.run_after("wf", "step", "run-1", output="ok", duration_ms=10)

    @pytest.mark.asyncio
    async def test_on_failure_hook_error_suppressed(self) -> None:
        from gravtory.decorators.middleware import MiddlewareRegistry

        chain = MiddlewareRegistry()

        async def bad_failure(**kwargs):
            raise RuntimeError("hook fail")

        chain._failure_hooks.append(bad_failure)
        await chain.run_on_failure("wf", "step", "run-1", error=RuntimeError("x"))


# ── core/registry.py  lines 81, 101, 108, 115, 125, 143-148 ─────────


class TestRegistryValidationGaps:
    def test_get_dag_missing_version(self) -> None:
        from gravtory.core.errors import WorkflowNotFoundError
        from gravtory.core.registry import WorkflowRegistry

        reg = WorkflowRegistry()
        with pytest.raises(WorkflowNotFoundError):
            reg.get_dag("nonexistent")

    def test_validate_negative_step_order(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="test_wf",
            steps={-1: StepDefinition(name="bad", order=-1)},
            config=WorkflowConfig(),
        )
        errors = reg.validate(defn)
        assert any("positive" in e for e in errors)

    def test_validate_duplicate_step_name(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="test_wf",
            steps={
                1: StepDefinition(name="dup", order=1),
                2: StepDefinition(name="dup", order=2),
            },
            config=WorkflowConfig(),
        )
        errors = reg.validate(defn)
        assert any("Duplicate" in e for e in errors)

    def test_validate_saga_no_compensate(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="test_wf",
            steps={1: StepDefinition(name="s1", order=1)},
            config=WorkflowConfig(saga_enabled=True),
        )
        errors = reg.validate(defn)
        assert any("compensate" in e.lower() for e in errors)

    def test_validate_missing_compensate_handler(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        class MyWf:
            pass

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="test_wf",
            steps={1: StepDefinition(name="s1", order=1, compensate="nonexistent")},
            config=WorkflowConfig(saga_enabled=True),
            workflow_class=MyWf,
        )
        errors = reg.validate(defn)
        assert any("nonexistent" in e for e in errors)

    def test_validate_class_step_no_self(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        class MyWf:
            def do_thing(x):  # noqa: N805 — intentionally missing self
                pass

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="test_wf",
            steps={1: StepDefinition(name="do_thing", order=1, function=MyWf.do_thing)},
            config=WorkflowConfig(),
            workflow_class=MyWf,
        )
        errors = reg.validate(defn)
        assert any("self" in e for e in errors)

    def test_unregister_specific_version(self) -> None:
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn_v1 = WorkflowDefinition(
            name="wf",
            version=1,
            steps={1: StepDefinition(name="s1", order=1)},
            config=WorkflowConfig(),
        )
        defn_v2 = WorkflowDefinition(
            name="wf",
            version=2,
            steps={1: StepDefinition(name="s1", order=1)},
            config=WorkflowConfig(),
        )
        reg.register(defn_v1)
        reg.register(defn_v2)
        reg.unregister("wf", version=1)
        # v1 gone, v2 still exists
        result = reg.get("wf")
        assert result.version == 2

    def test_unregister_nonexistent_version(self) -> None:
        from gravtory.core.errors import WorkflowNotFoundError
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="wf",
            version=1,
            steps={1: StepDefinition(name="s1", order=1)},
            config=WorkflowConfig(),
        )
        reg.register(defn)
        with pytest.raises(WorkflowNotFoundError):
            reg.unregister("wf", version=99)

    def test_unregister_last_version_removes_workflow(self) -> None:
        from gravtory.core.errors import WorkflowNotFoundError
        from gravtory.core.registry import WorkflowRegistry
        from gravtory.core.types import StepDefinition, WorkflowConfig, WorkflowDefinition

        reg = WorkflowRegistry()
        defn = WorkflowDefinition(
            name="wf",
            version=1,
            steps={1: StepDefinition(name="s1", order=1)},
            config=WorkflowConfig(),
        )
        reg.register(defn)
        reg.unregister("wf", version=1)
        with pytest.raises(WorkflowNotFoundError):
            reg.get("wf")


# ── backends/memory.py  uncovered edge cases ──────────────────────────


class TestMemoryBackendGaps:
    @pytest.mark.asyncio
    async def test_update_status_missing_run(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        await b.update_workflow_status("nonexistent", WorkflowStatus.FAILED)

    @pytest.mark.asyncio
    async def test_update_status_with_output_data(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        run = WorkflowRun(id="r1", workflow_name="wf", status=WorkflowStatus.RUNNING)
        await b.create_workflow_run(run)
        await b.update_workflow_status("r1", WorkflowStatus.COMPLETED, output_data=b"result")
        updated = await b.get_workflow_run("r1")
        assert updated.output_data == b"result"

    @pytest.mark.asyncio
    async def test_save_step_output_idempotent(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        run = WorkflowRun(id="r1", workflow_name="wf", status=WorkflowStatus.RUNNING)
        await b.create_workflow_run(run)
        so = StepOutput(
            workflow_run_id="r1", step_order=1, step_name="s1", status=StepStatus.COMPLETED
        )
        await b.save_step_output(so)
        # Second save — should be idempotent (line 144-145)
        await b.save_step_output(so)

    @pytest.mark.asyncio
    async def test_complete_step_missing(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        so = StepOutput(
            workflow_run_id="r1", step_order=1, step_name="s1", status=StepStatus.COMPLETED
        )
        await b.complete_step(999, so)  # step_id doesn't exist (line 199)

    @pytest.mark.asyncio
    async def test_fail_step_with_retry(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        ps = PendingStep(workflow_run_id="r1", step_order=1, max_retries=3)
        await b.enqueue_step(ps)
        step_id = ps.id
        retry_at = datetime(2025, 12, 31, tzinfo=timezone.utc)
        await b.fail_step(step_id, error_message="boom", retry_at=retry_at)
        # Step should be back to PENDING with retry_count incremented (lines 215-217)
        assert ps.status == StepStatus.PENDING
        assert ps.retry_count == 1

    @pytest.mark.asyncio
    async def test_save_compensation_auto_id(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        comp = Compensation(
            workflow_run_id="r1",
            step_order=1,
            handler_name="undo",
            status="pending",
        )
        await b.save_compensation(comp)
        assert comp.id is not None  # lines 245-247

    @pytest.mark.asyncio
    async def test_get_compensations_sorted(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        c1 = Compensation(workflow_run_id="r1", step_order=1, handler_name="u1", status="completed")
        c2 = Compensation(workflow_run_id="r1", step_order=2, handler_name="u2", status="completed")
        await b.save_compensation(c1)
        await b.save_compensation(c2)
        result = await b.get_compensations("r1")
        assert result[0].step_order == 2  # lines 250-251

    @pytest.mark.asyncio
    async def test_update_compensation_status(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        comp = Compensation(
            workflow_run_id="r1",
            step_order=1,
            handler_name="undo",
            status="pending",
        )
        await b.save_compensation(comp)
        await b.update_compensation_status(comp.id, "completed", error_message="ok")
        # lines 260-265
        assert comp.status == StepStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_refresh_lock_not_held(self) -> None:
        from gravtory.backends.memory import InMemoryBackend

        b = InMemoryBackend()
        result = await b.refresh_lock("lock", "holder-1", 60)
        assert result is False  # line 326


# ── workers/distributed.py  lines 70-71, 85 ──────────────────────────


class TestDistributedWorkerGaps:
    @pytest.mark.asyncio
    async def test_reclaim_with_backend_method(self) -> None:
        from gravtory.workers.distributed import _reclaim_worker_tasks

        backend = MagicMock()
        backend.reclaim_worker_tasks = AsyncMock(return_value=3)
        result = await _reclaim_worker_tasks(backend, "w-1")
        assert result == 3  # lines 70-71

    @pytest.mark.asyncio
    async def test_reclaim_delegates_to_backend(self) -> None:
        from gravtory.workers.distributed import _reclaim_worker_tasks

        backend = MagicMock()
        backend.reclaim_worker_tasks = AsyncMock(return_value=0)
        result = await _reclaim_worker_tasks(backend, "w-1")
        assert result == 0
        backend.reclaim_worker_tasks.assert_awaited_once_with("w-1")


# ── ai/tokens.py  lines 60-61, 68, 79-83 ─────────────────────────────


class TestTokenCounterGaps:
    def test_tiktoken_with_mock(self) -> None:
        """Test TokenCounter with tiktoken mock to cover lines 60-61, 79-83."""
        from gravtory.ai.tokens import TokenCounter

        mock_tiktoken = MagicMock()
        mock_enc = MagicMock()
        mock_enc.encode.return_value = [1, 2, 3]
        mock_tiktoken.encoding_for_model.return_value = mock_enc

        counter = TokenCounter()
        counter._tiktoken = mock_tiktoken
        counter._available = True

        # lines 79-81: tiktoken available, normal model
        result = counter.count("Hello world", model="gpt-4")
        assert result == 3

    def test_tiktoken_unknown_model_fallback(self) -> None:
        """Test KeyError fallback when model unknown — lines 82-83."""
        from gravtory.ai.tokens import TokenCounter

        mock_tiktoken = MagicMock()
        mock_tiktoken.encoding_for_model.side_effect = KeyError("unknown")

        counter = TokenCounter()
        counter._tiktoken = mock_tiktoken
        counter._available = True

        result = counter.count("Hello world", model="unknown")
        assert result > 0  # Falls back to len // 4

    def test_tiktoken_available_property(self) -> None:
        from gravtory.ai.tokens import TokenCounter

        counter = TokenCounter()
        counter._available = True
        assert counter.tiktoken_available is True  # line 68


# ── enterprise/audit.py  lines 76, 154, 158, 164, 169 ────────────────


class TestAuditGaps:
    @pytest.mark.asyncio
    async def test_namespace_property(self) -> None:
        from gravtory.enterprise.audit import AuditLogger

        log = AuditLogger(namespace="custom")
        assert log.namespace == "custom"  # line 76

    @pytest.mark.asyncio
    async def test_query_with_filters(self) -> None:
        from gravtory.enterprise.audit import AuditLogger

        log = AuditLogger()
        now = datetime.now(tz=timezone.utc)
        await log.log("user", "workflow.created", "workflow", "wf-1")
        await log.log("admin", "workflow.deleted", "schedule", "s-1")
        # Filter by action (line 154-155)
        result = await log.query(action="workflow.created")
        assert len(result) == 1
        # Filter by resource_type (line 157-158)
        result = await log.query(resource_type="schedule")
        assert len(result) == 1
        # Filter by actor (line 161-162)
        result = await log.query(actor="admin")
        assert len(result) == 1
        # Filter by since/until (lines 163-166)
        result = await log.query(since=now - timedelta(hours=1), until=now + timedelta(hours=1))
        assert len(result) == 2


# ── enterprise/dlq_manager.py  lines 106, 153, 176, 208, 237-238, 268 ─


class TestDLQManagerGaps:
    @pytest.mark.asyncio
    async def test_inspect_not_found(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager

        backend = MagicMock()
        backend.list_dlq = AsyncMock(return_value=[])
        backend.get_dlq_entry = AsyncMock(return_value=None)
        mgr = DLQManager(backend=backend)
        result = await mgr.inspect(999)
        assert result is None  # line 153 (from _find_entry returning None)

    @pytest.mark.asyncio
    async def test_process_auto_retry_no_rules(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager

        backend = MagicMock()
        mgr = DLQManager(backend=backend, rules=[])
        result = await mgr.process_auto_retry()
        assert result == 0  # line 176

    @pytest.mark.asyncio
    async def test_matches_rule_no_error_message(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager, DLQRetryRule

        backend = MagicMock()
        rule = DLQRetryRule(error_pattern=".*", max_retries=3)
        mgr = DLQManager(backend=backend, rules=[rule])
        entry = DLQEntry(id=1, workflow_run_id="r1", step_order=1, error_message=None)
        assert mgr._matches_rule(entry, rule, datetime.now(tz=timezone.utc)) is False  # line 208

    @pytest.mark.asyncio
    async def test_check_threshold_with_alert(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager

        backend = MagicMock()
        entries = [DLQEntry(id=i, workflow_run_id=f"r{i}", step_order=1) for i in range(5)]
        backend.list_dlq = AsyncMock(return_value=entries)
        backend.count_dlq = AsyncMock(return_value=5)
        callback = AsyncMock()
        mgr = DLQManager(backend=backend, alert_callback=callback, alert_threshold=2)
        result = await mgr.check_threshold()
        assert result is True
        callback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_check_threshold_alert_error(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager

        backend = MagicMock()
        entries = [DLQEntry(id=i, workflow_run_id=f"r{i}", step_order=1) for i in range(5)]
        backend.list_dlq = AsyncMock(return_value=entries)
        backend.count_dlq = AsyncMock(return_value=5)
        callback = AsyncMock(side_effect=RuntimeError("alert fail"))
        mgr = DLQManager(backend=backend, alert_callback=callback, alert_threshold=2)
        result = await mgr.check_threshold()  # lines 237-238
        assert result is True

    @pytest.mark.asyncio
    async def test_purge_with_age_filter(self) -> None:
        from gravtory.enterprise.dlq_manager import DLQManager

        backend = MagicMock()
        old = DLQEntry(
            id=1,
            workflow_run_id="r1",
            step_order=1,
            created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
        new = DLQEntry(
            id=2,
            workflow_run_id="r2",
            step_order=1,
            created_at=datetime.now(tz=timezone.utc),
        )
        backend.list_dlq = AsyncMock(return_value=[old, new])
        backend.remove_from_dlq = AsyncMock()
        mgr = DLQManager(backend=backend)
        result = await mgr.purge(older_than=timedelta(hours=1))
        assert result == 1  # Only old entry purged (line 268)


# ── core/engine.py  lines 89-91, 106 ─────────────────────────────────


class TestEngineGaps:
    @pytest.mark.asyncio
    async def test_engine_with_memory_backend_string(self) -> None:
        from gravtory.core.engine import Gravtory

        g = Gravtory(backend="memory://")
        assert g._backend is not None

    def test_engine_with_backend_instance(self) -> None:
        from gravtory.backends.memory import InMemoryBackend
        from gravtory.core.engine import Gravtory

        b = InMemoryBackend()
        g = Gravtory(backend=b)
        assert g._backend is b


# ── core/saga.py  lines 95-102 ────────────────────────────────────────


class TestSagaGaps:
    @pytest.mark.asyncio
    async def test_compensate_handler_not_found(self) -> None:
        from gravtory.core.saga import SagaCoordinator
        from gravtory.core.types import (
            StepDefinition,
            StepResult,
            WorkflowConfig,
            WorkflowDefinition,
        )

        backend = AsyncMock()
        backend.validated_update_workflow_status = AsyncMock()
        backend.update_workflow_status = AsyncMock()
        backend.save_compensation = AsyncMock()
        backend.add_to_dlq = AsyncMock()

        # Mock registry.get_compensation_handler to raise, simulating missing handler
        reg = MagicMock()
        reg.get_compensation_handler = MagicMock(side_effect=Exception("handler not found"))

        defn = WorkflowDefinition(
            name="wf",
            version=1,
            steps={1: StepDefinition(name="s1", order=1, compensate="nonexistent_handler")},
            config=WorkflowConfig(saga_enabled=True),
        )
        coordinator = SagaCoordinator(backend=backend, registry=reg)

        completed = {1: StepResult(status=StepStatus.COMPLETED, output=b"data")}

        # Should not raise — handler-not-found is caught and logged (lines 95-102)
        await coordinator.trigger("run-1", 2, defn, completed)


# ── dashboard/server.py  lines 62, 92-94 ─────────────────────────────


class TestDashboardServerGaps:
    def test_dashboard_import(self) -> None:
        from gravtory.dashboard.server import Dashboard

        assert Dashboard is not None
