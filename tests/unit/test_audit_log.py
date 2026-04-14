"""Tests for audit logging — AuditLogger."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.enterprise.audit import (
    WORKFLOW_COMPLETED,
    WORKFLOW_CREATED,
    WORKFLOW_FAILED,
    AuditEntry,
    AuditLogger,
)


class TestAuditLog:
    """AuditLogger recording and querying tests."""

    @pytest.fixture()
    def logger(self) -> AuditLogger:
        return AuditLogger(namespace="test-ns")

    @pytest.mark.asyncio()
    async def test_log_entry_created(self, logger: AuditLogger) -> None:
        """Logging an action creates an AuditEntry with correct fields."""
        entry = await logger.log(
            actor="user:alice",
            action=WORKFLOW_CREATED,
            resource_type="workflow",
            resource_id="run-123",
            details={"workflow_name": "OrderWorkflow"},
            ip_address="192.168.1.1",
        )
        assert entry.id == 1
        assert entry.namespace == "test-ns"
        assert entry.actor == "user:alice"
        assert entry.action == WORKFLOW_CREATED
        assert entry.resource_type == "workflow"
        assert entry.resource_id == "run-123"
        assert entry.details == {"workflow_name": "OrderWorkflow"}
        assert entry.ip_address == "192.168.1.1"
        assert entry.timestamp is not None

    @pytest.mark.asyncio()
    async def test_query_by_action(self, logger: AuditLogger) -> None:
        """Querying by action returns only matching entries."""
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r1"
        )
        await logger.log(
            actor="system", action=WORKFLOW_COMPLETED, resource_type="workflow", resource_id="r1"
        )
        await logger.log(
            actor="system", action=WORKFLOW_FAILED, resource_type="workflow", resource_id="r2"
        )

        results = await logger.query(action=WORKFLOW_CREATED)
        assert len(results) == 1
        assert results[0].resource_id == "r1"
        assert results[0].action == WORKFLOW_CREATED

    @pytest.mark.asyncio()
    async def test_query_by_resource(self, logger: AuditLogger) -> None:
        """Querying by resource_id returns only matching entries."""
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r1"
        )
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r2"
        )

        results = await logger.query(resource_id="r2")
        assert len(results) == 1
        assert results[0].resource_id == "r2"

    @pytest.mark.asyncio()
    async def test_query_by_time_range(self, logger: AuditLogger) -> None:
        """Querying by time range filters correctly."""
        now = datetime.now(tz=timezone.utc)
        await logger.log(
            actor="system",
            action=WORKFLOW_CREATED,
            resource_type="workflow",
            resource_id="r1",
        )

        # Since should include the entry
        results = await logger.query(since=now - timedelta(seconds=5))
        assert len(results) == 1

        # Until in the past should exclude
        results = await logger.query(until=now - timedelta(hours=1))
        assert len(results) == 0

    @pytest.mark.asyncio()
    async def test_query_by_actor(self, logger: AuditLogger) -> None:
        """Querying by actor returns only matching entries."""
        await logger.log(
            actor="user:alice", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r1"
        )
        await logger.log(
            actor="user:bob", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r2"
        )

        results = await logger.query(actor="user:bob")
        assert len(results) == 1
        assert results[0].resource_id == "r2"

    @pytest.mark.asyncio()
    async def test_count(self, logger: AuditLogger) -> None:
        """Count returns the number of matching entries."""
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r1"
        )
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r2"
        )
        await logger.log(
            actor="system", action=WORKFLOW_FAILED, resource_type="workflow", resource_id="r3"
        )

        assert await logger.count(action=WORKFLOW_CREATED) == 2
        assert await logger.count() == 3

    @pytest.mark.asyncio()
    async def test_clear(self, logger: AuditLogger) -> None:
        """Clear removes all entries."""
        await logger.log(
            actor="system", action=WORKFLOW_CREATED, resource_type="workflow", resource_id="r1"
        )
        assert await logger.count() == 1
        logger.clear()
        assert await logger.count() == 0

    def test_audit_entry_defaults(self) -> None:
        """AuditEntry has correct defaults."""
        entry = AuditEntry()
        assert entry.id is None
        assert entry.namespace == "default"
        assert entry.actor == "system"
        assert entry.action == ""
        assert entry.details == {}
        assert entry.ip_address is None


class TestAuditLogGapFill:
    """Gap-fill tests for audit log edge cases."""

    @pytest.mark.asyncio()
    async def test_log_increments_id(self) -> None:
        logger = AuditLogger(namespace="test")
        e1 = await logger.log(
            actor="sys", action=WORKFLOW_CREATED, resource_type="wf", resource_id="r1"
        )
        e2 = await logger.log(
            actor="sys", action=WORKFLOW_CREATED, resource_type="wf", resource_id="r2"
        )
        assert e2.id == e1.id + 1

    @pytest.mark.asyncio()
    async def test_query_combined_filters(self) -> None:
        """Query with multiple filters narrows results."""
        logger = AuditLogger(namespace="test")
        await logger.log(
            actor="alice", action=WORKFLOW_CREATED, resource_type="wf", resource_id="r1"
        )
        await logger.log(actor="bob", action=WORKFLOW_CREATED, resource_type="wf", resource_id="r2")
        await logger.log(
            actor="alice", action=WORKFLOW_FAILED, resource_type="wf", resource_id="r3"
        )

        results = await logger.query(actor="alice", action=WORKFLOW_CREATED)
        assert len(results) == 1
        assert results[0].resource_id == "r1"

    @pytest.mark.asyncio()
    async def test_many_entries(self) -> None:
        logger = AuditLogger(namespace="test")
        for i in range(50):
            await logger.log(
                actor="sys", action=WORKFLOW_CREATED, resource_type="wf", resource_id=f"r{i}"
            )
        assert await logger.count() == 50
