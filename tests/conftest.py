"""Root-level conftest.py — shared fixtures for all test suites."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.backends.memory import InMemoryBackend
from gravtory.core.engine import Gravtory
from gravtory.core.execution import ExecutionEngine
from gravtory.core.registry import WorkflowRegistry


@pytest.fixture
def memory_backend() -> InMemoryBackend:
    """Fresh in-memory backend for each test."""
    return InMemoryBackend()


@pytest.fixture
def registry() -> WorkflowRegistry:
    """Fresh workflow registry for each test."""
    return WorkflowRegistry()


@pytest.fixture
def execution_engine(
    registry: WorkflowRegistry, memory_backend: InMemoryBackend
) -> ExecutionEngine:
    """Execution engine with in-memory backend."""
    return ExecutionEngine(registry, memory_backend)


@pytest.fixture
async def grav() -> Any:
    """Gravtory instance using in-memory backend, started and ready."""
    g = Gravtory(":memory:")
    await g.start()
    yield g
    await g.shutdown()
