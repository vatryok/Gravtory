"""E2E test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def e2e_db(tmp_path: Path) -> str:
    """Return a fresh SQLite DSN for E2E tests."""
    return f"sqlite:///{tmp_path / 'e2e.db'}"
