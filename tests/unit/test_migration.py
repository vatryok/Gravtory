"""Tests for backends.migration — SchemaMigrator coverage."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gravtory.backends.migration import _MIGRATIONS, SchemaMigrator
from gravtory.backends.schema import CURRENT_SCHEMA_VERSION
from gravtory.core.errors import ConfigurationError


@pytest.fixture
def mock_backend() -> MagicMock:
    return MagicMock()


class TestSchemaMigrator:
    @pytest.mark.asyncio
    async def test_fresh_install(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        with patch.object(migrator, "_get_current_version", return_value=None):
            result = await migrator.check_and_migrate()
        assert result == CURRENT_SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_already_at_current(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        with patch.object(
            migrator,
            "_get_current_version",
            return_value=CURRENT_SCHEMA_VERSION,
        ):
            result = await migrator.check_and_migrate()
        assert result == CURRENT_SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_newer_than_current_raises(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        with (
            patch.object(
                migrator,
                "_get_current_version",
                return_value=CURRENT_SCHEMA_VERSION + 5,
            ),
            pytest.raises(ConfigurationError, match="newer"),
        ):
            await migrator.check_and_migrate()

    @pytest.mark.asyncio
    async def test_migration_needed(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        old_version = CURRENT_SCHEMA_VERSION - 1

        migration_fn = AsyncMock()
        with (
            patch.object(
                migrator,
                "_get_current_version",
                return_value=old_version,
            ),
            patch.dict(_MIGRATIONS, {old_version: migration_fn}),
        ):
            result = await migrator.check_and_migrate()
        assert result == CURRENT_SCHEMA_VERSION
        migration_fn.assert_awaited_once_with(mock_backend)

    @pytest.mark.asyncio
    async def test_missing_migration_raises(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        old_version = CURRENT_SCHEMA_VERSION - 1

        with (
            patch.object(
                migrator,
                "_get_current_version",
                return_value=old_version,
            ),
            patch.dict(_MIGRATIONS, {}, clear=True),
        ):
            with pytest.raises(ConfigurationError, match="No migration path"):
                await migrator.check_and_migrate()

    @pytest.mark.asyncio
    async def test_get_current_version_returns_current(self, mock_backend: MagicMock) -> None:
        migrator = SchemaMigrator(mock_backend)
        result = await migrator._get_current_version()
        assert result == CURRENT_SCHEMA_VERSION
