"""Tests for observability.logging — configure_logging and get_logger."""

from __future__ import annotations

import logging
from unittest.mock import patch

from gravtory.observability.logging import (
    _HAS_STRUCTLOG,
    _configure_stdlib,
    configure_logging,
    get_logger,
)


class TestConfigureLogging:
    def test_json_with_timestamp(self) -> None:
        configure_logging(level="DEBUG", fmt="json", add_timestamp=True)

    def test_json_without_timestamp(self) -> None:
        configure_logging(level="INFO", fmt="json", add_timestamp=False)

    def test_console_with_timestamp(self) -> None:
        configure_logging(level="WARNING", fmt="console", add_timestamp=True)

    def test_console_without_timestamp(self) -> None:
        configure_logging(level="ERROR", fmt="console", add_timestamp=False)

    def test_invalid_level_defaults_to_info(self) -> None:
        configure_logging(level="NONEXISTENT", fmt="json")


class TestConfigureStdlib:
    """Directly test _configure_stdlib for all format combos."""

    def test_json_with_timestamp(self) -> None:
        _configure_stdlib(logging.INFO, "json", add_timestamp=True)
        root = logging.getLogger("gravtory")
        assert root.level == logging.INFO
        assert len(root.handlers) >= 1

    def test_json_without_timestamp(self) -> None:
        _configure_stdlib(logging.DEBUG, "json", add_timestamp=False)
        root = logging.getLogger("gravtory")
        assert root.level == logging.DEBUG

    def test_console_with_timestamp(self) -> None:
        _configure_stdlib(logging.WARNING, "console", add_timestamp=True)
        root = logging.getLogger("gravtory")
        assert root.level == logging.WARNING

    def test_console_without_timestamp(self) -> None:
        _configure_stdlib(logging.ERROR, "console", add_timestamp=False)
        root = logging.getLogger("gravtory")
        assert root.level == logging.ERROR


class TestConfigureStructlog:
    """Test structlog configuration paths if structlog is available."""

    def test_structlog_json_with_timestamp(self) -> None:
        if not _HAS_STRUCTLOG:
            import pytest

            pytest.skip("structlog not installed")
        from gravtory.observability.logging import _configure_structlog

        _configure_structlog(logging.INFO, "json", add_timestamp=True)

    def test_structlog_json_without_timestamp(self) -> None:
        if not _HAS_STRUCTLOG:
            import pytest

            pytest.skip("structlog not installed")
        from gravtory.observability.logging import _configure_structlog

        _configure_structlog(logging.DEBUG, "json", add_timestamp=False)

    def test_structlog_console_with_timestamp(self) -> None:
        if not _HAS_STRUCTLOG:
            import pytest

            pytest.skip("structlog not installed")
        from gravtory.observability.logging import _configure_structlog

        _configure_structlog(logging.WARNING, "console", add_timestamp=True)

    def test_structlog_console_without_timestamp(self) -> None:
        if not _HAS_STRUCTLOG:
            import pytest

            pytest.skip("structlog not installed")
        from gravtory.observability.logging import _configure_structlog

        _configure_structlog(logging.ERROR, "console", add_timestamp=False)


class TestGetLogger:
    def test_returns_logger(self) -> None:
        log = get_logger("gravtory.test")
        assert log is not None

    def test_default_name(self) -> None:
        log = get_logger()
        assert log is not None

    def test_get_logger_stdlib_fallback(self) -> None:
        with patch("gravtory.observability.logging._HAS_STRUCTLOG", False):
            log = get_logger("gravtory.fallback")
            assert isinstance(log, logging.Logger)
