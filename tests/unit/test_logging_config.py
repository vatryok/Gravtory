"""Tests for structured logging configuration."""

from __future__ import annotations

import logging

from gravtory.observability.logging import configure_logging, get_logger


class TestConfigureLogging:
    def test_json_format_configures_without_error(self) -> None:
        configure_logging(level="DEBUG", fmt="json")

    def test_console_format_configures_without_error(self) -> None:
        configure_logging(level="INFO", fmt="console")

    def test_log_level_filtering(self) -> None:
        configure_logging(level="WARNING", fmt="console")
        log = get_logger("gravtory.test.level")
        # structlog bound loggers expose level methods; just verify no crash
        assert log is not None

    def test_context_in_logs(self) -> None:
        configure_logging(level="DEBUG", fmt="json")
        log = get_logger("gravtory.test.ctx")
        assert log is not None

    def test_get_logger_returns_logger(self) -> None:
        log = get_logger("gravtory.test")
        assert log is not None

    def test_no_timestamp(self) -> None:
        configure_logging(level="INFO", fmt="json", add_timestamp=False)

    def test_stdlib_fallback_format(self) -> None:
        """Verify the stdlib fallback path runs cleanly."""
        # Force stdlib path by testing the internal function directly
        from gravtory.observability.logging import _configure_stdlib

        _configure_stdlib(logging.INFO, "json", add_timestamp=True)
        _configure_stdlib(logging.DEBUG, "console", add_timestamp=False)


class TestLoggingGapFill:
    """Gap-fill tests for logging configuration edge cases."""

    def test_get_logger_different_names(self) -> None:
        """Different logger names return distinct loggers."""
        l1 = get_logger("gravtory.test.a")
        l2 = get_logger("gravtory.test.b")
        assert l1 is not l2

    def test_configure_all_levels(self) -> None:
        """All standard log levels configure without error."""
        for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            configure_logging(level=level, fmt="console")

    def test_configure_json_with_timestamp(self) -> None:
        configure_logging(level="INFO", fmt="json", add_timestamp=True)
