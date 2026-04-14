"""Tests for version management and consistency."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TestVersionFormat:
    def test_version_string_format(self) -> None:
        """__version__ matches X.Y.Z semver format."""
        from gravtory._version import __version__

        assert re.match(r"^\d+\.\d+\.\d+", __version__), (
            f"Version '{__version__}' does not match semver format"
        )

    def test_version_accessible_from_package(self) -> None:
        """Version is accessible via gravtory.__version__."""
        import gravtory

        assert hasattr(gravtory, "__version__")
        assert isinstance(gravtory.__version__, str)
        assert len(gravtory.__version__) > 0


class TestVersionConsistency:
    def test_version_in_pyproject_matches_init(self) -> None:
        """pyproject.toml version matches _version.py.

        Handles both static ``version = "X.Y.Z"`` and dynamic versions
        sourced from ``[tool.hatch.version] path = ...``.
        """
        from gravtory._version import __version__

        pyproject = PROJECT_ROOT / "pyproject.toml"
        content = pyproject.read_text()
        # Try static version first
        match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        if match is not None:
            pyproject_version = match.group(1)
            assert pyproject_version == __version__, (
                f"pyproject.toml version '{pyproject_version}' != _version.py version '{__version__}'"
            )
        else:
            # Dynamic version -- verify pyproject.toml declares it and the
            # hatch source path points to the actual _version.py file.
            assert "dynamic" in content and '"version"' in content, (
                "pyproject.toml has no static or dynamic version declaration"
            )
            path_match = re.search(r'^\s*path\s*=\s*"([^"]+_version\.py)"', content, re.MULTILINE)
            assert path_match is not None, (
                "pyproject.toml uses dynamic version but no [tool.hatch.version] path found"
            )
            version_file = PROJECT_ROOT / path_match.group(1)
            assert version_file.exists(), f"Version source file {version_file} does not exist"
            version_content = version_file.read_text()
            assert __version__ in version_content, (
                f"_version.py does not contain expected version '{__version__}'"
            )
