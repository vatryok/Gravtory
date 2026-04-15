"""Tests for documentation quality and correctness."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_DIR = PROJECT_ROOT / "docs"


class TestReadmeCodeExamples:
    def test_readme_exists(self) -> None:
        """README.md exists at project root."""
        readme = PROJECT_ROOT / "README.md"
        assert readme.exists(), "README.md not found"
        content = readme.read_text()
        assert len(content) > 100, "README.md is too short"

    def test_readme_code_blocks_parse(self) -> None:
        """All Python code blocks in README are valid syntax."""
        readme = PROJECT_ROOT / "README.md"
        content = readme.read_text()

        # Extract Python code blocks
        pattern = r"```python\n(.*?)```"
        blocks = re.findall(pattern, content, re.DOTALL)
        assert len(blocks) > 0, "No Python code blocks found in README"

        for i, block in enumerate(blocks):
            # Skip blocks that are clearly shell commands (pip install)
            if block.strip().startswith("pip "):
                continue
            try:
                ast.parse(block)
            except SyntaxError as e:
                pytest.fail(
                    f"README code block {i + 1} has syntax error: {e}\nCode:\n{block[:200]}..."
                )

    def test_readme_has_required_sections(self) -> None:
        """README contains key sections."""
        readme = PROJECT_ROOT / "README.md"
        content = readme.read_text()

        required_sections = [
            "Quick Start",
            "Features",
            "Installation",
            "Backends",
            "License",
        ]
        for section in required_sections:
            assert section in content, f"README missing section: {section}"


class TestDocumentation:
    def test_all_nav_pages_exist(self) -> None:
        """All pages referenced in mkdocs.yml exist."""
        mkdocs_yml = PROJECT_ROOT / "mkdocs.yml"
        if not mkdocs_yml.exists():
            pytest.skip("mkdocs.yml not found")

        content = mkdocs_yml.read_text()
        # Extract .md file references
        md_files = re.findall(r":\s*(\S+\.md)", content)
        assert len(md_files) > 0, "No .md references found in mkdocs.yml"

        missing = []
        for md_file in md_files:
            full_path = DOCS_DIR / md_file
            if not full_path.exists():
                missing.append(md_file)

        assert not missing, f"Missing doc pages: {missing}"

    def test_no_stub_guide_pages(self) -> None:
        """Guide pages should have real content, not stubs."""
        guides_dir = DOCS_DIR / "guides"
        if not guides_dir.exists():
            pytest.skip("docs/guides not found")

        stubs = []
        for md_file in guides_dir.glob("*.md"):
            content = md_file.read_text()
            if "*Guide coming soon.*" in content:
                stubs.append(md_file.name)

        assert not stubs, f"Stub guide pages: {stubs}"

    def test_changelog_exists(self) -> None:
        """CHANGELOG.md exists at project root."""
        changelog = PROJECT_ROOT / "CHANGELOG.md"
        assert changelog.exists(), "CHANGELOG.md not found"
        content = changelog.read_text()
        assert "1.0.0" in content, "CHANGELOG missing 1.0.0 entry"

    def test_security_md_exists(self) -> None:
        """SECURITY.md exists at project root."""
        security = PROJECT_ROOT / "SECURITY.md"
        assert security.exists(), "SECURITY.md not found"
        content = security.read_text()
        assert "Reporting" in content or "reporting" in content, (
            "SECURITY.md missing vulnerability reporting section"
        )
