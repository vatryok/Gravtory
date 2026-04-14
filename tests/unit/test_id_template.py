"""Unit tests for workflow ID templating."""

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.core.id_template import generate_workflow_id


class TestGenerateWorkflowId:
    def test_simple_substitution(self) -> None:
        assert generate_workflow_id("order-{order_id}", order_id="abc") == "order-abc"

    def test_multiple_variables(self) -> None:
        result = generate_workflow_id("batch-{date}-{batch}", date="2025-03", batch=1)
        assert result == "batch-2025-03-1"

    def test_no_variables(self) -> None:
        assert generate_workflow_id("simple") == "simple"

    def test_missing_variable_error(self) -> None:
        with pytest.raises(ConfigurationError, match="requires parameter"):
            generate_workflow_id("x-{missing}")

    def test_integer_values(self) -> None:
        assert generate_workflow_id("job-{n}", n=42) == "job-42"

    def test_special_characters_preserved(self) -> None:
        assert generate_workflow_id("wf-{id}", id="a/b:c") == "wf-a/b:c"

    def test_empty_value(self) -> None:
        assert generate_workflow_id("wf-{id}", id="") == "wf-"

    def test_extra_kwargs_ignored(self) -> None:
        result = generate_workflow_id("wf-{id}", id="123", extra="ignored")
        assert result == "wf-123"


class TestIdTemplateGapFill:
    """Gap-fill tests for ID template edge cases."""

    def test_multiple_same_variable(self) -> None:
        result = generate_workflow_id("{x}-{x}", x="abc")
        assert result == "abc-abc"

    def test_unicode_values(self) -> None:
        result = generate_workflow_id("wf-{name}", name="日本語")
        assert result == "wf-日本語"

    def test_long_template(self) -> None:
        result = generate_workflow_id(
            "org-{org}-team-{team}-project-{proj}-run-{run}",
            org="acme",
            team="eng",
            proj="api",
            run="42",
        )
        assert result == "org-acme-team-eng-project-api-run-42"
