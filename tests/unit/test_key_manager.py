"""Tests for per-namespace encryption key management — KeyManager."""

from __future__ import annotations

import pytest

from gravtory.enterprise.key_manager import KeyManager


class TestKeyManager:
    """KeyManager key resolution and management tests."""

    def test_namespace_key_lookup(self) -> None:
        """Explicit namespace key takes priority."""
        km = KeyManager(
            default_key="default-key",
            namespace_keys={"finance": "finance-key-2025"},
        )
        assert km.get_key("finance") == "finance-key-2025"

    def test_default_key_fallback(self) -> None:
        """Falls back to default_key when no namespace key exists."""
        km = KeyManager(default_key="fallback")
        assert km.get_key("unknown-ns") == "fallback"

    def test_no_key_returns_none(self) -> None:
        """Returns None when no key configured at all."""
        km = KeyManager()
        assert km.get_key("any") is None

    def test_key_provider_dynamic(self) -> None:
        """Dynamic key_provider is called for unknown namespaces."""

        def provider(ns: str) -> str | None:
            if ns.startswith("team-"):
                return f"key-for-{ns}"
            return None

        km = KeyManager(default_key="default", key_provider=provider)
        assert km.get_key("team-alpha") == "key-for-team-alpha"
        assert km.get_key("other") == "default"

    def test_explicit_key_beats_provider(self) -> None:
        """Explicit namespace_keys take priority over key_provider."""
        km = KeyManager(
            namespace_keys={"ns1": "explicit-key"},
            key_provider=lambda ns: "provider-key",
        )
        assert km.get_key("ns1") == "explicit-key"

    def test_set_and_remove_key(self) -> None:
        """set_key and remove_key manage namespace keys."""
        km = KeyManager()
        km.set_key("ns1", "my-key")
        assert km.get_key("ns1") == "my-key"
        km.remove_key("ns1")
        assert km.get_key("ns1") is None

    def test_list_namespaces_with_keys(self) -> None:
        """list_namespaces_with_keys returns sorted list."""
        km = KeyManager(namespace_keys={"beta": "b", "alpha": "a"})
        assert km.list_namespaces_with_keys() == ["alpha", "beta"]

    def test_get_encryptor_with_key(self) -> None:
        """get_encryptor returns an Encryptor when key exists."""
        km = KeyManager(default_key="test-key")
        try:
            enc = km.get_encryptor("any")
            assert enc is not None
            # Verify roundtrip
            ct = enc.encrypt(b"hello")
            pt = enc.decrypt(ct)
            assert pt == b"hello"
        except ImportError:
            pytest.skip("cryptography package not installed")

    def test_get_encryptor_without_key(self) -> None:
        """get_encryptor returns None when no key configured."""
        km = KeyManager()
        assert km.get_encryptor("any") is None


class TestKeyManagerGapFill:
    """Gap-fill tests for key manager edge cases."""

    def test_set_key_overrides_existing(self) -> None:
        km = KeyManager()
        km.set_key("ns1", "old-key")
        km.set_key("ns1", "new-key")
        assert km.get_key("ns1") == "new-key"

    def test_remove_key_no_error_if_missing(self) -> None:
        km = KeyManager()
        km.remove_key("nonexistent")  # Should not raise

    def test_provider_not_called_for_explicit_key(self) -> None:
        called = []

        def provider(ns: str) -> str | None:
            called.append(ns)
            return "provider-key"

        km = KeyManager(namespace_keys={"ns1": "explicit"}, key_provider=provider)
        assert km.get_key("ns1") == "explicit"
        assert "ns1" not in called


class TestRotateKeys:
    """Tests for the rotate_keys function."""

    @pytest.mark.asyncio
    async def test_rotate_keys_basic(self) -> None:
        from gravtory.backends.memory import InMemoryBackend
        from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus

        try:
            from gravtory.serialization.encryption import AES256GCMEncryptor
        except ImportError:
            pytest.skip("cryptography package not installed")

        from gravtory.enterprise.key_manager import rotate_keys

        backend = InMemoryBackend()
        await backend.initialize()

        old_key = "old-encryption-key-1234"
        new_key = "new-encryption-key-5678"
        old_enc = AES256GCMEncryptor(old_key)

        # Create a workflow run with encrypted step output
        run = WorkflowRun(
            id="run-rotate-1",
            workflow_name="wf",
            workflow_version=1,
            namespace="default",
            status=WorkflowStatus.COMPLETED,
        )
        await backend.create_workflow_run(run)
        encrypted_data = old_enc.encrypt(b"secret payload")
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="run-rotate-1",
                step_order=1,
                step_name="s1",
                output_data=encrypted_data,
                status=StepStatus.COMPLETED,
            )
        )

        rotated = await rotate_keys(backend, "default", old_key, new_key)
        assert rotated == 1

        # Verify the data is now encrypted with the new key
        outputs = await backend.get_step_outputs("run-rotate-1")
        new_enc = AES256GCMEncryptor(new_key)
        plaintext = new_enc.decrypt(outputs[0].output_data)
        assert plaintext == b"secret payload"

    @pytest.mark.asyncio
    async def test_rotate_keys_skips_none_output(self) -> None:
        from gravtory.backends.memory import InMemoryBackend
        from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus
        from gravtory.enterprise.key_manager import rotate_keys

        backend = InMemoryBackend()
        await backend.initialize()

        run = WorkflowRun(
            id="run-rotate-2",
            workflow_name="wf",
            workflow_version=1,
            namespace="default",
            status=WorkflowStatus.COMPLETED,
        )
        await backend.create_workflow_run(run)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="run-rotate-2",
                step_order=1,
                step_name="s1",
                output_data=None,
                status=StepStatus.COMPLETED,
            )
        )

        rotated = await rotate_keys(backend, "default", "old", "new")
        assert rotated == 0

    @pytest.mark.asyncio
    async def test_rotate_keys_handles_bad_data(self) -> None:
        from gravtory.backends.memory import InMemoryBackend
        from gravtory.core.types import StepOutput, StepStatus, WorkflowRun, WorkflowStatus
        from gravtory.enterprise.key_manager import rotate_keys

        backend = InMemoryBackend()
        await backend.initialize()

        run = WorkflowRun(
            id="run-rotate-3",
            workflow_name="wf",
            workflow_version=1,
            namespace="default",
            status=WorkflowStatus.COMPLETED,
        )
        await backend.create_workflow_run(run)
        await backend.save_step_output(
            StepOutput(
                workflow_run_id="run-rotate-3",
                step_order=1,
                step_name="s1",
                output_data=b"not-encrypted-data",
                status=StepStatus.COMPLETED,
            )
        )

        # Should not raise, just skip and log warning
        rotated = await rotate_keys(backend, "default", "wrong-key", "new-key")
        assert rotated == 0
