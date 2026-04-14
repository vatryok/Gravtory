"""Unit tests for AES-256-GCM encryption engine."""

from __future__ import annotations

import pytest

from gravtory.core.errors import GravtoryError

cryptography = pytest.importorskip("cryptography")

from gravtory.serialization.encryption import AES256GCMEncryptor


class TestEncryptDecryptRoundTrip:
    def setup_method(self) -> None:
        self.e = AES256GCMEncryptor("my-secret-key")

    def test_basic(self) -> None:
        data = b"hello world"
        assert self.e.decrypt(self.e.encrypt(data)) == data

    def test_empty(self) -> None:
        assert self.e.decrypt(self.e.encrypt(b"")) == b""

    def test_large(self) -> None:
        data = b"x" * 1_000_000
        assert self.e.decrypt(self.e.encrypt(data)) == data

    def test_binary(self) -> None:
        data = bytes(range(256))
        assert self.e.decrypt(self.e.encrypt(data)) == data


class TestEncryptionProperties:
    def setup_method(self) -> None:
        self.e = AES256GCMEncryptor("my-secret-key")

    def test_different_nonces(self) -> None:
        data = b"same plaintext"
        ct1 = self.e.encrypt(data)
        ct2 = self.e.encrypt(data)
        # Same plaintext, different nonces → different ciphertext
        assert ct1 != ct2
        # But both decrypt to same plaintext
        assert self.e.decrypt(ct1) == data
        assert self.e.decrypt(ct2) == data

    def test_plaintext_not_visible(self) -> None:
        data = b"super secret message that should not appear"
        encrypted = self.e.encrypt(data)
        assert data not in encrypted

    def test_deterministic_key_derivation(self) -> None:
        e1 = AES256GCMEncryptor("same-key")
        e2 = AES256GCMEncryptor("same-key")
        data = b"test"
        # Both should be able to decrypt each other's output
        assert e2.decrypt(e1.encrypt(data)) == data
        assert e1.decrypt(e2.encrypt(data)) == data


class TestDecryptionFailures:
    def test_wrong_key(self) -> None:
        e1 = AES256GCMEncryptor("correct-key")
        e2 = AES256GCMEncryptor("wrong-key")
        encrypted = e1.encrypt(b"secret")
        with pytest.raises(GravtoryError, match=r"wrong key.*corrupted"):
            e2.decrypt(encrypted)

    def test_corrupted_data(self) -> None:
        e = AES256GCMEncryptor("key")
        encrypted = e.encrypt(b"data")
        # Flip a byte in the ciphertext
        corrupted = encrypted[:15] + bytes([encrypted[15] ^ 0xFF]) + encrypted[16:]
        with pytest.raises(GravtoryError, match=r"wrong key.*corrupted"):
            e.decrypt(corrupted)

    def test_too_short(self) -> None:
        e = AES256GCMEncryptor("key")
        with pytest.raises(GravtoryError, match="too short"):
            e.decrypt(b"short")
        # Also verify that a 1-byte input is caught
        with pytest.raises(GravtoryError, match="too short"):
            e.decrypt(b"\x02")


class TestEncryptionGapFill:
    """Gap-fill tests for encryption edge cases."""

    def test_unicode_key(self) -> None:
        """Keys with unicode characters work correctly."""
        e = AES256GCMEncryptor("日本語キー[KEY]")
        data = b"secret data"
        assert e.decrypt(e.encrypt(data)) == data

    def test_very_long_key(self) -> None:
        """Very long key strings are handled (PBKDF2 normalizes)."""
        e = AES256GCMEncryptor("k" * 10_000)
        data = b"test"
        assert e.decrypt(e.encrypt(data)) == data

    def test_different_keys_produce_different_ciphertext(self) -> None:
        e1 = AES256GCMEncryptor("key-alpha")
        e2 = AES256GCMEncryptor("key-beta")
        data = b"same plaintext"
        ct1 = e1.encrypt(data)
        ct2 = e2.encrypt(data)
        assert ct1 != ct2

    def test_encrypt_output_is_bytes(self) -> None:
        e = AES256GCMEncryptor("key")
        result = e.encrypt(b"hello")
        assert isinstance(result, bytes)
        assert len(result) > len(b"hello")  # nonce + tag overhead
