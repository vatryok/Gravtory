"""Property-based tests for serialization round-trip invariants.

Tests that encode→decode is the identity function for arbitrary inputs,
and that compression wrappers preserve data integrity.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from gravtory.serialization.compression import GzipCompressor
from gravtory.serialization.encryption import AES256GCMEncryptor
from gravtory.serialization.json import JSONSerializer
from gravtory.serialization.pickle import PickleSerializer

pytestmark = pytest.mark.property


# ── Strategies ───────────────────────────────────────────────────

json_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=50),
    lambda children: (
        st.lists(children, max_size=5) | st.dictionaries(st.text(max_size=10), children, max_size=5)
    ),
    max_leaves=20,
)


# ── Tests ────────────────────────────────────────────────────────


class TestJSONSerializerRoundTrip:
    @given(data=json_values)
    @settings(max_examples=200)
    def test_serialize_deserialize_identity(self, data: object) -> None:
        """JSON serialize→deserialize returns the original value."""
        ser = JSONSerializer()
        encoded = ser.serialize(data)
        assert isinstance(encoded, bytes)
        decoded = ser.deserialize(encoded)
        assert decoded == data

    @given(data=json_values)
    @settings(max_examples=100)
    def test_serialize_produces_valid_utf8(self, data: object) -> None:
        """JSON serialization always produces valid UTF-8 bytes."""
        ser = JSONSerializer()
        encoded = ser.serialize(data)
        encoded.decode("utf-8")  # Should not raise


class TestPickleSerializerRoundTrip:
    @given(data=json_values)
    @settings(max_examples=200)
    def test_serialize_deserialize_identity(self, data: object) -> None:
        """Pickle serialize→deserialize returns the original value."""
        ser = PickleSerializer(unsafe_pickle=True)
        encoded = ser.serialize(data)
        assert isinstance(encoded, bytes)
        decoded = ser.deserialize(encoded)
        assert decoded == data


class TestGzipCompressionRoundTrip:
    @given(data=st.binary(min_size=0, max_size=2000))
    @settings(max_examples=200)
    def test_compress_decompress_identity(self, data: bytes) -> None:
        """compress→decompress returns the original bytes."""
        comp = GzipCompressor()
        compressed = comp.compress(data)
        assert isinstance(compressed, bytes)
        decompressed = comp.decompress(compressed)
        assert decompressed == data

    @given(data=st.binary(min_size=0, max_size=2000))
    @settings(max_examples=100)
    def test_compressed_is_bytes(self, data: bytes) -> None:
        """Compressed output is always bytes."""
        comp = GzipCompressor()
        compressed = comp.compress(data)
        assert isinstance(compressed, bytes)


class TestAES256GCMRoundTrip:
    @given(data=st.binary(min_size=1, max_size=2000))
    @settings(max_examples=200, deadline=None)
    def test_encrypt_decrypt_identity(self, data: bytes) -> None:
        """encrypt→decrypt returns the original bytes."""
        pytest.importorskip("cryptography")
        enc = AES256GCMEncryptor("test-secret-key-32-bytes-long!!")
        encrypted = enc.encrypt(data)
        assert isinstance(encrypted, bytes)
        assert encrypted != data
        decrypted = enc.decrypt(encrypted)
        assert decrypted == data

    @given(data=st.binary(min_size=1, max_size=2000))
    @settings(max_examples=100, deadline=None)
    def test_both_encryptions_decrypt_correctly(self, data: bytes) -> None:
        """Two encryptions of the same plaintext both decrypt correctly."""
        pytest.importorskip("cryptography")
        enc = AES256GCMEncryptor("test-secret-key-32-bytes-long!!")
        e1 = enc.encrypt(data)
        e2 = enc.encrypt(data)
        assert enc.decrypt(e1) == data
        assert enc.decrypt(e2) == data
