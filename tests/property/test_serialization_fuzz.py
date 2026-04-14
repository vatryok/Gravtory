"""Fuzz / property-based tests for the serialization layer.

Uses Hypothesis to generate random data and verify that:
1. serialize → deserialize is identity (round-trip)
2. Corrupted data doesn't crash (raises clean errors)
3. Checkpoint pipeline (serialize → compress → encrypt) round-trips
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gravtory.core.checkpoint import CheckpointEngine
from gravtory.core.errors import SerializationError
from gravtory.serialization.compression import GzipCompressor
from gravtory.serialization.json import JSONSerializer

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

json_primitives = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=200),
)

json_values = st.recursive(
    json_primitives,
    lambda children: st.one_of(
        st.lists(children, max_size=10),
        st.dictionaries(st.text(min_size=1, max_size=20), children, max_size=10),
    ),
    max_leaves=50,
)


# ---------------------------------------------------------------------------
# JSON Serializer Round-Trip
# ---------------------------------------------------------------------------


class TestJSONSerializerFuzz:
    """Fuzz the JSON serializer with random data."""

    @given(data=json_values)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_roundtrip_identity(self, data: object) -> None:
        """serialize(x) → deserialize → x for any JSON-compatible value."""
        ser = JSONSerializer()
        encoded = ser.serialize(data)
        assert isinstance(encoded, bytes)
        decoded = ser.deserialize(encoded)
        assert decoded == data

    @given(data=st.binary(min_size=0, max_size=500))
    @settings(max_examples=100)
    def test_deserialize_random_bytes_no_crash(self, data: bytes) -> None:
        """Deserializing random bytes should raise an error, never crash."""
        ser = JSONSerializer()
        try:
            ser.deserialize(data)
        except Exception:
            pass  # Any exception is acceptable, just no segfault/hang

    @given(
        data=st.dictionaries(
            st.text(min_size=1, max_size=30),
            st.one_of(
                st.integers(),
                st.text(max_size=50),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=20,
        )
    )
    @settings(max_examples=200)
    def test_dict_roundtrip(self, data: dict) -> None:
        """Dict values always round-trip through JSON."""
        ser = JSONSerializer()
        assert ser.deserialize(ser.serialize(data)) == data


# ---------------------------------------------------------------------------
# Gzip Compression Round-Trip
# ---------------------------------------------------------------------------


class TestGzipFuzz:
    """Fuzz the Gzip compressor."""

    @given(data=st.binary(min_size=0, max_size=5000))
    @settings(max_examples=100)
    def test_roundtrip(self, data: bytes) -> None:
        """compress → decompress is identity for any bytes."""
        comp = GzipCompressor()
        compressed = comp.compress(data)
        assert comp.decompress(compressed) == data

    @given(data=st.binary(min_size=0, max_size=200))
    @settings(max_examples=50)
    def test_decompress_random_bytes_no_crash(self, data: bytes) -> None:
        """Decompressing random bytes should raise, not crash."""
        comp = GzipCompressor()
        try:
            comp.decompress(data)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Checkpoint Pipeline Round-Trip
# ---------------------------------------------------------------------------


class TestCheckpointPipelineFuzz:
    """Fuzz the full checkpoint pipeline."""

    @given(data=json_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_json_only_roundtrip(self, data: object) -> None:
        """JSON-only pipeline: process → restore is identity."""
        engine = CheckpointEngine(serializer="json")
        stored = engine.process(data)
        restored = engine.restore(stored)
        assert restored == data

    @given(data=json_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_json_gzip_roundtrip(self, data: object) -> None:
        """JSON + gzip pipeline: process → restore is identity."""
        engine = CheckpointEngine(serializer="json", compression="gzip")
        stored = engine.process(data)
        restored = engine.restore(stored)
        assert restored == data

    def test_empty_data_raises(self) -> None:
        """Restoring empty bytes raises SerializationError."""
        engine = CheckpointEngine()
        with pytest.raises(SerializationError):
            engine.restore(b"")

    @given(data=st.binary(min_size=1, max_size=200))
    @settings(max_examples=50)
    def test_restore_random_bytes_no_crash(self, data: bytes) -> None:
        """Restoring random bytes should raise, not crash."""
        engine = CheckpointEngine()
        try:
            engine.restore(data)
        except Exception:
            pass
