"""T-003: Property-based testing for checkpoint round-trips.

Uses Hypothesis to verify that arbitrary data survives the
serialize → compress → encrypt → decrypt → decompress → deserialize pipeline.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gravtory.core.checkpoint import CheckpointEngine

# ---------------------------------------------------------------------------
# Strategies for arbitrary checkpoint-safe data
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
        st.dictionaries(st.text(max_size=20), children, max_size=10),
    ),
    max_leaves=50,
)


# ---------------------------------------------------------------------------
# T-003a: JSON serializer round-trip
# ---------------------------------------------------------------------------


@given(data=json_values)
@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
def test_json_checkpoint_round_trip(data):
    """Arbitrary JSON-safe data should survive process/restore cycle."""
    engine = CheckpointEngine(serializer="json", max_checkpoint_size=0)
    blob = engine.process(data)
    restored = engine.restore(blob)
    assert restored == data


# ---------------------------------------------------------------------------
# T-003b: JSON + gzip compression round-trip
# ---------------------------------------------------------------------------


@given(data=json_values)
@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
def test_json_gzip_checkpoint_round_trip(data):
    """Arbitrary data should survive process/restore with gzip compression."""
    engine = CheckpointEngine(
        serializer="json",
        compression="gzip",
        auto_compress_threshold=0,
        max_checkpoint_size=0,
    )
    blob = engine.process(data)
    restored = engine.restore(blob)
    assert restored == data


# ---------------------------------------------------------------------------
# T-003c: JSON + encryption round-trip
# ---------------------------------------------------------------------------


@given(data=json_values)
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_json_encrypted_checkpoint_round_trip(data):
    """Arbitrary data should survive process/restore with AES-256-GCM encryption."""
    pytest.importorskip("cryptography")
    engine = CheckpointEngine(
        serializer="json",
        encryption_key="test-key-for-property-testing-32ch",
        max_checkpoint_size=0,
    )
    blob = engine.process(data)
    restored = engine.restore(blob)
    assert restored == data


# ---------------------------------------------------------------------------
# T-003d: JSON + gzip + encryption round-trip
# ---------------------------------------------------------------------------


@given(data=json_values)
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_full_pipeline_checkpoint_round_trip(data):
    """Arbitrary data should survive the full serialize→compress→encrypt pipeline."""
    pytest.importorskip("cryptography")
    engine = CheckpointEngine(
        serializer="json",
        compression="gzip",
        encryption_key="test-key-for-property-testing-32ch",
        auto_compress_threshold=0,
        max_checkpoint_size=0,
    )
    blob = engine.process(data)
    restored = engine.restore(blob)
    assert restored == data


# ---------------------------------------------------------------------------
# T-003e: Different engines can restore each other's data (header portability)
# ---------------------------------------------------------------------------


@given(data=json_values)
@settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
def test_cross_engine_restore(data):
    """Data encoded by one engine should be restorable by a fresh engine
    (the header contains all info needed to determine the pipeline)."""
    engine_write = CheckpointEngine(
        serializer="json",
        compression="gzip",
        auto_compress_threshold=0,
        max_checkpoint_size=0,
    )
    engine_read = CheckpointEngine(serializer="json", max_checkpoint_size=0)

    blob = engine_write.process(data)
    restored = engine_read.restore(blob)
    assert restored == data
