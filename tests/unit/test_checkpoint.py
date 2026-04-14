"""Unit tests for CheckpointEngine — full pipeline round-trips and header encoding."""

from __future__ import annotations

import dataclasses
from datetime import datetime

import pytest

from gravtory.core.checkpoint import (
    CheckpointEngine,
    _build_header,
    _parse_header,
)
from gravtory.core.errors import ConfigurationError, SerializationError

cryptography = pytest.importorskip("cryptography")


# ── Parametrized round-trip across ALL combos ─────────────────────


@pytest.mark.parametrize("serializer", ["json", "msgpack", "pickle"])
@pytest.mark.parametrize("compression", [None, "gzip", "lz4", "zstd"])
@pytest.mark.parametrize("encryption", [None, "test-key"])
def test_round_trip_all_combos(
    serializer: str, compression: str | None, encryption: str | None
) -> None:
    """Full pipeline round-trip for every serializer x compressor x encryption combo."""
    kwargs: dict[str, object] = dict(
        serializer=serializer,
        compression=compression,
        encryption_key=encryption,
    )
    if serializer == "pickle":
        kwargs["pickle_allowed_classes"] = {
            "builtins.dict",
            "builtins.list",
            "builtins.int",
            "builtins.str",
        }
    engine = CheckpointEngine(**kwargs)  # type: ignore[arg-type]
    data = {"key": "value", "number": 42, "list": [1, 2, 3]}
    assert engine.restore(engine.process(data)) == data


@pytest.mark.parametrize("serializer", ["json", "msgpack", "pickle"])
@pytest.mark.parametrize("compression", [None, "gzip", "lz4", "zstd"])
@pytest.mark.parametrize("encryption", [None, "test-key"])
def test_round_trip_none(serializer: str, compression: str | None, encryption: str | None) -> None:
    """None round-trips correctly through every combo."""
    kwargs: dict[str, object] = dict(
        serializer=serializer,
        compression=compression,
        encryption_key=encryption,
    )
    if serializer == "pickle":
        kwargs["pickle_allowed_classes"] = {
            "builtins.dict",
            "builtins.list",
            "builtins.int",
            "builtins.str",
        }
    engine = CheckpointEngine(**kwargs)  # type: ignore[arg-type]
    assert engine.restore(engine.process(None)) is None


# ── Header byte encoding ─────────────────────────────────────────


class TestHeaderByte:
    def test_json_no_compression_no_encryption(self) -> None:
        h = _build_header(ser_id=0, comp_id=0, encrypted=False)
        assert h == 0b00000000
        ser, comp, enc = _parse_header(h)
        assert ser == 0
        assert comp == 0
        assert enc is False

    def test_json_gzip_no_encryption(self) -> None:
        h = _build_header(ser_id=0, comp_id=1, encrypted=False)
        ser, comp, enc = _parse_header(h)
        assert ser == 0
        assert comp == 1
        assert enc is False

    def test_json_no_compression_encrypted(self) -> None:
        h = _build_header(ser_id=0, comp_id=0, encrypted=True)
        ser, comp, enc = _parse_header(h)
        assert ser == 0
        assert comp == 0
        assert enc is True

    def test_pickle_zstd_encrypted(self) -> None:
        h = _build_header(ser_id=2, comp_id=3, encrypted=True)
        ser, comp, enc = _parse_header(h)
        assert ser == 2
        assert comp == 3
        assert enc is True

    def test_msgpack_lz4(self) -> None:
        h = _build_header(ser_id=1, comp_id=2, encrypted=False)
        ser, comp, enc = _parse_header(h)
        assert ser == 1
        assert comp == 2
        assert enc is False

    def test_round_trip_all_combos(self) -> None:
        for ser_id in range(3):
            for comp_id in range(4):
                for encrypted in [False, True]:
                    h = _build_header(ser_id, comp_id, encrypted)
                    s, c, e = _parse_header(h)
                    assert s == ser_id
                    if comp_id == 0:
                        assert c == 0
                    else:
                        assert c == comp_id
                    assert e == encrypted


# ── JSON-only pipeline ────────────────────────────────────────────


class TestJSONOnly:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(serializer="json")

    def test_none(self) -> None:
        assert self.engine.restore(self.engine.process(None)) is None

    def test_basic_dict(self) -> None:
        d = {"key": "value", "number": 42, "list": [1, 2, 3]}
        assert self.engine.restore(self.engine.process(d)) == d

    def test_nested_dict(self) -> None:
        d = {"a": {"b": {"c": [1, {"d": True}]}}}
        assert self.engine.restore(self.engine.process(d)) == d

    def test_empty_string(self) -> None:
        assert self.engine.restore(self.engine.process("")) == ""

    def test_unicode(self) -> None:
        assert self.engine.restore(self.engine.process("日本語テスト")) == "日本語テスト"

    def test_datetime(self) -> None:
        dt = datetime(2024, 6, 15, 10, 30, 0)
        result = self.engine.restore(self.engine.process(dt))
        assert isinstance(result, datetime)
        assert result == dt

    def test_large_payload(self) -> None:
        data = list(range(100_000))
        assert self.engine.restore(self.engine.process(data)) == data

    def test_header_is_first_byte(self) -> None:
        raw = self.engine.process(42)
        assert raw[0] == 0  # JSON, no compression, no encryption


# ── JSON + Gzip pipeline ─────────────────────────────────────────


class TestJSONGzip:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(serializer="json", compression="gzip")

    def test_round_trip(self) -> None:
        d = {"key": "value", "numbers": list(range(1000))}
        assert self.engine.restore(self.engine.process(d)) == d

    def test_compression_reduces_size(self) -> None:
        d = {"data": "x" * 10000}
        plain = CheckpointEngine(serializer="json")
        compressed_bytes = self.engine.process(d)
        plain_bytes = plain.process(d)
        assert len(compressed_bytes) < len(plain_bytes)


# ── JSON + Encryption pipeline ────────────────────────────────────


class TestJSONEncrypted:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(serializer="json", encryption_key="test-key")

    def test_round_trip(self) -> None:
        d = {"secret": "data"}
        assert self.engine.restore(self.engine.process(d)) == d

    def test_plaintext_not_visible(self) -> None:
        d = {"secret": "super_secret_value_12345"}
        encrypted = self.engine.process(d)
        assert b"super_secret_value_12345" not in encrypted

    def test_header_shows_encrypted(self) -> None:
        raw = self.engine.process(42)
        assert raw[0] & 2  # bit 1 set


# ── JSON + Gzip + Encryption pipeline ────────────────────────────


class TestFullPipeline:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(serializer="json", compression="gzip", encryption_key="key")

    def test_round_trip(self) -> None:
        d = {"nested": {"data": [1, 2, 3]}, "flag": True}
        assert self.engine.restore(self.engine.process(d)) == d

    def test_none(self) -> None:
        assert self.engine.restore(self.engine.process(None)) is None


# ── Pickle pipeline ──────────────────────────────────────────────


@dataclasses.dataclass
class _PickleTestItem:
    x: int


class TestPicklePipeline:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(
            serializer="pickle",
            pickle_allowed_classes={
                "builtins.dict",
                "builtins.set",
                "builtins.int",
                "builtins.str",
                "tests.unit.test_checkpoint._PickleTestItem",
            },
        )

    def test_basic(self) -> None:
        d = {"a": 1, "b": {2, 3}}
        result = self.engine.restore(self.engine.process(d))
        assert result == d

    def test_custom_class(self) -> None:
        f = _PickleTestItem(42)
        result = self.engine.restore(self.engine.process(f))
        assert isinstance(result, _PickleTestItem)
        assert result.x == 42


# ── restore_typed ─────────────────────────────────────────────────


class TestRestoreTyped:
    def setup_method(self) -> None:
        self.engine = CheckpointEngine(serializer="json")

    def test_raw_dict_without_type(self) -> None:
        d = {"x": 1}
        result = self.engine.restore_typed(self.engine.process(d), None)
        assert result == d

    def test_dataclass_reconstruction(self) -> None:
        @dataclasses.dataclass
        class Item:
            name: str
            count: int

        d = {"name": "widget", "count": 5}
        result = self.engine.restore_typed(self.engine.process(d), Item)
        assert isinstance(result, Item)
        assert result.name == "widget"
        assert result.count == 5

    def test_none_data(self) -> None:
        result = self.engine.restore_typed(self.engine.process(None), str)
        assert result is None

    def test_primitive_passthrough(self) -> None:
        result = self.engine.restore_typed(self.engine.process(42), int)
        assert result == 42


# ── Error cases ──────────────────────────────────────────────────


class TestCheckpointErrors:
    def test_unknown_serializer(self) -> None:
        with pytest.raises(ConfigurationError, match="Unknown serializer"):
            CheckpointEngine(serializer="xml")

    def test_unknown_compressor(self) -> None:
        with pytest.raises(ConfigurationError, match="Unknown compressor"):
            CheckpointEngine(compression="brotli")

    def test_empty_data_restore(self) -> None:
        engine = CheckpointEngine()
        with pytest.raises(SerializationError, match="empty"):
            engine.restore(b"")

    def test_encrypted_data_without_key(self) -> None:
        e1 = CheckpointEngine(serializer="json", encryption_key="key")
        encrypted = e1.process({"x": 1})
        e2 = CheckpointEngine(serializer="json")
        with pytest.raises(SerializationError, match="encrypted"):
            e2.restore(encrypted)


# ── Cross-engine restore ─────────────────────────────────────────


class TestCrossEngineRestore:
    def test_different_engine_reads_header(self) -> None:
        """Engine B can restore data from Engine A by reading header."""
        e_gzip = CheckpointEngine(serializer="json", compression="gzip", encryption_key="k")
        e_plain = CheckpointEngine(serializer="json", encryption_key="k")
        data = {"cross": "engine"}
        blob = e_gzip.process(data)
        # e_plain doesn't use gzip itself, but restore reads the header and
        # creates the right decompressor
        assert e_plain.restore(blob) == data


# ── End-to-end: Pydantic step outputs through full pipeline ──────


class TestPydanticEndToEnd:
    """Simulate a workflow where step outputs are Pydantic models
    serialized through the full checkpoint pipeline."""

    def test_pydantic_model_round_trip(self) -> None:
        """Pydantic model → process → restore_typed → original model."""
        pydantic = pytest.importorskip("pydantic")

        class OrderInfo(pydantic.BaseModel):  # type: ignore[name-defined,misc]
            order_id: str
            amount: float
            items: list[str]

        engine = CheckpointEngine(serializer="json", compression="gzip", encryption_key="secret")

        original = OrderInfo(order_id="ORD-123", amount=99.95, items=["widget", "gear"])
        blob = engine.process(original)

        # Plaintext must not be visible in encrypted+compressed output
        assert b"ORD-123" not in blob

        restored = engine.restore_typed(blob, OrderInfo)
        assert isinstance(restored, OrderInfo)
        assert restored.order_id == "ORD-123"
        assert restored.amount == pytest.approx(99.95)
        assert restored.items == ["widget", "gear"]

    def test_pydantic_nested_models(self) -> None:
        """Nested Pydantic models survive the full pipeline."""
        pydantic = pytest.importorskip("pydantic")

        class Address(pydantic.BaseModel):  # type: ignore[name-defined,misc]
            city: str
            zip_code: str

        class Customer(pydantic.BaseModel):  # type: ignore[name-defined,misc]
            name: str
            address: Address

        engine = CheckpointEngine(serializer="json")
        original = Customer(name="Alice", address=Address(city="NYC", zip_code="10001"))
        blob = engine.process(original)
        restored = engine.restore_typed(blob, Customer)
        assert isinstance(restored, Customer)
        assert restored.name == "Alice"
        assert isinstance(restored.address, Address)
        assert restored.address.city == "NYC"

    def test_dataclass_through_pipeline(self) -> None:
        """Dataclass step outputs through full compress+encrypt pipeline."""

        @dataclasses.dataclass
        class StepResult:
            status: str
            count: int

        engine = CheckpointEngine(serializer="json", compression="gzip", encryption_key="k")
        blob = engine.process({"status": "ok", "count": 42})
        restored = engine.restore_typed(blob, StepResult)
        assert isinstance(restored, StepResult)
        assert restored.status == "ok"
        assert restored.count == 42


# ── Checkpoint gap-fill tests ────────────────────────────────────


class TestCheckpointGapFill:
    """Gap-fill tests for checkpoint edge cases."""

    def test_very_large_payload_10mb(self) -> None:
        """10 MB payload survives round-trip."""
        engine = CheckpointEngine(serializer="json", compression="gzip")
        data = {"big": "x" * (10 * 1024 * 1024)}
        assert engine.restore(engine.process(data)) == data

    def test_deeply_nested_dict_100_levels(self) -> None:
        """100-level nested dict survives round-trip."""
        engine = CheckpointEngine(serializer="json")
        d: dict[str, object] = {"value": "leaf"}
        for _ in range(100):
            d = {"child": d}
        assert engine.restore(engine.process(d)) == d

    def test_unicode_emoji_round_trip(self) -> None:
        """Unicode emoji characters survive round-trip."""
        engine = CheckpointEngine(serializer="json")
        data = {"emoji": "[ROCKET][PARTY][LAPTOP][FIRE]", "mixed": "Hello 世界 [EARTH]"}
        assert engine.restore(engine.process(data)) == data

    def test_binary_data_round_trip_pickle(self) -> None:
        """Binary data survives round-trip with pickle serializer."""
        engine = CheckpointEngine(serializer="pickle", pickle_allowed_classes={"builtins.bytes"})
        data = b"\x00\x01\x02\xff" * 1000
        assert engine.restore(engine.process(data)) == data

    def test_inf_nan_handling_pickle(self) -> None:
        """Inf and NaN survive round-trip via pickle (JSON cannot handle these)."""
        import math

        engine = CheckpointEngine(
            serializer="pickle", pickle_allowed_classes={"builtins.dict", "builtins.float"}
        )
        data = {"inf": float("inf"), "neg_inf": float("-inf"), "nan": float("nan")}
        restored = engine.restore(engine.process(data))
        assert restored["inf"] == float("inf")
        assert restored["neg_inf"] == float("-inf")
        assert math.isnan(restored["nan"])

    def test_checkpoint_engine_reuse(self) -> None:
        """Same engine can process many items without state leaks."""
        engine = CheckpointEngine(serializer="json", compression="gzip")
        for i in range(100):
            data = {"index": i, "payload": f"item_{i}"}
            assert engine.restore(engine.process(data)) == data

    def test_empty_dict_round_trip(self) -> None:
        """Empty dict survives round-trip."""
        engine = CheckpointEngine(serializer="json")
        assert engine.restore(engine.process({})) == {}
