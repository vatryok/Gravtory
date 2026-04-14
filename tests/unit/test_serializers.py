"""Unit tests for JSON, MsgPack, and Pickle serializers."""

from __future__ import annotations

import dataclasses
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

from gravtory.serialization.json import JSONSerializer


class Color(Enum):
    RED = "red"
    GREEN = "green"


@dataclasses.dataclass
class Point:
    x: int
    y: int


# ── JSON Serializer ──────────────────────────────────────────────


class TestJSONBasicTypes:
    def setup_method(self) -> None:
        self.s = JSONSerializer()

    def test_none(self) -> None:
        assert self.s.deserialize(self.s.serialize(None)) is None

    def test_int(self) -> None:
        assert self.s.deserialize(self.s.serialize(42)) == 42

    def test_float(self) -> None:
        assert self.s.deserialize(self.s.serialize(3.14)) == pytest.approx(3.14)

    def test_string(self) -> None:
        assert self.s.deserialize(self.s.serialize("hello")) == "hello"

    def test_bool(self) -> None:
        assert self.s.deserialize(self.s.serialize(True)) is True

    def test_list(self) -> None:
        assert self.s.deserialize(self.s.serialize([1, 2, 3])) == [1, 2, 3]

    def test_dict(self) -> None:
        d = {"a": 1, "b": [2, 3]}
        assert self.s.deserialize(self.s.serialize(d)) == d

    def test_nested(self) -> None:
        d = {"users": [{"name": "Alice", "scores": [1, 2]}]}
        assert self.s.deserialize(self.s.serialize(d)) == d

    def test_empty_string(self) -> None:
        assert self.s.deserialize(self.s.serialize("")) == ""

    def test_unicode(self) -> None:
        assert self.s.deserialize(self.s.serialize("日本語")) == "日本語"


class TestJSONExtendedTypes:
    def setup_method(self) -> None:
        self.s = JSONSerializer()

    def test_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 30, 0)
        result = self.s.deserialize(self.s.serialize(dt))
        assert isinstance(result, datetime)
        assert result == dt

    def test_date(self) -> None:
        d = date(2024, 1, 15)
        result = self.s.deserialize(self.s.serialize(d))
        assert isinstance(result, date)
        assert result == d

    def test_timedelta(self) -> None:
        td = timedelta(hours=2, minutes=30)
        result = self.s.deserialize(self.s.serialize(td))
        assert isinstance(result, timedelta)
        assert result == td

    def test_uuid(self) -> None:
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = self.s.deserialize(self.s.serialize(u))
        assert isinstance(result, UUID)
        assert result == u

    def test_bytes(self) -> None:
        b = b"\x00\x01\x02\xff"
        result = self.s.deserialize(self.s.serialize(b))
        assert isinstance(result, bytes)
        assert result == b

    def test_decimal(self) -> None:
        d = Decimal("3.14159")
        result = self.s.deserialize(self.s.serialize(d))
        assert isinstance(result, Decimal)
        assert result == d

    def test_set(self) -> None:
        s = {1, 2, 3}
        result = self.s.deserialize(self.s.serialize(s))
        assert isinstance(result, set)
        assert result == s

    def test_frozenset(self) -> None:
        fs = frozenset([4, 5, 6])
        result = self.s.deserialize(self.s.serialize(fs))
        assert isinstance(result, frozenset)
        assert result == fs

    def test_enum(self) -> None:
        result = self.s.deserialize(self.s.serialize(Color.RED))
        assert result == "red"

    def test_dataclass(self) -> None:
        p = Point(1, 2)
        result = self.s.deserialize(self.s.serialize(p))
        assert result == {"x": 1, "y": 2}

    def test_large_payload(self) -> None:
        data = list(range(100_000))
        assert self.s.deserialize(self.s.serialize(data)) == data


# ── MsgPack Serializer ───────────────────────────────────────────

try:
    import msgpack as _msgpack  # type: ignore[import-untyped]  # noqa: F401

    _has_msgpack = True
except ImportError:
    _has_msgpack = False


@pytest.mark.skipif(not _has_msgpack, reason="msgpack not installed")
class TestMsgPackBasicTypes:
    def setup_method(self) -> None:
        from gravtory.serialization.msgpack import MsgPackSerializer

        self.s = MsgPackSerializer()

    def test_none(self) -> None:
        assert self.s.deserialize(self.s.serialize(None)) is None

    def test_int(self) -> None:
        assert self.s.deserialize(self.s.serialize(42)) == 42

    def test_string(self) -> None:
        assert self.s.deserialize(self.s.serialize("hello")) == "hello"

    def test_dict(self) -> None:
        d = {"a": 1, "b": [2, 3]}
        assert self.s.deserialize(self.s.serialize(d)) == d

    def test_list(self) -> None:
        assert self.s.deserialize(self.s.serialize([1, 2, 3])) == [1, 2, 3]

    def test_datetime_ext(self) -> None:
        dt = datetime(2024, 1, 15, 12, 30, 0)
        result = self.s.deserialize(self.s.serialize(dt))
        assert isinstance(result, datetime)
        assert result == dt

    def test_bytes_ext(self) -> None:
        b = b"\x00\x01\xff"
        result = self.s.deserialize(self.s.serialize(b))
        # MsgPack serializes bytes natively (use_bin_type=True)
        assert result == b

    def test_dataclass(self) -> None:
        p = Point(1, 2)
        result = self.s.deserialize(self.s.serialize(p))
        assert result == {"x": 1, "y": 2}


# ── Pickle Serializer ────────────────────────────────────────────


class TestPickleBasicTypes:
    def setup_method(self) -> None:
        from gravtory.serialization.pickle import PickleSerializer

        self.s = PickleSerializer(unsafe_pickle=True)

    def test_none(self) -> None:
        assert self.s.deserialize(self.s.serialize(None)) is None

    def test_int(self) -> None:
        assert self.s.deserialize(self.s.serialize(42)) == 42

    def test_string(self) -> None:
        assert self.s.deserialize(self.s.serialize("hello")) == "hello"

    def test_dict(self) -> None:
        d = {"a": 1, "b": [2, 3]}
        assert self.s.deserialize(self.s.serialize(d)) == d

    def test_set(self) -> None:
        s = {1, 2, 3}
        assert self.s.deserialize(self.s.serialize(s)) == s

    def test_custom_class(self) -> None:
        p = Point(10, 20)
        result = self.s.deserialize(self.s.serialize(p))
        assert isinstance(result, Point)
        assert result.x == 10
        assert result.y == 20

    def test_complex_types(self) -> None:
        data = {frozenset([1, 2]): "hello", (3, 4): [5, 6]}
        assert self.s.deserialize(self.s.serialize(data)) == data


class TestPickleRestricted:
    def test_allowed_class_works(self) -> None:
        from gravtory.serialization.pickle import PickleSerializer

        s = PickleSerializer(allowed_classes={f"{Point.__module__}.{Point.__qualname__}"})
        p = Point(1, 2)
        result = s.deserialize(s.serialize(p))
        assert isinstance(result, Point)
        assert result == p

    def test_disallowed_class_blocked(self) -> None:
        import pickle

        from gravtory.serialization.pickle import PickleSerializer

        s = PickleSerializer(allowed_classes={"builtins.int"})
        p = Point(1, 2)
        data = s.serialize(p)
        with pytest.raises(pickle.UnpicklingError, match="not in allowed list"):
            s.deserialize(data)


class TestSerializerGapFill:
    """Gap-fill tests for serializer edge cases."""

    def test_json_deeply_nested(self) -> None:
        s = JSONSerializer()
        d: dict[str, object] = {"level": 0}
        current = d
        for i in range(1, 50):
            child: dict[str, object] = {"level": i}
            current["child"] = child  # type: ignore[assignment]
            current = child
        assert s.deserialize(s.serialize(d))["level"] == 0  # type: ignore[index]

    def test_json_special_chars(self) -> None:
        s = JSONSerializer()
        data = {'key\n\t"\\': "value\r\n"}
        assert s.deserialize(s.serialize(data)) == data

    def test_json_large_int(self) -> None:
        s = JSONSerializer()
        big = 10**100
        assert s.deserialize(s.serialize(big)) == big

    def test_json_empty_collections(self) -> None:
        s = JSONSerializer()
        assert s.deserialize(s.serialize([])) == []
        assert s.deserialize(s.serialize({})) == {}

    def test_json_mixed_types_in_list(self) -> None:
        s = JSONSerializer()
        data = [1, "two", 3.0, None, True, [4], {"five": 5}]
        assert s.deserialize(s.serialize(data)) == data
