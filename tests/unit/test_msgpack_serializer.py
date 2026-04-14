"""Tests for serialization.msgpack — MsgPackSerializer and encode/decode helpers."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from gravtory.serialization.msgpack import (
    EXT_BYTES,
    EXT_DATETIME,
    MsgPackSerializer,
    _decode,
    _encode,
)


class TestMsgPackEncode:
    def test_encode_datetime(self) -> None:
        dt = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        result = _encode(dt)
        assert result.code == EXT_DATETIME
        assert b"2025-01-01" in result.data

    def test_encode_bytes(self) -> None:
        result = _encode(b"raw bytes")
        assert result.code == EXT_BYTES
        assert result.data == b"raw bytes"

    def test_encode_dataclass(self) -> None:
        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        result = _encode(Point(1, 2))
        assert result == {"x": 1, "y": 2}

    def test_encode_set(self) -> None:
        result = _encode({1, 2, 3})
        assert sorted(result) == [1, 2, 3]

    def test_encode_unsupported_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot serialize"):
            _encode(object())

    def test_encode_pydantic_v2_model_dump(self) -> None:
        class FakeModel:
            def model_dump(self) -> dict:
                return {"field": "value"}

        result = _encode(FakeModel())
        assert result == {"field": "value"}

    def test_encode_pydantic_v1_dict(self) -> None:
        class FakeModelV1:
            def dict(self) -> dict:
                return {"old": "format"}

        result = _encode(FakeModelV1())
        assert result == {"old": "format"}


class TestMsgPackDecode:
    def test_decode_datetime(self) -> None:
        data = b"2025-06-01T12:00:00+00:00"
        result = _decode(EXT_DATETIME, data)
        assert isinstance(result, datetime)
        assert result.year == 2025

    def test_decode_bytes(self) -> None:
        result = _decode(EXT_BYTES, b"raw")
        assert result == b"raw"

    def test_decode_unknown_ext(self) -> None:
        import msgpack

        result = _decode(99, b"data")
        assert isinstance(result, msgpack.ExtType)
        assert result.code == 99


class TestMsgPackSerializer:
    def test_roundtrip_simple(self) -> None:
        s = MsgPackSerializer()
        data = {"key": "value", "num": 42, "nested": [1, 2, 3]}
        packed = s.serialize(data)
        assert isinstance(packed, bytes)
        unpacked = s.deserialize(packed)
        assert unpacked == data

    def test_roundtrip_datetime(self) -> None:
        s = MsgPackSerializer()
        dt = datetime(2025, 3, 15, 10, 30, tzinfo=timezone.utc)
        packed = s.serialize(dt)
        unpacked = s.deserialize(packed)
        assert isinstance(unpacked, datetime)
        assert unpacked.year == 2025

    def test_roundtrip_bytes(self) -> None:
        s = MsgPackSerializer()
        data = {"payload": b"binary content"}
        packed = s.serialize(data)
        unpacked = s.deserialize(packed)
        assert unpacked["payload"] == b"binary content"

    def test_roundtrip_nested(self) -> None:
        s = MsgPackSerializer()
        data = {"items": [{"a": 1}, {"b": 2}], "flag": True}
        packed = s.serialize(data)
        unpacked = s.deserialize(packed)
        assert unpacked == data

    def test_roundtrip_none(self) -> None:
        s = MsgPackSerializer()
        packed = s.serialize(None)
        assert s.deserialize(packed) is None

    def test_roundtrip_dataclass(self) -> None:
        @dataclasses.dataclass
        class Item:
            name: str
            count: int

        s = MsgPackSerializer()
        packed = s.serialize(Item("widget", 5))
        unpacked = s.deserialize(packed)
        assert unpacked == {"name": "widget", "count": 5}
