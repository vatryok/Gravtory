# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""MessagePack serializer — faster and more compact than JSON.

Requires: pip install gravtory[msgpack]
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any

from gravtory.core.errors import ConfigurationError
from gravtory.serialization.serializer import Serializer

try:
    import msgpack as _msgpack
except ImportError:  # pragma: no cover
    _msgpack = None  # type: ignore[assignment,unused-ignore]

EXT_DATETIME = 1
EXT_BYTES = 2


def _encode(obj: Any) -> Any:
    """Custom encoder for non-native msgpack types."""
    if isinstance(obj, datetime):
        return _msgpack.ExtType(EXT_DATETIME, obj.isoformat().encode("utf-8"))
    if isinstance(obj, bytes):
        return _msgpack.ExtType(EXT_BYTES, obj)
    if hasattr(obj, "model_dump"):  # Pydantic v2
        return obj.model_dump()
    if hasattr(obj, "dict"):  # Pydantic v1
        return obj.dict()
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Cannot serialize {type(obj).__name__}")


def _decode(code: int, data: bytes) -> Any:
    """Custom decoder for ext types."""
    if code == EXT_DATETIME:
        return datetime.fromisoformat(data.decode("utf-8"))
    if code == EXT_BYTES:
        return data
    return _msgpack.ExtType(code, data)


class MsgPackSerializer(Serializer):
    """MessagePack serializer — faster and more compact than JSON.

    Benefits over JSON:
      - 2-5x faster serialization
      - 30-50% smaller output
      - Native bytes support (no base64 encoding)

    Uses ext types for Python-specific types:
      - EXT_DATETIME (1) → datetime as ISO string bytes
      - EXT_BYTES (2) → raw bytes
    """

    def __init__(self) -> None:
        if _msgpack is None:
            raise ConfigurationError(
                "msgpack package is not installed. Install it with: pip install gravtory[msgpack]"
            )

    def serialize(self, data: Any) -> bytes:
        """Serialize Python object to msgpack bytes."""
        result: bytes = _msgpack.packb(data, default=_encode, use_bin_type=True)
        return result

    def deserialize(self, data: bytes) -> Any:
        """Deserialize msgpack bytes to Python object."""
        return _msgpack.unpackb(data, ext_hook=_decode, raw=False)
