# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""JSON serializer with enhanced type support."""

from __future__ import annotations

import dataclasses
import json
from base64 import b64decode, b64encode
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from gravtory.serialization.serializer import Serializer


class GravtoryJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder handling Python types not natively supported."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return {"__grav_type__": "datetime", "v": o.isoformat()}
        if isinstance(o, date):
            return {"__grav_type__": "date", "v": o.isoformat()}
        if isinstance(o, timedelta):
            return {"__grav_type__": "timedelta", "v": o.total_seconds()}
        if isinstance(o, UUID):
            return {"__grav_type__": "uuid", "v": str(o)}
        if isinstance(o, bytes):
            return {"__grav_type__": "bytes", "v": b64encode(o).decode("ascii")}
        if isinstance(o, Decimal):
            return {"__grav_type__": "decimal", "v": str(o)}
        if isinstance(o, set):
            return {"__grav_type__": "set", "v": list(o)}
        if isinstance(o, frozenset):
            return {"__grav_type__": "frozenset", "v": list(o)}
        if isinstance(o, Enum):
            return o.value
        if hasattr(o, "model_dump"):  # Pydantic v2
            return o.model_dump()
        if hasattr(o, "dict"):  # Pydantic v1
            return o.dict()
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        return super().default(o)


def _object_hook(obj: dict[str, Any]) -> Any:
    """Reconstruct tagged types on deserialization."""
    gtype = obj.get("__grav_type__")
    if gtype is None:
        return obj
    val = obj["v"]
    if gtype == "datetime":
        return datetime.fromisoformat(val)
    if gtype == "date":
        return date.fromisoformat(val)
    if gtype == "timedelta":
        return timedelta(seconds=val)
    if gtype == "uuid":
        return UUID(val)
    if gtype == "bytes":
        return b64decode(val)
    if gtype == "decimal":
        return Decimal(val)
    if gtype == "set":
        return set(val)
    if gtype == "frozenset":
        return frozenset(val)
    return obj  # pragma: no cover


class JSONSerializer(Serializer):
    """JSON serializer with enhanced type support.

    Handles types that stdlib json doesn't support natively:
      - datetime → ISO 8601 string (tagged, round-trips)
      - date → ISO 8601 string (tagged, round-trips)
      - timedelta → total seconds (tagged, round-trips)
      - UUID → string (tagged, round-trips)
      - bytes → base64 string (tagged, round-trips)
      - Decimal → string (tagged, round-trips)
      - set / frozenset → list (tagged, round-trips)
      - Pydantic models → model_dump()
      - Enum → value
      - dataclasses → asdict()
    """

    def serialize(self, data: Any) -> bytes:
        """Serialize Python object to JSON bytes."""
        return json.dumps(data, cls=GravtoryJSONEncoder, separators=(",", ":")).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize JSON bytes to Python object with type reconstruction."""
        return json.loads(data.decode("utf-8"), object_hook=_object_hook)
