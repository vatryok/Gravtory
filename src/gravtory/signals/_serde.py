# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Shared signal data serialization/deserialization."""

from __future__ import annotations

import json
from typing import Any


def deserialize_signal_data(raw: bytes | None) -> dict[str, Any]:
    """Deserialize signal data from bytes to dict.

    Returns an empty dict for ``None`` input.  Falls back to a ``_raw``
    key if the payload is not valid JSON.
    """
    if raw is None:
        return {}
    try:
        return dict(json.loads(raw))
    except (json.JSONDecodeError, TypeError):
        return {"_raw": raw.decode("utf-8", errors="replace")}
