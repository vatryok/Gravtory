# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Pickle serializer — handles ANY Python object.

SECURITY WARNING: Pickle can execute arbitrary code during
deserialization. Only use in trusted environments.
"""

from __future__ import annotations

import io
import pickle
from typing import Any

from gravtory.core.errors import ConfigurationError
from gravtory.serialization.serializer import Serializer


class RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that only allows a set of whitelisted classes."""

    def __init__(self, file: io.BytesIO, allowed: frozenset[str]) -> None:
        super().__init__(file)
        self._allowed = allowed

    def find_class(self, module: str, name: str) -> type:
        full = f"{module}.{name}"
        if full not in self._allowed:
            raise pickle.UnpicklingError(f"Class {full!r} not in allowed list")
        return super().find_class(module, name)  # type: ignore[no-any-return]


class PickleSerializer(Serializer):
    """Pickle serializer — handles ANY Python object.

    SECURITY WARNING: Pickle can execute arbitrary code during
    deserialization. Only use in trusted environments.

    Benefits:
      - Handles complex Python objects (numpy arrays, custom classes)
      - No need for custom encoders

    Drawbacks:
      - Security risk (code execution on deserialize)
      - Not cross-language
      - Fragile across Python versions

    If ``allowed_classes`` is set, only those fully-qualified class names
    can be unpickled. This mitigates (but doesn't eliminate) the security risk.
    """

    PROTOCOL: int = pickle.HIGHEST_PROTOCOL

    def __init__(
        self,
        allowed_classes: set[str] | None = None,
        *,
        unsafe_pickle: bool = False,
    ) -> None:
        import logging

        if allowed_classes is None and not unsafe_pickle:
            raise ConfigurationError(
                "PickleSerializer requires an explicit 'allowed_classes' set "
                "to restrict deserialization. If you understand the security "
                "risk (arbitrary code execution) and intentionally want "
                "unrestricted pickle, pass unsafe_pickle=True."
            )
        if unsafe_pickle and allowed_classes is None:
            logging.getLogger("gravtory.security").warning(
                "SECURITY: PickleSerializer instantiated with unsafe_pickle=True. "
                "Arbitrary code execution is possible during deserialization."
            )
        self._allowed: frozenset[str] | None = (
            frozenset(allowed_classes) if allowed_classes else None
        )

    def serialize(self, data: Any) -> bytes:
        """Serialize Python object to pickle bytes."""
        return pickle.dumps(data, protocol=self.PROTOCOL)

    def deserialize(self, data: bytes) -> Any:
        """Deserialize pickle bytes to Python object."""
        if self._allowed is not None:
            return RestrictedUnpickler(io.BytesIO(data), self._allowed).load()
        return pickle.loads(data)  # nosec
