# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Per-namespace encryption key management with key rotation.

Provides :class:`KeyManager` for resolving encryption keys per namespace
and :func:`rotate_keys` for re-encrypting existing step output data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from gravtory.backends.base import Backend
    from gravtory.serialization.serializer import Encryptor

logger = logging.getLogger("gravtory.enterprise.key_manager")


class KeyManager:
    """Manages encryption keys per namespace.

    Keys can be:
      - Provided directly via *namespace_keys*
      - Loaded dynamically from a *key_provider* callback
      - Fallback to *default_key*

    Usage::

        km = KeyManager(
            default_key="fallback-key",
            namespace_keys={"finance": "finance-key-2025"},
        )
        key = km.get_key("finance")   # → "finance-key-2025"
        key = km.get_key("other")     # → "fallback-key"
        key = km.get_key("no-key")    # → "fallback-key"
    """

    def __init__(
        self,
        default_key: str | None = None,
        namespace_keys: dict[str, str] | None = None,
        key_provider: Callable[[str], str | None] | None = None,
    ) -> None:
        self._default_key = default_key
        self._namespace_keys: dict[str, str] = namespace_keys or {}
        self._key_provider = key_provider

    def get_key(self, namespace: str) -> str | None:
        """Get encryption key for a namespace.

        Priority:
          1. ``namespace_keys[namespace]`` (explicit mapping)
          2. ``key_provider(namespace)`` (dynamic lookup)
          3. ``default_key`` (fallback)
          4. ``None`` (no encryption)
        """
        # 1. Explicit mapping
        if namespace in self._namespace_keys:
            return self._namespace_keys[namespace]

        # 2. Dynamic provider
        if self._key_provider is not None:
            key = self._key_provider(namespace)
            if key is not None:
                return key

        # 3. Default fallback
        return self._default_key

    def set_key(self, namespace: str, key: str) -> None:
        """Set or update the encryption key for a namespace."""
        self._namespace_keys[namespace] = key

    def remove_key(self, namespace: str) -> None:
        """Remove the encryption key for a namespace."""
        self._namespace_keys.pop(namespace, None)

    def list_namespaces_with_keys(self) -> list[str]:
        """Return all namespaces that have explicit keys."""
        return sorted(self._namespace_keys.keys())

    def get_encryptor(self, namespace: str) -> Encryptor | None:
        """Create an :class:`Encryptor` for the given namespace.

        Returns None if no key is configured for *namespace*.

        Raises:
            ConfigurationError: If cryptography package is not installed.
        """
        key = self.get_key(namespace)
        if key is None:
            return None
        from gravtory.serialization.encryption import AES256GCMEncryptor

        return AES256GCMEncryptor(key)


async def rotate_keys(
    backend: Backend,
    namespace: str,
    old_key: str,
    new_key: str,
) -> int:
    """Re-encrypt all step outputs in a namespace with a new key.

    The function fetches step output objects from the backend and
    mutates their ``output_data`` attribute in place (decrypt with
    *old_key*, re-encrypt with *new_key*).  For the in-memory backend
    this immediately persists because objects are mutable references.
    For persistent backends (SQL, MongoDB, Redis), a dedicated
    ``update_step_output`` backend method is required.

    Args:
        backend: Backend instance (must be initialized).
        namespace: Namespace whose data to re-encrypt.
        old_key: Current encryption key.
        new_key: New encryption key.

    Returns:
        Number of step outputs that were re-encrypted.
    """
    from gravtory.serialization.encryption import AES256GCMEncryptor

    old_enc = AES256GCMEncryptor(old_key)
    new_enc = AES256GCMEncryptor(new_key)

    runs = await backend.list_workflow_runs(namespace=namespace, limit=10000)
    rotated = 0

    for run in runs:
        outputs = await backend.get_step_outputs(run.id)
        for output in outputs:
            if output.output_data is None:
                continue
            try:
                plaintext = old_enc.decrypt(output.output_data)
                new_data = new_enc.encrypt(plaintext)
                await backend.update_step_output(
                    run.id,
                    output.step_order,
                    new_data,
                )
                rotated += 1
            except Exception:
                logger.warning(
                    "Failed to rotate key for run=%s step=%d — "
                    "data may not be encrypted or uses a different key",
                    run.id,
                    output.step_order,
                    exc_info=True,
                )
    return rotated
