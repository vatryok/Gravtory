# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Abstract base classes for serialization, compression, and encryption."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """Abstract serializer interface."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize a Python object to bytes."""
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes back to a Python object."""
        ...


class Compressor(ABC):
    """Abstract compressor interface."""

    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compress bytes."""
        ...

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress bytes."""
        ...


class Encryptor(ABC):
    """Abstract encryptor interface."""

    @abstractmethod
    def encrypt(self, data: bytes, aad: bytes | None = None) -> bytes:
        """Encrypt bytes. Output includes nonce/IV needed for decryption.

        Args:
            data: Plaintext bytes to encrypt.
            aad: Optional Associated Authenticated Data. When provided, the
                 ciphertext is cryptographically bound to this context
                 (e.g. run_id + step_order). The same AAD must be passed
                 to decrypt().
        """
        ...

    @abstractmethod
    def decrypt(self, data: bytes, aad: bytes | None = None) -> bytes:
        """Decrypt bytes previously encrypted by encrypt().

        Args:
            data: Ciphertext bytes (including nonce/IV/tag).
            aad: The same AAD that was passed to encrypt(). Decryption fails
                 if AAD doesn't match.
        """
        ...
