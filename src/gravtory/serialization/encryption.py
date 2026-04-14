# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""AES-256-GCM authenticated encryption for checkpoint data.

Requires: pip install gravtory[encryption]  (cryptography package)
"""

from __future__ import annotations

import hashlib
import os

from gravtory.core.errors import ConfigurationError, GravtoryError
from gravtory.serialization.serializer import Encryptor

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    _HAS_CRYPTOGRAPHY = True
except ImportError:  # pragma: no cover
    _HAS_CRYPTOGRAPHY = False


_NONCE_SIZE = 12  # 96-bit nonce for AES-GCM
_SALT_SIZE = 16  # 128-bit random salt for PBKDF2
_KDF_ITERATIONS = 600_000
_LEGACY_SALT = b"gravtory-checkpoint-v1"
# Version byte prefixed to encrypted output:
#   0x01 = legacy (static salt)
#   0x02 = v2 (random per-encryption salt prepended)
_VERSION_LEGACY = 0x01
_VERSION_V2 = 0x02


class AES256GCMEncryptor(Encryptor):
    """AES-256-GCM authenticated encryption.

    Each encrypt() call generates a random 12-byte nonce and a random
    16-byte salt for PBKDF2 key derivation.

    Output format (v2):
      version (1 byte) || salt (16 bytes) || nonce (12 bytes) || ciphertext || tag (16 bytes)

    Decryption auto-detects version and handles legacy (static-salt) data.

    Key derivation: PBKDF2-HMAC-SHA256 with 600,000 iterations (OWASP 2024).
    """

    def __init__(self, key: str) -> None:
        """Derive the encryption key once at init time.

        The master key is derived using PBKDF2 with a fixed salt for
        encryption. A fresh random nonce is generated per encrypt() call
        for AES-GCM semantic security.  Decrypt caches derived keys by
        salt to avoid repeated PBKDF2 for legacy/v2 data.

        Args:
            key: User password/key string (any length).
        """
        if not _HAS_CRYPTOGRAPHY:
            raise ConfigurationError(
                "cryptography package is not installed. "
                "Install it with: pip install gravtory[encryption]"
            )
        self._passphrase = key.encode("utf-8")
        # Derive a stable master key once (used for all encrypt() calls)
        self._master_salt = os.urandom(_SALT_SIZE)
        self._master_key = self._derive_key(self._passphrase, self._master_salt)
        # LRU cache for decrypt: salt → derived_key (avoids repeated PBKDF2)
        self._key_cache: dict[bytes, bytes] = {self._master_salt: self._master_key}

    @staticmethod
    def _derive_key(passphrase: bytes, salt: bytes) -> bytes:
        """Derive a 256-bit AES key from passphrase and salt via PBKDF2."""
        return hashlib.pbkdf2_hmac("sha256", passphrase, salt, _KDF_ITERATIONS)

    def _get_or_derive_key(self, salt: bytes) -> bytes:
        """Return cached derived key or derive and cache it."""
        key = self._key_cache.get(salt)
        if key is not None:
            return key
        key = self._derive_key(self._passphrase, salt)
        # Bound the cache to prevent unbounded memory growth
        if len(self._key_cache) >= 64:
            # Evict oldest entry (dict preserves insertion order in 3.7+)
            oldest = next(iter(self._key_cache))
            del self._key_cache[oldest]
        self._key_cache[salt] = key
        return key

    def encrypt(self, data: bytes, aad: bytes | None = None) -> bytes:
        """Encrypt data with AES-256-GCM using the pre-derived master key.

        Returns version + salt + nonce + ciphertext + tag (all concatenated).
        A fresh random nonce per call ensures AES-GCM semantic security.

        When *aad* is provided (e.g. ``b"run_id:step_order"``), the ciphertext
        is cryptographically bound to that context.  Swapping encrypted data
        between different runs/steps will cause decryption to fail.
        """
        aesgcm = AESGCM(self._master_key)
        nonce = os.urandom(_NONCE_SIZE)
        ct: bytes = aesgcm.encrypt(nonce, data, aad)
        return bytes([_VERSION_V2]) + self._master_salt + nonce + ct

    def decrypt(self, data: bytes, aad: bytes | None = None) -> bytes:
        """Decrypt data with AES-256-GCM, auto-detecting format version.

        The same *aad* that was passed to encrypt() must be provided here.

        Raises:
            GravtoryError: On decryption failure (wrong key, corrupted data, AAD mismatch).
        """
        # Minimum valid size is version(1) + nonce(12) = 13 for legacy format.
        if len(data) < 1 + _NONCE_SIZE:
            raise GravtoryError("Encrypted data too short — corrupted or truncated.")

        version = data[0]

        if version == _VERSION_V2:
            # v2 format: version(1) + salt(16) + nonce(12) + ciphertext+tag
            min_len = 1 + _SALT_SIZE + _NONCE_SIZE
            if len(data) < min_len:
                raise GravtoryError("Encrypted data too short for v2 format.")
            salt = data[1 : 1 + _SALT_SIZE]
            nonce = data[1 + _SALT_SIZE : 1 + _SALT_SIZE + _NONCE_SIZE]
            ciphertext = data[1 + _SALT_SIZE + _NONCE_SIZE :]
            derived_key = self._get_or_derive_key(salt)
        elif version == _VERSION_LEGACY:
            # Legacy v1 format: version(1) + nonce(12) + ciphertext+tag
            if len(data) < 1 + _NONCE_SIZE:
                raise GravtoryError("Encrypted data too short for legacy format.")
            nonce = data[1 : 1 + _NONCE_SIZE]
            ciphertext = data[1 + _NONCE_SIZE :]
            derived_key = self._get_or_derive_key(_LEGACY_SALT)
        else:
            raise GravtoryError(
                f"Unknown encryption version byte: 0x{version:02x}. "
                f"Expected 0x{_VERSION_LEGACY:02x} (legacy) or 0x{_VERSION_V2:02x} (v2)."
            )

        aesgcm = AESGCM(derived_key)
        try:
            plaintext: bytes = aesgcm.decrypt(nonce, ciphertext, aad)
        except Exception as exc:
            raise GravtoryError(
                "Decryption failed — wrong key, corrupted data, or AAD mismatch."
            ) from exc
        return plaintext
