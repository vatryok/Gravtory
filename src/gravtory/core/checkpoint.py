# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Checkpoint engine — serialize → compress → encrypt → store pipeline.

The CheckpointEngine orchestrates the full checkpoint pipeline:
  Python object → serialize → compress → encrypt → bytes (store)
  bytes (load) → decrypt → decompress → deserialize → Python object

A 1-byte header is prepended to every payload encoding the pipeline
configuration so that restore() is self-describing — it can deserialize
without knowing the original config.

Header byte layout:
  Bit 0:   compression enabled
  Bit 1:   encryption enabled
  Bits 2-4: serializer type (0=json, 1=msgpack, 2=pickle)
  Bits 5-7: compressor type (0=none, 1=gzip, 2=lz4, 3=zstd)
"""

from __future__ import annotations

import dataclasses
import struct
import zlib
from typing import TYPE_CHECKING, Any

from gravtory.core.errors import ConfigurationError, SerializationError

if TYPE_CHECKING:
    from gravtory.serialization.serializer import Compressor, Encryptor, Serializer

# ── Serializer type IDs ──────────────────────────────────────────

_SER_JSON = 0
_SER_MSGPACK = 1
_SER_PICKLE = 2

_SER_NAMES: dict[int, str] = {_SER_JSON: "json", _SER_MSGPACK: "msgpack", _SER_PICKLE: "pickle"}
_SER_IDS: dict[str, int] = {v: k for k, v in _SER_NAMES.items()}

# ── Compressor type IDs ─────────────────────────────────────────

_COMP_NONE = 0
_COMP_GZIP = 1
_COMP_LZ4 = 2
_COMP_ZSTD = 3

_COMP_NAMES: dict[int, str] = {
    _COMP_NONE: "none",
    _COMP_GZIP: "gzip",
    _COMP_LZ4: "lz4",
    _COMP_ZSTD: "zstd",
}
_COMP_IDS: dict[str, int] = {v: k for k, v in _COMP_NAMES.items()}


def _build_header(ser_id: int, comp_id: int, encrypted: bool) -> int:
    """Encode pipeline config into a single byte."""
    header = 0
    if comp_id != _COMP_NONE:
        header |= 1  # bit 0: compression
    if encrypted:
        header |= 2  # bit 1: encryption
    header |= (ser_id & 0x07) << 2  # bits 2-4: serializer
    header |= (comp_id & 0x07) << 5  # bits 5-7: compressor
    return header


def _parse_header(header: int) -> tuple[int, int, bool]:
    """Decode header byte → (serializer_id, compressor_id, encrypted)."""
    compressed = bool(header & 1)
    encrypted = bool(header & 2)
    ser_id = (header >> 2) & 0x07
    comp_id = (header >> 5) & 0x07
    if not compressed:
        comp_id = _COMP_NONE
    return ser_id, comp_id, encrypted


def _create_serializer(name: str, pickle_allowed_classes: set[str] | None = None) -> Serializer:
    """Instantiate a serializer by name."""
    if name == "json":
        from gravtory.serialization.json import JSONSerializer

        return JSONSerializer()
    if name == "msgpack":
        from gravtory.serialization.msgpack import MsgPackSerializer

        return MsgPackSerializer()
    if name == "pickle":
        from gravtory.serialization.pickle import PickleSerializer

        if pickle_allowed_classes is None:
            raise ConfigurationError(
                "Pickle serializer requires an explicit 'pickle_allowed_classes' set "
                "to restrict deserialization. Pass a set of fully-qualified class names "
                "(e.g. {'builtins.dict', 'builtins.list'}) to CheckpointEngine."
            )
        return PickleSerializer(allowed_classes=pickle_allowed_classes)
    raise ConfigurationError(f"Unknown serializer: {name!r}. Supported: json, msgpack, pickle")


def _create_compressor(name: str | None) -> Compressor | None:
    """Instantiate a compressor by name (None = no compression)."""
    if name is None:
        return None
    if name == "gzip":
        from gravtory.serialization.compression import GzipCompressor

        return GzipCompressor()
    if name == "lz4":
        from gravtory.serialization.compression import LZ4Compressor

        return LZ4Compressor()
    if name == "zstd":
        from gravtory.serialization.compression import ZstdCompressor

        return ZstdCompressor()
    raise ConfigurationError(f"Unknown compressor: {name!r}. Supported: gzip, lz4, zstd")


def _create_encryptor(key: str | None) -> Encryptor | None:
    """Instantiate an encryptor from a key (None = no encryption)."""
    if key is None:
        return None
    from gravtory.serialization.encryption import AES256GCMEncryptor

    return AES256GCMEncryptor(key)


def _serializer_for_id(ser_id: int, pickle_allowed_classes: set[str] | None = None) -> Serializer:
    """Resolve serializer from header id."""
    name = _SER_NAMES.get(ser_id)
    if name is None:
        raise SerializationError(f"Unknown serializer id in header: {ser_id}")
    return _create_serializer(name, pickle_allowed_classes=pickle_allowed_classes)


def _compressor_for_id(comp_id: int) -> Compressor | None:
    """Resolve compressor from header id."""
    if comp_id == _COMP_NONE:
        return None
    name = _COMP_NAMES.get(comp_id)
    if name is None or name == "none":
        raise SerializationError(f"Unknown compressor id in header: {comp_id}")
    return _create_compressor(name)


class CheckpointEngine:
    """Orchestrates the serialize → compress → encrypt → store pipeline.

    Args:
        serializer: Name of serializer ("json", "msgpack", "pickle").
        compression: Name of compressor (None, "gzip", "lz4", "zstd").
        encryption_key: AES-256-GCM key string (None = no encryption).
        auto_compress_threshold: Minimum payload size in bytes before
            compression is applied. Default ``1024`` (1 KB) to avoid
            overhead on small payloads. Set to ``0`` to always compress.
    """

    _DEFAULT_MAX_SIZE = 16 * 1024 * 1024  # 16 MB

    def __init__(
        self,
        serializer: str = "json",
        compression: str | None = None,
        encryption_key: str | None = None,
        auto_compress_threshold: int = 1024,
        max_checkpoint_size: int | None = None,
        pickle_allowed_classes: set[str] | None = None,
    ) -> None:
        self._pickle_allowed_classes = pickle_allowed_classes
        self._serializer = _create_serializer(
            serializer, pickle_allowed_classes=pickle_allowed_classes
        )
        self._compressor = _create_compressor(compression)
        self._encryptor = _create_encryptor(encryption_key)
        self._auto_compress_threshold = auto_compress_threshold
        self._max_size = (
            max_checkpoint_size if max_checkpoint_size is not None else self._DEFAULT_MAX_SIZE
        )

        self._ser_id = _SER_IDS[serializer]
        self._comp_id = _COMP_IDS.get(compression or "none", _COMP_NONE)
        self._encrypted = encryption_key is not None

    def process(self, data: Any, *, aad: bytes | None = None) -> bytes:
        """Serialize → compress → encrypt.

        Returns bytes with a 1-byte header describing the pipeline,
        followed by the payload.

        If ``auto_compress_threshold`` is set and the serialized payload
        is smaller than the threshold, compression is skipped (the header
        records ``comp_id=NONE`` so that ``restore()`` handles it correctly).

        When *aad* is provided (e.g. ``b"run_id:step_order"``), the
        encrypted ciphertext is cryptographically bound to that context.
        Swapping encrypted data between different runs/steps will cause
        ``restore()`` to fail with an AAD mismatch error.
        """
        payload = self._serializer.serialize(data)
        if self._max_size > 0 and len(payload) > self._max_size:
            raise SerializationError(
                f"Checkpoint payload size ({len(payload):,} bytes) exceeds "
                f"max_checkpoint_size ({self._max_size:,} bytes). "
                f"Reduce step output size or increase the limit."
            )
        use_compression = (
            self._compressor is not None and len(payload) >= self._auto_compress_threshold
        )
        if use_compression and self._compressor is not None:
            payload = self._compressor.compress(payload)
            comp_id = self._comp_id
        else:
            comp_id = _COMP_NONE
        if self._encryptor is not None:
            payload = self._encryptor.encrypt(payload, aad=aad)
        header = _build_header(self._ser_id, comp_id, self._encrypted)
        header_byte = bytes([header])
        # CRC-32 covers header + payload for integrity verification
        crc = zlib.crc32(header_byte + payload) & 0xFFFFFFFF
        return header_byte + struct.pack("<I", crc) + payload

    def restore(self, data: bytes, *, aad: bytes | None = None) -> Any:
        """Decrypt → decompress → deserialize.

        Reads the 1-byte header to determine the pipeline, so this
        works even without knowing the original config.

        The same *aad* that was passed to ``process()`` must be provided
        here for encrypted data. Mismatched AAD causes a decryption error.
        """
        if len(data) < 5:
            raise SerializationError("Cannot restore empty or truncated checkpoint data.")
        header = data[0]
        stored_crc = struct.unpack("<I", data[1:5])[0]
        payload = data[5:]
        # Verify CRC-32 integrity before processing
        expected_crc = zlib.crc32(bytes([header]) + payload) & 0xFFFFFFFF
        if stored_crc != expected_crc:
            raise SerializationError(
                f"Checkpoint CRC mismatch: stored=0x{stored_crc:08x}, "
                f"computed=0x{expected_crc:08x}. Data may be corrupted."
            )
        ser_id, comp_id, encrypted = _parse_header(header)

        if encrypted:
            if self._encryptor is None:
                raise SerializationError(
                    "Checkpoint data is encrypted but no encryption key was provided."
                )
            payload = self._encryptor.decrypt(payload, aad=aad)

        compressor = _compressor_for_id(comp_id)
        if compressor is not None:
            payload = compressor.decompress(payload)

        serializer = _serializer_for_id(ser_id, pickle_allowed_classes=self._pickle_allowed_classes)
        return serializer.deserialize(payload)

    def restore_typed(self, data: bytes, output_type: type | None) -> Any:
        """Restore with type-aware deserialization.

        If ``output_type`` is a Pydantic model, reconstruct it via
        ``model_validate()``. If it's a dataclass, construct via ``(**dict)``.
        Otherwise, return the raw deserialized value.
        """
        raw = self.restore(data)
        if output_type is None or raw is None:
            return raw
        # Pydantic v2
        if hasattr(output_type, "model_validate") and isinstance(raw, dict):
            return output_type.model_validate(raw)
        # dataclass
        if dataclasses.is_dataclass(output_type) and isinstance(raw, dict):
            return output_type(**raw)
        return raw
