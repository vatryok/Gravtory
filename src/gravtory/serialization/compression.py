# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Compression engines — Gzip (stdlib), LZ4 (optional), Zstd (optional)."""

from __future__ import annotations

import gzip

from gravtory.core.errors import ConfigurationError
from gravtory.serialization.serializer import Compressor


class GzipCompressor(Compressor):
    """Gzip compressor — always available (stdlib)."""

    def __init__(self, level: int = 6) -> None:
        self._level = level

    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self._level)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


class LZ4Compressor(Compressor):
    """LZ4 — fastest compression. Good for low-latency checkpoints.

    Requires: pip install gravtory[lz4]
    """

    def __init__(self) -> None:
        try:
            import lz4.frame as _lz4  # noqa: F401
        except ImportError:
            raise ConfigurationError(
                "lz4 package is not installed. Install it with: pip install gravtory[lz4]"
            ) from None

    def compress(self, data: bytes) -> bytes:
        import lz4.frame

        result: bytes = lz4.frame.compress(data)
        return result

    def decompress(self, data: bytes) -> bytes:
        import lz4.frame

        result: bytes = lz4.frame.decompress(data)
        return result


class ZstdCompressor(Compressor):
    """Zstandard — best balance of speed and ratio.

    Requires: pip install gravtory[zstd]
    """

    def __init__(self, level: int = 3) -> None:
        try:
            import zstandard as _zstd  # noqa: F401
        except ImportError:
            raise ConfigurationError(
                "zstandard package is not installed. Install it with: pip install gravtory[zstd]"
            ) from None
        self._level = level

    def compress(self, data: bytes) -> bytes:
        import zstandard

        cctx = zstandard.ZstdCompressor(level=self._level)
        result: bytes = cctx.compress(data)
        return result

    def decompress(self, data: bytes) -> bytes:
        import zstandard

        dctx = zstandard.ZstdDecompressor()
        result: bytes = dctx.decompress(data)
        return result
