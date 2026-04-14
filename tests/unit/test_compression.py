"""Unit tests for compression engines."""

from __future__ import annotations

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.serialization.compression import GzipCompressor


class TestGzipRoundTrip:
    def setup_method(self) -> None:
        self.c = GzipCompressor()

    def test_basic(self) -> None:
        data = b"hello world"
        assert self.c.decompress(self.c.compress(data)) == data

    def test_empty(self) -> None:
        assert self.c.decompress(self.c.compress(b"")) == b""

    def test_large(self) -> None:
        data = b"x" * 1_000_000
        compressed = self.c.compress(data)
        assert len(compressed) < len(data)
        assert self.c.decompress(compressed) == data

    def test_reduces_size_for_repetitive(self) -> None:
        data = b"abcdefghij" * 10_000
        compressed = self.c.compress(data)
        assert len(compressed) < len(data) // 2

    def test_binary_data(self) -> None:
        data = bytes(range(256)) * 100
        assert self.c.decompress(self.c.compress(data)) == data

    def test_level_1_fast(self) -> None:
        c = GzipCompressor(level=1)
        data = b"test" * 1000
        assert c.decompress(c.compress(data)) == data

    def test_level_9_best(self) -> None:
        c = GzipCompressor(level=9)
        data = b"test" * 1000
        assert c.decompress(c.compress(data)) == data


class TestLZ4:
    def test_import_or_skip(self) -> None:
        try:
            from gravtory.serialization.compression import LZ4Compressor

            c = LZ4Compressor()
            data = b"hello world" * 100
            assert c.decompress(c.compress(data)) == data
        except ConfigurationError:
            pytest.skip("lz4 not installed")


class TestZstd:
    def test_import_or_skip(self) -> None:
        try:
            from gravtory.serialization.compression import ZstdCompressor

            c = ZstdCompressor()
            data = b"hello world" * 100
            assert c.decompress(c.compress(data)) == data
        except ConfigurationError:
            pytest.skip("zstandard not installed")


class TestCompressionGapFill:
    """Gap-fill tests for compression edge cases."""

    def test_gzip_single_byte(self) -> None:
        c = GzipCompressor()
        assert c.decompress(c.compress(b"\x00")) == b"\x00"

    def test_gzip_all_byte_values(self) -> None:
        c = GzipCompressor()
        data = bytes(range(256))
        assert c.decompress(c.compress(data)) == data

    def test_gzip_unicode_encoded(self) -> None:
        c = GzipCompressor()
        data = "日本語テスト[PARTY]".encode()
        assert c.decompress(c.compress(data)) == data

    def test_gzip_level_comparison(self) -> None:
        """Higher levels produce smaller output for compressible data."""
        data = b"a" * 100_000
        c1 = GzipCompressor(level=1)
        c9 = GzipCompressor(level=9)
        assert len(c9.compress(data)) <= len(c1.compress(data))

    def test_gzip_idempotent_decompress(self) -> None:
        """Decompressing the same data twice gives same result."""
        c = GzipCompressor()
        data = b"test data" * 100
        compressed = c.compress(data)
        assert c.decompress(compressed) == c.decompress(compressed)
