import lzma
from collections.abc import Sequence

from django.conf import settings

from django_valkey.compressors.base import BaseCompressor


class LzmaCompressor(BaseCompressor):
    """
    LzmaCompressor
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.lzma.LzmaCompressor",
                }
            }
        }

    compression parameters:
    to set `preset` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.
    to set `format` use `COMPRESS_LZMA_FORMAT` in your settings, it defaults to 1.
    to set `check` use `COMPRESS_LZMA_CHECK` in your settings, it defaults to -1.
    to set `filters` use `COMPRESS_LZMA_FILTERS` in your settings, it defaults to None.

    decompression parameters:
    to set `memlimit` use `DECOMPRESS_LZMA_MEMLIMIT` in your settings, it defaults to None.
    to set `format` use `DECOMPRESS_LZMA_FORMAT` in your settings, it defaults to 0.
    to set `filters` use `DECOMPRESS_LZMA_FILTERS` in your settings, it defaults to None.
    """

    format = getattr(settings, "COMPRESS_LZMA_FORMAT", 1)
    check = getattr(settings, "COMPRESS_LZMA_CHECK", -1)
    filters: Sequence[dict] | None = getattr(settings, "COMPRESS_LZMA_FILTERS", None)

    memlimit: int | None = getattr(settings, "DECOMPRESS_LZMA_MEMLIMIT", None)
    decomp_format = getattr(settings, "DECOMPRESS_LZMA_FORMAT", 0)
    decomp_filters = getattr(settings, "DECOMPRESS_LZMA_FILTERS", None)

    def _compress(self, value: bytes) -> bytes:
        return lzma.compress(
            value,
            preset=self.level or 4,
            format=self.format or 4,
            check=self.check,
            filters=self.filters,
        )

    def _decompress(self, value: bytes) -> bytes:
        return lzma.decompress(
            value,
            memlimit=self.memlimit,
            format=self.decomp_format,
            filters=self.decomp_filters,
        )
