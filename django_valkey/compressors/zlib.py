import platform
import zlib

from django.conf import settings

from django_valkey.compressors.base import BaseCompressor


class ZlibCompressor(BaseCompressor):
    """
    zlib compression
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.zlib.ZlibCompressor",
                }
            }
        }

    compression parameters:
    to set `level` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.

    to set `wbits` use `COMPRESS_ZLIB_WBITS` in your settings, defaults to 15.
    this works for both compression and decompression

    """

    wbits = getattr(settings, "COMPRESS_ZLIB_WBITS", 15)

    def _compress(self, value: bytes) -> bytes:
        if int(platform.python_version_tuple()[1]) >= 11:
            return zlib.compress(value, level=self.level or 6, wbits=self.wbits)
        return zlib.compress(value, level=self.level or 6)

    def _decompress(self, value: bytes) -> bytes:
        return zlib.decompress(value, wbits=self.wbits)
