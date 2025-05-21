import re
from contextlib import suppress
from typing import Any

from valkey.typing import KeyT, EncodableT

from django_valkey.compressors.base import BaseCompressor
from django_valkey.exceptions import CompressorError
from django_valkey.serializers.base import BaseSerializer


class CacheKey(str):
    """
    A stub string class that we can use to check if a key was created already.
    """

    def original_key(self) -> str:
        return self.rsplit(":", 1)[1]


def default_reverse_key(key: str) -> str:
    return key.split(":", 2)[2]


special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


def make_key(
    key: KeyT | None, key_func, version: int | None = None, prefix: str | None = None
) -> CacheKey | None:
    if not key:
        return key

    if isinstance(key, CacheKey):
        return key

    return CacheKey(key_func(key, prefix, version))


def make_pattern(
    pattern: str | None, key_func, version: int | None = None, prefix: str | None = None
) -> CacheKey | None:
    if not pattern:
        return pattern

    if isinstance(pattern, CacheKey):
        return pattern

    prefix = glob_escape(prefix)
    version_str = glob_escape(str(version))

    return CacheKey(key_func(pattern, prefix, version_str))


def encode(
    value: EncodableT, serializer: BaseSerializer, compressor: BaseCompressor
) -> bytes | int | float:
    if type(value) not in {int, float}:
        value = serializer.dumps(value)
        return compressor.compress(value)

    return value


def decode(value: bytes, serializer: BaseSerializer, compressor: BaseCompressor) -> Any:
    try:
        if value.isdigit():
            value = int(value)
        else:
            value = float(value)

    except (ValueError, TypeError):
        with suppress(CompressorError):
            value = compressor.decompress(value)
        value = serializer.loads(value)

    return value
