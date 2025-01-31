from contextlib import suppress
import re
from typing import Any

from django.core.cache.backends.base import get_key_func

from valkey.typing import EncodableT

from django_valkey.compressors.identity import IdentityCompressor
from django_valkey.exceptions import CompressorError
from django_valkey.serializers.pickle import PickleSerializer
from django_valkey.typings import KeyT


class CacheKey(str):
    """
    A stub string class that we can use to check if a key was created already.
    """

    def original_key(self) -> str:
        return self.rsplit(":", 1)[1]


def default_reverse_key(key: str) -> str:
    return key.split(":", 2)[2]


def make_key(
    key: KeyT,
    version: int | None = 1,
    prefix: str | None = "",
    key_func=get_key_func(None),
) -> KeyT:
    """Return key as a CacheKey instance so it has additional methods"""
    if isinstance(key, CacheKey):
        return key

    return CacheKey(key_func(key, prefix, version))


special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


def make_pattern(
    pattern: str,
    version: int | None = 1,
    prefix: str | None = "",
    key_func=get_key_func(None),
) -> KeyT:
    if isinstance(pattern, CacheKey):
        return pattern

    prefix = glob_escape(prefix)
    version_str = glob_escape(str(version))

    return CacheKey(key_func(pattern, prefix, version_str))


def decode(
    value: bytes, serializer=PickleSerializer({}), compressor=IdentityCompressor({})
) -> Any:
    """
    Decode the given value with the specified serializer and compressor.
    """
    try:
        value = int(value) if value.isdigit() else float(value)  # type: ignore[assignment]
    except (ValueError, TypeError):
        # Handle little values, chosen to be not compressed
        with suppress(CompressorError):
            value = compressor.decompress(value)
        value = serializer.loads(value)
    except AttributeError:
        # if value is None:
        return value
    return value


def encode(
    value: EncodableT,
    serializer=PickleSerializer({}),
    compressor=IdentityCompressor({}),
) -> bytes | int | float:
    """
    Encode the given value using the specified serializer and compressor.
    """

    if type(value) is not int and type(value) is not float:
        value = serializer.dumps(value)
        return compressor.compress(value)

    return value
