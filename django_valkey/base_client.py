import builtins
from collections.abc import Iterable
import random
import re
import socket
from contextlib import suppress
from typing import (
    Any,
    TYPE_CHECKING,
)

from django.conf import settings
from django.core.cache.backends.base import get_key_func
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey import Valkey
from valkey.asyncio import Valkey as AValkey
from valkey.cluster import ValkeyCluster
from valkey.exceptions import ConnectionError, ResponseError, TimeoutError
from valkey.typing import EncodableT

from django_valkey import pool
from django_valkey.compressors.identity import IdentityCompressor
from django_valkey.exceptions import CompressorError
from django_valkey.serializers.pickle import PickleSerializer
from django_valkey.util import CacheKey
from django_valkey.typing import KeyT

if TYPE_CHECKING:
    from django_valkey.cache import ValkeyCache


_main_exceptions = (TimeoutError, ResponseError, ConnectionError, socket.timeout)

special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


class BaseClient:
    def __init__(
        self,
        server: str | Iterable,
        params: dict[str, Any],
        backend: "ValkeyCache",
    ) -> None:
        self._backend = backend
        self._server = server
        if not self._server:
            error_message = "Missing connections string"
            raise ImproperlyConfigured(error_message)
        if not isinstance(self._server, (list, tuple, set)):
            self._server = self._server.split(",")

        self._params = params

        self.reverse_key = get_key_func(
            params.get("REVERSE_KEY_FUNCTION")
            or "django_valkey.util.default_reverse_key"
        )

        self._clients: list[Valkey | AValkey | ValkeyCluster | None] = [None] * len(
            self._server
        )
        self._options: dict = params.get("OPTIONS", {})
        self._replica_read_only = self._options.get("REPLICA_READ_ONLY", True)

        serializer_path = self._options.get(
            "SERIALIZER", "django_valkey.serializers.pickle.PickleSerializer"
        )
        serializer_cls = import_string(serializer_path)

        compressor_path = self._options.get(
            "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
        )
        compressor_cls = import_string(compressor_path)

        self._serializer: PickleSerializer | Any = serializer_cls(options=self._options)
        self._compressor: IdentityCompressor | Any = compressor_cls(
            options=self._options
        )

        self._connection_factory = getattr(
            settings, "DJANGO_VALKEY_CONNECTION_FACTORY", self.CONNECTION_FACTORY_PATH
        )
        self.connection_factory = pool.get_connection_factory(
            options=self._options, path=self._connection_factory
        )

    def __contains__(self, key: KeyT) -> bool:
        return self.has_key(key)

    def _has_compression_enabled(self) -> bool:
        return (
            self._options.get(
                "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
            )
            != "django_valkey.compressors.identity.IdentityCompressor"
        )

    def get_next_client_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> int:
        """
        Return a next index for read client. This function implements a default
        behavior for get a next read client for a replication setup.

        Overwrite this function if you want a specific
        behavior.
        """
        if write or len(self._server) == 1:
            return 0

        if tried is None:
            tried = []

        if tried and len(tried) < len(self._server):
            not_tried = [i for i in range(len(self._server)) if i not in tried]
            return random.choice(not_tried)

        return random.randint(1, len(self._server) - 1)

    def decode(self, value: bytes) -> Any:
        """
        Decode the given value.
        """
        try:
            value = int(value) if value.isdigit() else float(value)
        except (ValueError, TypeError):
            # Handle little values, chosen to be not compressed
            with suppress(CompressorError):
                value = self._compressor.decompress(value)
            value = self._serializer.loads(value)
        except AttributeError:
            # if value is None:
            return value
        return value

    def encode(self, value: EncodableT) -> bytes | int | float:
        """
        Encode the given value.
        """

        if type(value) is not int and type(value) is not float:
            value = self._serializer.dumps(value)
            return self._compressor.compress(value)

        return value

    def make_key(
        self, key: KeyT, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        """Return key as a CacheKey instance so it has additional methods"""
        if isinstance(key, CacheKey):
            return key

        if prefix is None:
            prefix = self._backend.key_prefix

        if version is None:
            version = self._backend.version

        return CacheKey(self._backend.key_func(key, prefix, version))

    def make_pattern(
        self, pattern: str, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        if isinstance(pattern, CacheKey):
            return pattern

        if prefix is None:
            prefix = self._backend.key_prefix
        prefix = glob_escape(prefix)

        if version is None:
            version = self._backend.version
        version_str = glob_escape(str(version))

        return CacheKey(self._backend.key_func(pattern, prefix, version_str))

    def _decode_iterable_result(
        self, result: Any, convert_to_set: bool = True
    ) -> list[Any] | builtins.set[Any] | Any | None:
        if result is None:
            return None
        if isinstance(result, list):
            if convert_to_set:
                return {self.decode(value) for value in result}
            return [self.decode(value) for value in result]
        return self.decode(result)
