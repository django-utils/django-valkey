import contextlib
import itertools
import random
import re
import socket
import time
from collections.abc import AsyncGenerator, Iterable, Iterator
from typing import (
    Any,
    Dict,
    List,
    Set,
    Tuple,
    cast,
    TYPE_CHECKING,
    Generic,
    TypeVar,
)

from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT, get_key_func
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey.exceptions import (
    ConnectionError as ValkeyConnectionError,
    ResponseError,
    TimeoutError as ValkeyTimeoutError,
)
from valkey.typing import AbsExpiryT, EncodableT, ExpiryT, KeyT, PatternT

from django_valkey import pool
from django_valkey.base import ATTR_DOES_NOT_EXIST
from django_valkey.compressors.identity import IdentityCompressor
from django_valkey.exceptions import ConnectionInterrupted
from django_valkey.serializers.pickle import PickleSerializer
from django_valkey.util import CacheKey, decode, encode, make_key, make_pattern

if TYPE_CHECKING:
    from valkey.lock import Lock
    from valkey.asyncio.lock import Lock as AsyncLock
    from django_valkey.cache import ValkeyCache


_main_exceptions = (
    ValkeyTimeoutError,
    ResponseError,
    ValkeyConnectionError,
    socket.timeout,
)

special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


Backend = TypeVar("Backend")


class BaseClient(Generic[Backend]):
    def __init__(
        self,
        server: str | Iterable,
        params: Dict[str, Any],
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

        self._clients: List[Backend | Any | None] = [None] * len(self._server)
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

    def _has_compression_enabled(self) -> bool:
        return (
            self._options.get(
                "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
            )
            != "django_valkey.compressors.identity.IdentityCompressor"
        )

    def get_next_client_index(
        self, write: bool = True, tried: List[int] | None = None
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
            not_tried = [i for i in range(0, len(self._server)) if i not in tried]
            return random.choice(not_tried)

        return random.randint(1, len(self._server) - 1)

    def decode(self, value: bytes) -> Any:
        """
        Decode the given value.
        """
        return decode(value, serializer=self._serializer, compressor=self._compressor)

    def encode(self, value: EncodableT) -> bytes | int | float:
        """
        Encode the given value.
        """

        return encode(
            value=value, serializer=self._serializer, compressor=self._compressor
        )

    def _decode_iterable_result(
        self, result: Any, convert_to_set: bool = True
    ) -> List[Any] | Any | None:
        if result is None:
            return None
        if isinstance(result, list):
            if convert_to_set:
                return {self.decode(value) for value in result}
            return [self.decode(value) for value in result]
        return self.decode(result)

    def make_key(
        self, key: KeyT, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        """Return key as a CacheKey instance so it has additional methods"""
        return make_key(
            key,
            key_func=self._backend.key_func,
            version=version or self._backend.version,
            prefix=prefix or self._backend.key_prefix,
        )

    def make_pattern(
        self, pattern: str, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        return make_pattern(
            pattern=pattern,
            key_func=self._backend.key_func,
            version=version or self._backend.version,
            prefix=prefix or self._backend.key_prefix,
        )


class ClientCommands(Generic[Backend]):
    def __contains__(self, key: KeyT) -> bool:
        return self.has_key(key)

    def _get_client(self, write=True, tried=None, client=None, **kwargs):
        if client:
            return client
        return self.get_client(write=write, tried=tried, **kwargs)

    def get_client(
        self: BaseClient,
        write: bool = True,
        tried: List[int] | None = None,
        **kwargs,
    ) -> Backend | Any:
        """
        Method used for obtain a raw valkey client.

        This function is used by almost all cache backend
        operations for obtain a native valkey client/connection
        instance.
        """
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = self.connect(index)

        return self._clients[index]  # type:ignore

    def get_client_with_index(
        self: BaseClient,
        write: bool = True,
        tried: List[int] | None = None,
    ) -> Tuple[Backend | Any, int]:
        """
        Method used for obtain a raw valkey client.

        This function is used by almost all cache backend
        operations for obtain a native valkey client/connection
        instance.
        """
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = self.connect(index)

        return self._clients[index], index  # type:ignore

    def connect(self: BaseClient, index: int = 0) -> Backend | Any:
        """
        Given a connection index, returns a new raw valkey client/connection
        instance. Index is used for replication setups and indicates that
        connection string should be used. In normal setups, index is 0.
        """
        return self.connection_factory.connect(self._server[index])

    def disconnect(
        self: BaseClient, index: int = 0, client: Backend | Any | None = None
    ) -> None:
        """
        delegates the connection factory to disconnect the client
        """
        if client is None:
            client = self._clients[index]

        if client is not None:
            self.connection_factory.disconnect(client)

    def close(self) -> None:
        close_flag = self._options.get(
            "CLOSE_CONNECTION",
            getattr(settings, "DJANGO_VALKEY_CLOSE_CONNECTION", False),
        )
        if close_flag:
            self._close()

    def _close(self) -> None:
        """
        default implementation: Override in custom client
        """
        num_clients = len(self._clients)
        for idx in range(num_clients):
            self.disconnect(index=idx)
        self._clients = [None] * num_clients

    def set(
        self: BaseClient,
        key: KeyT,
        value: EncodableT,
        timeout: int | float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """
        Persist a value to the cache, and set an optional expiration time.

        Also supports optional nx parameter. If set to True - will use valkey
        setnx instead of set.
        """
        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)

        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        original_client = client
        tried: List[int] = []
        while True:
            try:
                if client is None:
                    client, index = self.get_client_with_index(write=True, tried=tried)

                if timeout is not None:
                    # Convert to milliseconds
                    timeout = int(timeout * 1000)

                    if timeout <= 0:
                        if nx:
                            # Using negative timeouts when nx is True should
                            # not expire (in our case delete) the value if it exists.
                            # Obviously expire not existent value is noop.
                            return not self.has_key(key, version=version, client=client)

                        # TODO: check if this is still valid
                        # valkey doesn't support negative timeouts in ex flags
                        # so it seems that it's better to just delete the key
                        # than to set it and then expire in a pipeline
                        return bool(self.delete(key, client=client, version=version))

                return client.set(nkey, nvalue, nx=nx, px=timeout, xx=xx)
            except _main_exceptions as e:
                if (
                    not original_client
                    and not self._replica_read_only
                    and len(tried) < len(self._server)
                ):
                    tried.append(index)
                    client = None
                    continue
                raise ConnectionInterrupted(connection=client) from e

    def incr_version(
        self: BaseClient,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.
        """

        client = self._get_client(write=True, client=client)

        new_key, old_key, value, ttl, version = self._incr_version(
            key, delta, version, client
        )
        self.set(new_key, value, timeout=ttl, client=client)
        self.delete(old_key, client=client)
        return version + delta

    def _incr_version(
        self: BaseClient,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Tuple:
        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        value = self.get(old_key, version=version, client=client)

        try:
            ttl = self.ttl(old_key, version=version, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            error_message = f"Key '{key!r}' not found"
            raise ValueError(error_message)

        if isinstance(key, CacheKey):
            new_key = self.make_key(key.original_key(), version=version + delta)
        else:
            new_key = self.make_key(key, version=version + delta)

        return new_key, old_key, value, ttl, version

    def add(
        self: BaseClient,
        key: KeyT,
        value: EncodableT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        return self.set(key, value, timeout, version=version, client=client, nx=True)

    def get(
        self: BaseClient,
        key: KeyT,
        default: Any | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Any:
        """
        Retrieve a value from the cache.

        Returns decoded value if key is found, the default if not.
        """
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            value = client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            return default

        return self.decode(value)

    def persist(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.persist(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def expire(
        self: BaseClient,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout  # type: ignore

        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        # for some strange reason mypy complains,
        # saying that timeout type is float | timedelta
        try:
            return client.expire(key, timeout)  # type: ignore

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def expire_at(
        self: BaseClient,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when``, which can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.expireat(key, when)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def pexpire(
        self: BaseClient,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout  # type: ignore

        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.pexpire(key, timeout)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def pexpire_at(
        self: BaseClient,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when``, which can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.pexpireat(key, when)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def get_lock(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        client: Backend | Any | None = None,
        lock_class=None,
        thread_local: bool = True,
    ) -> "Lock":
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.lock(
                key,
                timeout=timeout,
                sleep=sleep,
                blocking=blocking,
                blocking_timeout=blocking_timeout,
                lock_class=lock_class,
                thread_local=thread_local,
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    # TODO: delete this in future releases
    lock = get_lock

    def delete(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove a key from the cache.
        """
        key = self.make_key(key, version=version, prefix=prefix)

        client = self._get_client(write=True, client=client, key=key)

        try:
            return client.delete(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete_pattern(
        self: BaseClient,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
        itersize: int | None = None,
    ) -> int:
        """
        Remove all keys matching pattern.
        """

        client = self._get_client(write=True, client=client)

        pattern = self.make_pattern(pattern, version=version, prefix=prefix)

        try:
            count = 0
            pipeline = client.pipeline()

            for key in client.scan_iter(match=pattern, count=itersize):
                pipeline.delete(key)
                count += 1
            pipeline.execute()

            return count
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete_many(
        self: BaseClient,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove multiple keys at once.
        """
        keys = [self.make_key(k, version=version) for k in keys]

        if not keys:
            return 0

        client = self._get_client(write=True, client=client)

        try:
            return client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def clear(self: BaseClient, client: Backend | Any | None = None) -> bool:
        """
        Flush all cache keys.
        """

        client = self._get_client(write=True, client=client)

        try:
            return client.flushdb()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def mget(
        self: BaseClient,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> dict:
        """
        atomic method.
        Retrieve many keys.
        """

        client = self._get_client(write=False, client=client)

        if not keys:
            return {}

        recovered_data = {}

        map_keys = {self.make_key(k, version=version): k for k in keys}

        try:
            results = client.mget(map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)
        return recovered_data

    def get_many(
        self: BaseClient,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | None = None,
    ) -> dict:
        """
        non-atomic bulk method.
        get values of the provided keys
        """
        client = self._get_client(write=False, client=client)

        try:
            pipeline = client.pipeline()
            for key in keys:
                key = self.make_key(key, version=version)
                pipeline.get(key)
            values = pipeline.execute()

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        recovered_data = {}
        for key, value in zip(keys, values):
            if not value:
                continue
            recovered_data[key] = self.decode(value)
        return recovered_data

    def set_many(
        self: BaseClient,
        data: Dict[KeyT, EncodableT],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[bool]:
        """a non-atomic bulk method
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        client = self._get_client(write=True, client=client)

        try:
            res: list[bool] = []
            pipeline = client.pipeline()
            for key, value in data.items():
                res.append(
                    self.set(key, value, timeout, version=version, client=pipeline)
                )
            pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return res

    def mset(
        self: BaseClient,
        data: Dict[KeyT, Any],
        timeout: float | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        """
        an atomic bulk method
        """
        client = self._get_client(write=True, client=client)
        data = {
            self.make_key(k, version=version): self.encode(v) for k, v in data.items()
        }
        try:
            return client.mset(data)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def _incr(
        self: BaseClient,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
        ignore_key_check: bool = False,
    ) -> int:
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        try:
            try:
                # if key expired after exists check, then we get
                # key with wrong value and ttl -1.
                # use lua script for atomicity
                if not ignore_key_check:
                    lua = """
                    local exists = server.call('EXISTS', KEYS[1])
                    if (exists == 1) then
                        return server.call('INCRBY', KEYS[1], ARGV[1])
                    else return false end
                    """
                else:
                    lua = """
                    return server.call('INCRBY', KEYS[1], ARGV[1])
                    """
                value = client.eval(lua, 1, key, delta)
                if value is None:
                    error_message = f"Key '{key!r}' not found"
                    raise ValueError(error_message)
            except ResponseError as e:
                # if cached value or total value is greater than 64-bit signed
                # integer.
                # elif int is encoded. so valkey sees the data as string.
                # In these situations valkey will throw ResponseError

                # try to keep TTL of key
                timeout = self.ttl(key, version=version, client=client)

                # returns -2 if the key does not exist
                # means, that key have expired
                if timeout == -2:
                    error_message = f"Key '{key!r}' not found"
                    raise ValueError(error_message) from e
                value = self.get(key, version=version, client=client) + delta
                self.set(key, value, version=version, timeout=timeout, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return value

    def incr(
        self: BaseClient,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
        ignore_key_check: bool = False,
    ) -> int:
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception. if ignore_key_check=True then the key will be
        created and set to the delta value by default.
        """
        return self._incr(
            key=key,
            delta=delta,
            version=version,
            client=client,
            ignore_key_check=ignore_key_check,
        )

    def decr(
        self: BaseClient,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Decrease delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(key=key, delta=-delta, version=version, client=client)

    def ttl(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            if not client.exists(key):
                return 0

            t = client.ttl(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if t >= 0:
            return t
        if t == -2:
            return 0

        # Should never reach here
        return None

    def pttl(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        key = self.make_key(key, version=version)
        client = self._get_client(write=False, client=client, key=key)

        try:
            if not client.exists(key):
                return 0

            t = client.pttl(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if t >= 0:
            return t
        if t == -2:
            return 0

        # Should never reach here
        return None

    def has_key(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Test if key exists.
        """
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)
        try:
            return client.exists(key) == 1
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def iter_keys(
        self: BaseClient,
        search: str,
        itersize: int | None = None,
        client: Backend | Any | None = None,
        version: int | None = None,
    ) -> Iterator[str]:
        """
        Same as keys, but uses cursors
        to make memory efficient keys iteration.
        """

        client = self._get_client(write=False, client=client)

        pattern = self.make_pattern(search, version=version)
        try:
            for item in client.scan_iter(match=pattern, count=itersize):
                yield self.reverse_key(item.decode())
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def scan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        client: Backend | None = None,
        version: int | None = None,
        **kwargs,
    ) -> tuple[int, list[str]]:
        client = self._get_client(write=False, client=client)
        pattern = self.make_pattern(match, version=version)

        try:
            cursor, result = client.scan(
                cursor=cursor, match=pattern, count=count, _type=_type, **kwargs
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        uses_key = {"string", "zset", "list", "set"}

        if not _type or _type.lower() in uses_key:
            result = [self.reverse_key(res.decode()) for res in result]
        else:
            # in types like hash and stream, scan returns the names, not the keys,
            # so `reverse_key` shouldn't be used
            result = [res.decode() for res in result]
        return cursor, result

    def keys(
        self: BaseClient,
        search: str,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> List[str]:
        """
        Execute KEYS command and return matched results.
        Warning: this can return huge number of results, in
        this case, it strongly recommended use iter_keys
        for it.
        """

        client = self._get_client(write=False, client=client)

        pattern = self.make_pattern(search, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in client.keys(pattern)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def touch(
        self: BaseClient,
        key: KeyT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Sets a new expiration for a key.
        """

        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        if timeout is None:
            try:
                return bool(client.persist(key))

            except _main_exceptions as e:
                raise ConnectionInterrupted(connection=client) from e

        # Convert to milliseconds
        timeout = int(timeout * 1000)
        try:
            return bool(client.pexpire(key, timeout))

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sadd(
        self: BaseClient,
        key: KeyT,
        *values: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        encoded_values = [self.encode(value) for value in values]
        try:
            return client.sadd(key, *encoded_values)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def scard(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            return client.scard(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sdiff(
        self: BaseClient,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return self._decode_iterable_result(
                client.sdiff(*nkeys), convert_to_set=convert_to_set
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sdiffstore(
        self: BaseClient,
        dest: KeyT,
        *keys: KeyT,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        dest = self.make_key(dest, version=version_dest)
        nkeys = [self.make_key(key, version=version_keys) for key in keys]
        try:
            return client.sdiffstore(dest, *nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sinter(
        self: BaseClient,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return self._decode_iterable_result(
                client.sinter(*nkeys), convert_to_set=convert_to_set
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sintercard(
        self,
        *keys: Iterable[KeyT],
        limit: int = 0,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        client = self._get_client(write=False, client=client)
        numkeys = str(len(keys))
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return client.sintercard(numkeys=numkeys, keys=nkeys, limit=limit)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sinterstore(
        self: BaseClient,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        dest = self.make_key(dest, version=version)
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return client.sinterstore(dest, *nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def smismember(
        self: BaseClient,
        key: KeyT,
        *members: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> List[bool]:
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        encoded_members = [self.encode(member) for member in members]

        try:
            return [bool(value) for value in client.smismember(key, *encoded_members)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sismember(
        self: BaseClient,
        key: KeyT,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        member = self.encode(member)
        try:
            return bool(client.sismember(key, member))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def smembers(
        self: BaseClient,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            return self._decode_iterable_result(
                client.smembers(key), convert_to_set=convert_to_set
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def smove(
        self: BaseClient,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        source = self.make_key(source, version=version)
        destination = self.make_key(destination)

        client = self._get_client(write=True, client=client, key=source)

        member = self.encode(member)
        try:
            return client.smove(source, destination, member)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def spop(
        self: BaseClient,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set | list | Any:
        nkey = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=nkey)

        try:
            result = client.spop(nkey, count)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return self._decode_iterable_result(result, convert_to_set=convert_to_set)

    def srandmember(
        self: BaseClient,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> List | Set | Any:
        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            result = client.srandmember(key, count)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return self._decode_iterable_result(result, convert_to_set=convert_to_set)

    def srem(
        self: BaseClient,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        key = self.make_key(key, version=version)

        client = self._get_client(write=True, client=client, key=key)

        nmembers = [self.encode(member) for member in members]
        try:
            return client.srem(key, *nmembers)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sscan(
        self: BaseClient,
        key: KeyT,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
        cursor: int = 0,
        convert_to_set: bool = True,
    ) -> tuple[int, Set[Any] | list[Any]]:
        if self._has_compression_enabled() and match:
            err_msg = "Using match with compression is not supported."
            raise ValueError(err_msg)

        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            cursor, result = client.sscan(
                key,
                cursor=cursor,
                match=cast("PatternT", self.encode(match)) if match else None,
                count=count,
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return cursor, self._decode_iterable_result(
            result, convert_to_set=convert_to_set
        )

    def sscan_iter(
        self: BaseClient,
        key: KeyT,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Iterator[Any]:
        if self._has_compression_enabled() and match:
            err_msg = "Using match with compression is not supported."
            raise ValueError(err_msg)

        key = self.make_key(key, version=version)

        client = self._get_client(write=False, client=client, key=key)

        try:
            for value in client.sscan_iter(
                key,
                match=cast("PatternT", self.encode(match)) if match else None,
                count=count,
            ):
                yield self.decode(value)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sunion(
        self: BaseClient,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return self._decode_iterable_result(
                client.sunion(*nkeys), convert_to_set=convert_to_set
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sunionstore(
        self: BaseClient,
        destination: Any,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        destination = self.make_key(destination, version=version)
        encoded_keys = [self.make_key(key, version=version) for key in keys]
        try:
            return client.sunionstore(destination, *encoded_keys)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hdel(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove keys from hash name.
        Returns the number of fields deleted from the hash.
        """
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return client.hdel(name, nkey)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hexists(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return client.hexists(name, nkey)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hget(
        self: BaseClient,
        name: str,
        key: KeyT,
        default: Any | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Any:
        """
        Return the value of the hash name at key, or return the default value
        """
        client = self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        try:
            result = client.hget(name=name, key=nkey)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if not result:
            return default

        value = self.decode(result)
        return value

    def hgetall(
        self: BaseClient,
        name: str,
        client: Backend | None = None,
    ) -> dict[str, Any]:
        """
        Return all key and values in the hash
        """
        client = self._get_client(write=False, client=client)
        try:
            raw_results = client.hgetall(name=name)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        results = {
            self.reverse_key(k.decode()): self.decode(v) for k, v in raw_results.items()
        }
        return results

    def hincrby(
        self: BaseClient,
        name: str,
        key: KeyT,
        amount: int = 1,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        """
        Increment the value of key in hash
        returns the new value
        """
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return client.hincrby(name=name, key=nkey, amount=amount)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hincrbyfloat(
        self: BaseClient,
        name: str,
        key: KeyT,
        amount: float = 1.0,
        version: int | None = None,
        client: Backend | None = None,
    ) -> float:
        """
        Increment the value of key in hash
        returns the new value
        """
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return client.hincrbyfloat(name=name, key=nkey, amount=amount)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hkeys(
        self: BaseClient,
        name: str,
        client: Backend | Any | None = None,
    ) -> List[Any]:
        """
        Return a list of keys in hash name.
        """
        client = self._get_client(write=False, client=client)
        try:
            return [self.reverse_key(k.decode()) for k in client.hkeys(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hlen(
        self: BaseClient,
        name: str,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Return the number of items in hash name.
        """
        client = self._get_client(write=False, client=client)
        try:
            return client.hlen(name)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hset(
        self: BaseClient,
        name: str,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: dict[KeyT, EncodableT] | None = None,
        items: list[KeyT, EncodableT] | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Set the value of hash name at key to value.
        :param key: a single key for single operation
        :param value: a single value for a single key
        :param mapping: a dict, dict keys are hash keys and values are hash value, can do bulk operation
        :param items: a list, items on even indexes are keys (0, 2, 4...), items at odd indexes are values (1, 3, 4)
        Returns the number of fields added to the hash.
        """
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version) if key and value else None
        nvalue = self.encode(value) if key and value else None
        nmapping = (
            {
                self.make_key(k, version=version): self.encode(v)
                for k, v in mapping.items()
            }
            if mapping
            else None
        )
        # items in even indexes are keys, items in odd indexes are values
        nitems = (
            [
                self.make_key(v, version=version) if i % 2 == 0 else self.encode(v)
                for i, v in enumerate(items)
            ]
            if items
            else None
        )
        try:
            return client.hset(
                name, key=nkey, value=nvalue, mapping=nmapping, items=nitems
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hsetnx(
        self: BaseClient,
        name: str,
        key: KeyT,
        value: EncodableT,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        """
        Set the value of hash at key, if key doesn't exist
        Returns the number of fields added to the hash.
        """
        client = self._get_client(write=True, client=client)

        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)

        try:
            return client.hsetnx(name=name, key=nkey, value=nvalue)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hmget(
        self: BaseClient,
        name: str,
        key: KeyT | list[KeyT],
        *args,
        version: int | None = None,
        client: Backend | None = None,
    ) -> dict[KeyT, EncodableT]:
        client = self._get_client(write=False, client=client)

        recovered_data = {}

        map_keys = {
            self.make_key(k, version=version): k
            for k in itertools.chain(args, [key] if isinstance(key, str) else key)
        }

        try:
            results = client.hmget(name=name, keys=map_keys)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)

        return recovered_data

    def hvals(
        self: BaseClient, name: str, client: Backend | None = None
    ) -> list[EncodableT]:
        """
        Returns all values in hash.
        """
        client = self._get_client(write=False, client=client)

        try:
            results = client.hvals(name=name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        nresults = [self.decode(value) for value in results]
        return nresults

    def hstrlen(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        client = self._get_client(write=False, client=client)

        nkey = self.make_key(key, version=version)

        try:
            return client.hstrlen(name=name, key=nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e


class AsyncClientCommands(Generic[Backend]):
    def __getattr__(self, item):
        if item.startswith("a"):
            attr = getattr(self, item[1:], ATTR_DOES_NOT_EXIST)
            if attr is not ATTR_DOES_NOT_EXIST:
                return attr
        raise AttributeError(
            f"{self.__class__.__name__} object has no attribute {item}"
        )

    async def _get_client(self, write=True, tried=None, client=None):
        if client:
            return client
        return await self.get_client(write, tried)

    async def get_client(
        self,
        write: bool = True,
        tried: list[int] | None = None,
    ) -> Backend | Any:
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index]

    async def get_client_with_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> tuple[Backend, int]:
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index], index

    async def connect(self, index: int = 0) -> Backend | Any:
        return await self.connection_factory.connect(self._server[index])

    async def disconnect(self, index: int = 0, client=None):
        if client is None:
            client = self._clients[index]

        if client is not None:
            await self.connection_factory.disconnect(client)

    async def close(self) -> None:
        close_flag = self._options.get(
            "CLOSE_CONNECTION",
            getattr(settings, "DJANGO_VALKEY_CLOSE_CONNECTION", False),
        )
        if close_flag:
            await self._close()

    async def _close(self) -> None:
        """
        default implementation: Override in custom client
        """
        num_clients = len(self._clients)
        for index in range(num_clients):
            # TODO: check disconnect and close
            await self.disconnect(index=index)
        self._clients = [None] * num_clients

    async def set(
        self,
        key,
        value,
        timeout=DEFAULT_TIMEOUT,
        version=None,
        client: Backend | Any | None = None,
        nx=False,
        xx=False,
    ) -> bool:
        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)

        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        original_client = client
        tried = []

        while True:
            try:
                if client is None:
                    client, index = await self.get_client_with_index(
                        write=True, tried=tried
                    )

                if timeout is not None:
                    # convert to milliseconds
                    timeout = int(timeout) * 1000

                    if timeout <= 0:
                        if nx:
                            return not await self.has_key(
                                key, version=version, client=client
                            )

                        return bool(
                            await self.delete(key, client=client, version=version)
                        )
                return await client.set(nkey, nvalue, nx=nx, px=timeout, xx=xx)

            except _main_exceptions as e:
                if (
                    not original_client
                    and not self._replica_read_only
                    and len(tried) < len(self._server)
                ):
                    tried.append(index)
                    client = None
                    continue

                raise ConnectionInterrupted(connection=client) from e

    async def incr_version(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: Backend | None | Any = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)

        new_key, old_key, value, ttl, version = await self._incr_version(
            key, delta, version, client
        )

        await self.set(new_key, value, timeout=ttl, client=client)
        await self.delete(old_key, client=client)
        return version + delta

    async def _incr_version(self, key, delta, version, client) -> tuple:
        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        value = await self.get(old_key, version=version, client=client)

        ttl = await self.ttl(old_key, version=version, client=client)

        if value is None:
            error_message = f"Key '{old_key!r}' does not exist"
            raise ValueError(error_message)

        if isinstance(key, CacheKey):
            new_key = self.make_key(key.original_key(), version=version + delta)
        else:
            new_key = self.make_key(key, version=version + delta)

        return new_key, old_key, value, ttl, version

    async def add(
        self,
        key,
        value,
        timeout: float | int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        return await self.set(
            key, value, timeout, version=version, client=client, nx=True
        )

    async def get(
        self,
        key,
        default: Any | None = None,
        version: int | None = None,
        client: Backend | None | Any = None,
    ) -> Any:
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)

        try:
            value = await client.get(key)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            return default

        return self.decode(value)

    async def persist(
        self, key, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)
        key = self.make_key(key, version=version)

        try:
            return await client.persist(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def expire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        try:
            return await client.expire(key, timeout)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def expire_at(
        self, key, when, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        try:
            return await client.expireat(key, when)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def pexpire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        try:
            return await client.pexpire(key, timeout)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def pexpire_at(
        self, key, when, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        try:
            return await client.pexpireat(key, when)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def get_lock(
        self,
        key,
        version: int | None = None,
        timeout: float | int | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        client: Backend | Any | None = None,
        lock_class=None,
        thread_local: bool = True,
    ) -> "AsyncLock":
        """Returns a Lock object, the object then should be used in an async context manager"""

        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        try:
            return client.lock(
                key,
                timeout=timeout,
                sleep=sleep,
                blocking=blocking,
                blocking_timeout=blocking_timeout,
                lock_class=lock_class,
                thread_local=thread_local,
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    # TODO: delete this in future releases
    lock = aget_lock = get_lock

    async def delete(
        self,
        key,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)

        try:
            return await client.delete(
                self.make_key(key, version=version, prefix=prefix)
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
        itersize: int | None = None,
    ) -> int:
        """
        Remove all keys matching a pattern.
        """
        client = await self._get_client(write=True, client=client)

        pattern = self.make_pattern(pattern, version=version, prefix=prefix)

        try:
            count = 0
            pipeline = await client.pipeline()

            async with contextlib.aclosing(
                client.scan_iter(match=pattern, count=itersize)
            ) as values:
                async for key in values:
                    await pipeline.delete(key)
                    count += 1
                await pipeline.execute()

            return count

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def delete_many(
        self, keys, version: int | None = None, client: Backend | None = None
    ) -> int:
        """Remove multiple keys at once."""
        keys = [self.make_key(k, version=version) for k in keys]

        if not keys:
            return 0

        client = await self._get_client(write=True, client=client)

        try:
            return await client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def clear(self, client: Backend | Any | None = None) -> bool:
        """
        Flush all cache keys.
        """

        client = await self._get_client(write=True, client=client)

        try:
            return await client.flushdb()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def mget(
        self, keys, version: int | None = None, client: Backend | Any | None = None
    ) -> dict:
        if not keys:
            return {}

        client = await self._get_client(write=False, client=client)

        recovered_data = {}

        map_keys = {self.make_key(k, version=version): k for k in keys}

        try:
            results = await client.mget(*map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)

        return recovered_data

    async def get_many(
        self,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | None = None,
    ):
        """
        non-atomic bulk method.
        get values of the provided keys.
        """
        client = await self._get_client(write=False, client=client)

        try:
            pipeline = await client.pipeline()
            for key in keys:
                key = self.make_key(key, version=version)
                await pipeline.get(key)
            values = await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        recovered_data = {}
        for key, value in zip(keys, values):
            if not value:
                continue
            recovered_data[key] = self.decode(value)
        return recovered_data

    async def set_many(
        self,
        data: dict,
        timeout: float | int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[bool]:
        client = await self._get_client(write=True, client=client)

        try:
            res: list[bool] = []
            pipeline = await client.pipeline()
            for key, value in data.items():
                res.append(
                    await self.set(
                        key, value, timeout, version=version, client=pipeline
                    )
                )
            await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return res

    async def mset(
        self,
        data: Dict[KeyT, Any],
        timeout: float | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        client = await self._get_client(write=True, client=client)

        data = {
            self.make_key(k, version=version): self.encode(v) for k, v in data.items()
        }

        try:
            return await client.mset(data)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def _incr(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
        ignoree_key_check: bool = False,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        key = self.make_key(key, version=version)

        try:
            try:
                # if key expired after exists check, then we get
                # key with wrong value and ttl -1.
                # use lua script for atomicity
                if not ignoree_key_check:
                    lua = """
                    local exists = server.call('EXISTS', KEYS[1])
                    if (exists == 1) then
                        return server.call('INCRBY', KEYS[1], ARGV[1])
                    else return false end
                    """
                else:
                    lua = """
                    return server.call('INCRBY', KEYS[1], ARGV[1])
                    """
                value = await client.eval(lua, 1, key, delta)
                if value is None:
                    error_message = f"Key '{key!r}' not found"
                    raise ValueError(error_message)
            except ResponseError as e:
                # if cached value or total value is greater than 64-bit signed
                # integer.
                # elif int is encoded. so valkey sees the data as string.
                # In these situations valkey will throw ResponseError

                # try to keep TTL of key
                timeout = await self.ttl(key, version=version, client=client)

                if timeout == -2:
                    error_message = f"Key '{key!r}' not found"
                    raise ValueError(error_message) from e
                value = await self.get(key, version=version, client=client) + delta
                await self.set(
                    key, value, version=version, timeout=timeout, client=client
                )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return value

    async def incr(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
        ignore_key_check: bool = False,
    ) -> int:
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception. if ignore_key_check=True then the key will be
        created and set to the delta value by default.
        """
        return await self._incr(
            key,
            delta,
            version=version,
            client=client,
            ignoree_key_check=ignore_key_check,
        )

    async def decr(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Decrease delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return await self._incr(key, delta=-delta, version=version, client=client)

    async def ttl(
        self, key, version: int | None = None, client: Backend | Any | None = None
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self._get_client(write=False, client=client)
        key = self.make_key(key, version=version)
        try:
            if not await client.exists(key):
                return 0

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        try:
            t = await client.ttl(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    async def pttl(
        self, key, version: int | None = None, client: Backend | Any | None = None
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        try:
            if not await client.exists(key):
                return 0
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        try:
            t = await client.pttl(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    async def has_key(
        self, key, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        """
        Test if key exists.
        """
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        try:
            return await client.exists(key) == 1
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def iter_keys(
        self,
        search: str,
        itersize: int | None = None,
        client: Backend | Any | None = None,
        version: int | None = None,
    ) -> AsyncGenerator[str, None]:
        """
        Same as keys, but uses cursors
        to make memory efficient keys iteration.
        """
        client = await self._get_client(write=False, client=client)
        pattern = self.make_pattern(search, version=version)
        async with contextlib.aclosing(
            client.scan_iter(match=pattern, count=itersize)
        ) as values:
            try:
                async for item in values:
                    yield self.reverse_key(item.decode())

            except _main_exceptions as e:
                raise ConnectionInterrupted(connection=client) from e

    async def scan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        client: Backend | None = None,
        version: int | None = None,
        **kwargs,
    ) -> tuple[int, list[str]]:
        client = await self._get_client(write=False, client=client)
        pattern = self.make_pattern(match, version=version)
        try:
            cursor, result = await client.scan(
                cursor=cursor, match=pattern, count=count, _type=_type, **kwargs
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        uses_key = {"string", "zset", "list", "set"}

        if not _type or _type.lower() in uses_key:
            result = [self.reverse_key(res.decode()) for res in result]
        else:
            # in types like hash and stream, scan returns the names, not the keys,
            # so `reverse_key` shouldn't be used
            result = [res.decode() for res in result]

        return cursor, result

    async def keys(
        self,
        search: str,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[Any]:
        client = await self._get_client(write=False, client=client)

        pattern = self.make_pattern(search, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in await client.keys(pattern)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def touch(
        self,
        key,
        timeout: float | int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Sets a new expiration for a key.
        """
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)
        if timeout is None:
            try:
                return bool(await client.persist(key))
            except _main_exceptions as e:
                raise ConnectionInterrupted(connection=client) from e

        # convert timeout to milliseconds
        timeout = int(timeout * 1000)
        try:
            return bool(await client.pexpire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sadd(
        self,
        key,
        *values: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        key = self.make_key(key, version=version)
        encoded_values = [self.encode(value) for value in values]

        try:
            return await client.sadd(key, *encoded_values)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def scard(
        self, key, version: int | None = None, client: Backend | Any | None = None
    ) -> int:
        client = await self._get_client(write=False, client=client)
        key = self.make_key(key, version=version)
        try:
            return await client.scard(key)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sdiff(
        self,
        *keys,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            results = await client.sdiff(*nkeys)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return self._decode_iterable_result(results, convert_to_set=convert_to_set)

    async def sdiffstore(
        self,
        dest,
        *keys,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        dest = self.make_key(dest, version=version_dest)
        nkeys = [self.make_key(key, version=version_keys) for key in keys]
        try:
            return await client.sdiffstore(dest, *nkeys)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sinter(
        self,
        *keys,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            results = await client.sinter(*nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return self._decode_iterable_result(results, convert_to_set=convert_to_set)

    async def sintercard(
        self,
        *keys: Iterable[KeyT],
        limit: int = 0,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)
        numkeys = str(len(keys))
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            return await client.sintercard(numkeys=numkeys, keys=nkeys, limit=limit)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sinterstore(
        self,
        dest,
        *keys,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        dest = self.make_key(dest, version=version)
        nkeys = [self.make_key(key, version=version) for key in keys]

        try:
            return await client.sinterstore(dest, *nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def smismember(
        self,
        key,
        *members: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[bool]:
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        encoded_members = [self.encode(member) for member in members]

        try:
            return [
                bool(value) for value in await client.smismember(key, *encoded_members)
            ]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sismember(
        self,
        key,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        member = self.encode(member)
        try:
            return bool(await client.sismember(key, member))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def smembers(
        self,
        key,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        try:
            results = await client.smembers(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return self._decode_iterable_result(results, convert_to_set=convert_to_set)

    async def smove(
        self,
        source,
        destination,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        client = await self._get_client(write=False, client=client)
        source = self.make_key(source, version=version)
        destination = self.make_key(destination, version=version)
        member = self.encode(member)
        try:
            return await client.smove(source, destination, member)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def spop(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set | list | Any:
        client = await self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            result = await client.spop(nkey, count)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return self._decode_iterable_result(result, convert_to_set=convert_to_set)

    async def srandmember(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> list | Set | Any:
        client = await self._get_client(write=False, client=client)
        key = self.make_key(key, version=version)
        try:
            result = await client.srandmember(key, count)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return self._decode_iterable_result(result, convert_to_set=convert_to_set)

    async def srem(
        self,
        key,
        *members,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        nmembers = [self.encode(member) for member in members]
        try:
            return await client.srem(key, *nmembers)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def sscan(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
        cursor: int = 0,
        convert_to_set: bool = True,
    ) -> tuple[int, Set[Any] | list[Any]]:
        # TODO check this is correct
        if self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        try:
            cursor, result = await client.sscan(
                key,
                cursor=cursor,
                match=cast("PatternT", self.encode(match)) if match else None,
                count=count,
            )

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return cursor, self._decode_iterable_result(
            result, convert_to_set=convert_to_set
        )

    async def sscan_iter(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
    ):
        if self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)

        async with contextlib.aclosing(
            client.sscan_iter(
                key,
                match=cast("PatternT", self.encode(match)) if match else None,
                count=count,
            )
        ) as values:
            try:
                async for value in values:
                    yield self.decode(value)

            except _main_exceptions as e:
                raise ConnectionInterrupted(connection=client) from e

    async def sunion(
        self,
        *keys,
        version: int | None = None,
        client: Backend | Any | None = None,
        convert_to_set: bool = True,
    ) -> Set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            results = await client.sunion(*nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return self._decode_iterable_result(results, convert_to_set=convert_to_set)

    async def sunionstore(
        self,
        destination: Any,
        *keys,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        destination = self.make_key(destination, version=version)
        encoded_keys = [self.make_key(key, version=version) for key in keys]
        try:
            return await client.sunionstore(destination, *encoded_keys)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hdel(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove keys from hash name.
        Returns the number of fields deleted from the hash.
        """
        client = await self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hdel(name, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hexists(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = await self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hexists(name, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hget(
        self,
        name: str,
        key: KeyT,
        default: Any | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Any:
        """
        Return the value of the hash name at key, or return the default value
        """
        client = await self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        try:
            result = await client.hget(name=name, key=nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        if not result:
            return default

        value = self.decode(result)
        return value

    async def hgetall(self, name: str, client: Backend | None = None) -> dict[str, Any]:
        """
        Return all key and values in the hash
        """
        client = await self._get_client(write=False, client=client)
        try:
            raw_results = await client.hgetall(name=name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        results = {
            self.reverse_key(k.decode()): self.decode(v) for k, v in raw_results.items()
        }
        return results

    async def hincrby(
        self,
        name: str,
        key: KeyT,
        amount: int = 1,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        """
        Increment the value of key in hash
        returns the new value
        """
        client = await self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hincrby(name=name, key=nkey, amount=amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hincrbyfloat(
        self,
        name: str,
        key: KeyT,
        amount: float = 1.0,
        version: int | None = None,
        client: Backend | None = None,
    ) -> float:
        """
        Increment the value of key in hash
        returns the new value
        """
        client = await self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hincrbyfloat(name=name, key=nkey, amount=amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hkeys(self, name: str, client: Backend | Any | None = None) -> list[Any]:
        client = await self._get_client(write=False, client=client)

        try:
            return [self.reverse_key(k.decode()) for k in await client.hkeys(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hlen(self, name: str, client: Backend | Any | None = None) -> int:
        """
        Return the number of items in hash name.
        """
        client = await self._get_client(write=False, client=client)
        try:
            return await client.hlen(name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hset(
        self,
        name: str,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: dict[KeyT, EncodableT] | None = None,
        items: list[tuple[KeyT, EncodableT]] | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Set the value of hash name at key to value.
        :param key: a single key for single operation
        :param value: a single value for a single key
        :param mapping: a dict, dict keys are hash keys and values are hash value, can do bulk operation
        :param items: a list, items on even indexes are keys (0, 2, 4...), items at odd indexes are values (1, 3, 4)
        Returns the number of fields added to the hash.
        """
        client = await self._get_client(write=True, client=client)

        nkey = self.make_key(key, version=version) if key and value else None
        nvalue = self.encode(value) if key and value else None

        nmapping = (
            {
                self.make_key(k, version=version): self.encode(v)
                for k, v in mapping.items()
            }
            if mapping
            else None
        )

        # items in even indexes are keys, items in odd indexes are values
        nitems = (
            [
                self.make_key(v, version=version) if i % 2 == 0 else self.encode(v)
                for i, v in enumerate(items)
            ]
            if items
            else None
        )

        try:
            return await client.hset(
                name, key=nkey, value=nvalue, mapping=nmapping, items=nitems
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hsetnx(
        self,
        name: str,
        key: KeyT,
        value: EncodableT,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        """
        Set the value of hash at key, if key doesn't exist
        Returns the number of fields added to the hash.
        """
        client = await self._get_client(write=True, client=client)

        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)

        try:
            return await client.hsetnx(name=name, key=nkey, value=nvalue)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hmget(
        self,
        name: str,
        key: KeyT | list[KeyT],
        *args,
        version: int | None = None,
        client: Backend | None = None,
    ) -> dict[KeyT, EncodableT]:
        client = await self._get_client(write=False, client=client)

        recovered_data = {}

        map_keys = {
            self.make_key(k, version=version): k
            for k in itertools.chain(args, [key] if isinstance(key, str) else key)
        }

        try:
            results = await client.hmget(name=name, keys=map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)

        return recovered_data

    async def hvals(self, name: str, client: Backend | None = None) -> list[EncodableT]:
        """
        Returns all values in hash.
        """
        client = await self._get_client(write=False, client=client)

        try:
            results = await client.hvals(name=name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        nresults = [self.decode(value) for value in results]
        return nresults

    async def hstrlen(
        self,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)

        nkey = self.make_key(key, version=version)
        try:
            return await client.hstrlen(name=name, key=nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e


# Herd related code:
class Marker:
    """
    Dummy class for use as
    marker for herded keys.
    """

    pass


def _is_expired(x, herd_timeout: int) -> bool:
    if x >= herd_timeout:
        return True
    val = x + random.randint(1, herd_timeout)

    if val >= herd_timeout:
        return True
    return False


class HerdCommonMethods:
    def __init__(self, *args, **kwargs):
        self._marker = Marker()
        self._herd_timeout: int = getattr(settings, "CACHE_HERD_TIMEOUT", 60)
        super().__init__(*args, **kwargs)

    def _pack(self, value: Any, timeout) -> tuple[Marker, Any, int]:
        herd_timeout = (timeout or self._backend.default_timeout) + int(time.time())
        return self._marker, value, herd_timeout

    def _unpack(self, value: tuple[Marker, Any, int]) -> tuple[Any, bool]:
        try:
            marker, unpacked, herd_timeout = value
        except (ValueError, TypeError):
            return value, False

        if not isinstance(marker, Marker):
            return value, False

        now = time.time()
        if herd_timeout < now:
            x = now - herd_timeout
            return unpacked, _is_expired(x, self._herd_timeout)

        return unpacked, False
