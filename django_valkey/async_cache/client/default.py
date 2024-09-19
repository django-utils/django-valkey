import contextlib
import random
from contextlib import suppress
from typing import Any, Iterable, Set, cast, TYPE_CHECKING, AsyncGenerator

from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT, get_key_func
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey.asyncio import Valkey as AValkey
from valkey.exceptions import ResponseError
from valkey.typing import PatternT

from django_valkey import pool
from django_valkey.client.default import special_re, _main_exceptions, glob_escape
from django_valkey.exceptions import CompressorError, ConnectionInterrupted
from django_valkey.util import CacheKey

if TYPE_CHECKING:
    from django_valkey.async_cache.cache import AsyncValkeyCache
    from django_valkey.async_cache.pool import (
        AsyncConnectionFactory,
        AsyncSentinelConnectionFactory,
    )


async def glove_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


class AsyncDefaultClient:
    def __init__(
        self,
        server: str | Iterable,
        params: dict[str, Any],
        backend: "AsyncValkeyCache",
    ) -> None:
        self._backend = backend
        self._server = server
        if not self._server:
            error_message = "Missing connections string"
            raise ImproperlyConfigured(error_message)
        if not isinstance(self._server, (list, tuple, set)):
            self._server = self._server.split(",")

        self.params = params

        self.reverse_key = get_key_func(
            params.get("REVERSE_KEY_FUNCTION")
            or "django_valkey.util.default_reverse_key"
        )

        self._clients = [None] * len(self._server)
        self._options = params.get("OPTIONS", {})
        self._replica_read_only = self._options.get("REPLICA_READ_ONLY", True)

        self._connection_factory = getattr(
            settings,
            "DJANGO_VALKEY_CONNECTION_FACTORY",
            "django_valkey.async_cache.pool.AsyncConnectionFactory",
        )
        self.connection_factory: (
            AsyncConnectionFactory | AsyncSentinelConnectionFactory | Any
        ) = pool.get_connection_factory(
            options=self._options, path=self._connection_factory
        )

        serializer_path = self._options.get(
            "SERIALIZER", "django_valkey.serializers.pickle.PickleSerializer"
        )
        serializer_cls: type = import_string(serializer_path)

        compressor_path = self._options.get(
            "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
        )
        compressor_cls: type = import_string(compressor_path)

        self._serializer = serializer_cls(options=self._options)
        self._compressor = compressor_cls(options=self._options)

    def __contains__(self, item) -> bool:
        c = yield from self.__contains(item)
        return c

    async def __contains(self, key) -> bool:
        yield await self.has_key(key)

    async def _has_compression_enabled(self) -> bool:
        return (
            self._options.get(
                "COMPRESSOR", "django_valkey.compressors.IdentityCompressor"
            )
            != "django_valkey.compressors.IdentityCompressor"
        )

    async def _decode_iterable_result(
        self, result: Any, convert_to_set: bool = True
    ) -> list[Any] | Set[Any] | Any | None:
        if result is None:
            return None
        if isinstance(result, list):
            if convert_to_set:
                return {await self.decode(value) for value in result}
            return [await self.decode(value) for value in result]
        return await self.decode(result)

    async def get_next_client_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> int:
        if write or len(self._server) == 1:
            return 0

        if tried is None:
            tried = []

        if tried and len(tried) < len(self._server):
            not_tried = [i for i in range(0, len(self._server)) if i not in tried]
            return random.choice(not_tried)

        return random.randint(1, len(self._server) - 1)

    async def get_client(
        self,
        write: bool = True,
        tried: list[int] | None = None,
        client: AValkey | Any | None = None,
    ) -> AValkey | Any:
        if client:
            return client

        index = await self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index]

    async def get_client_with_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> tuple[AValkey, int]:
        index = await self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index], index

    async def connect(self, index: int = 0) -> AValkey | Any:
        return await self.connection_factory.connect(self._server[index])

    aconnect = connect

    async def disconnect(self, index: int = 0, client=None):
        if client is None:
            client = self._clients[index]

        if client is not None:
            await self.connection_factory.disconnect(client)

    adisconnect = disconnect

    async def set(
        self,
        key,
        value,
        timeout=DEFAULT_TIMEOUT,
        version=None,
        client: AValkey | Any | None = None,
        nx=False,
        xx=False,
    ) -> bool:
        nkey = await self.make_key(key, version=version)
        nvalue = await self.encode(value)

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
                return bool(await client.set(nkey, nvalue, nx=nx, px=timeout, xx=xx))

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

    aset = set

    async def incr_version(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: AValkey | None | Any = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)

        new_key, old_key, value, ttl, version = await self._incr_version(
            key, delta, version, client
        )

        await self.set(new_key, value, timeout=ttl, client=client)
        await self.delete(old_key, client=client)
        return version + delta

    async def _incr_version(self, key, delta, version, client) -> tuple:
        if version is None:
            version = self._backend.version

        old_key = await self.make_key(key, version)
        value = await self.get(old_key, version=version, client=client)

        try:
            ttl = await self.ttl(old_key, version=version, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            error_message = f"Key '{old_key!r}' does not exist"
            raise ValueError(error_message)

        if isinstance(key, CacheKey):
            new_key = await self.make_key(
                await key.original_key(), version=version + delta
            )
        else:
            new_key = await self.make_key(key, version=version + delta)

        return new_key, old_key, value, ttl, version

    aincr_version = incr_version

    async def add(
        self,
        key,
        value,
        timeout: float | int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        return await self.set(
            key, value, timeout, version=version, client=client, nx=True
        )

    aadd = add

    async def get(
        self,
        key,
        default: Any | None = None,
        version: int | None = None,
        client: AValkey | None | Any = None,
    ) -> Any:
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)

        try:
            value = await client.get(key)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            return default

        return await self.decode(value)

    aget = get

    async def persist(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self.get_client(write=True, client=client)
        key = await self.make_key(key, version=version)

        return await client.persist(key)

    apersist = persist

    async def expire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.expire(key, timeout)

    aexpire = expire

    async def expire_at(
        self, key, when, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.expireat(key, when)

    aexpire_at = expire_at

    async def pexpire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.pexpire(key, timeout)

    apexpire = pexpire

    async def pexpire_at(
        self, key, when, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.pexpireat(key, when)

    apexpire_at = pexpire_at

    async def get_lock(
        self,
        key,
        version: int | None = None,
        timeout: float | int | None = None,
        sleep: float = 0.1,
        blocking_timeout: float | None = None,
        client: AValkey | Any | None = None,
        thread_local: bool = True,
    ):
        """Returns a Lock object, the object then should be used in an async context manager"""

        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    # TODO: delete this in future releases
    lock = alock = aget_lock = get_lock

    async def delete(
        self,
        key,
        version: int | None = None,
        prefix: str | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)

        try:
            return await client.delete(
                await self.make_key(key, version=version, prefix=prefix)
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    adelete = delete

    async def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: AValkey | Any | None = None,
        itersize: int | None = None,
    ) -> int:
        """
        Remove all keys matching a pattern.
        """
        client = await self.get_client(write=True, client=client)

        pattern = await self.make_pattern(pattern, version=version, prefix=prefix)

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

    adelete_pattern = delete_pattern

    async def delete_many(
        self, keys, version: int | None = None, client: AValkey | None = None
    ) -> int:
        """Remove multiple keys at once."""
        keys = [await self.make_key(k, version=version) for k in keys]

        if not keys:
            return 0

        client = await self.get_client(write=True, client=client)

        try:
            return await client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    adelete_many = delete_many

    async def clear(self, client: AValkey | Any | None = None) -> None:
        """
        Flush all cache keys.
        """

        client = await self.get_client(write=True, client=client)

        try:
            await client.flushdb()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    aclear = clear

    async def decode(self, value) -> Any:
        """
        Decode the given value.
        """
        try:
            value = int(value)
        except (ValueError, TypeError):
            # Handle values that weren't compressed (small stuff)
            with suppress(CompressorError):
                value = self._compressor.decompress(value)

            value = self._serializer.loads(value)
        return value

    adecode = decode

    async def encode(self, value) -> bytes | int:
        """
        Encode the given value.
        """
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializer.dumps(value)
            return self._compressor.compress(value)

        return value

    aencode = encode

    async def get_many(
        self, keys, version: int | None = None, client: AValkey | Any | None = None
    ) -> dict:
        if not keys:
            return {}

        client = await self.get_client(write=False, client=client)

        recovered_data = {}

        map_keys = {await self.make_key(k, version=version): k for k in keys}

        try:
            results = await client.mget(*map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = await self.decode(value)

        return recovered_data

    aget_many = get_many

    async def set_many(
        self,
        data: dict,
        timeout: float | int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> None:
        client = await self.get_client(write=True, client=client)

        try:
            pipeline = await client.pipeline()
            for key, value in data.items():
                await self.set(key, value, timeout, version=version, client=pipeline)
            await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    aset_many = set_many

    async def _incr(
        self,
        key,
        delta: 1,
        version: int | None = None,
        client: AValkey | Any | None = None,
        ignoree_key_check: bool = False,
    ) -> int:
        client = await self.get_client(write=True, client=client)
        key = await self.make_key(key, version=version)

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
        client: AValkey | Any | None = None,
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

    aincr = incr

    async def decr(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        """
        Decreace delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return await self._incr(key, delta=-delta, version=version, client=client)

    adecr = decr

    async def ttl(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self.get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        if not await client.exists(key):
            return 0

        t = await client.ttl(key)
        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    attl = ttl

    async def pttl(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        if not await client.exists(key):
            return 0

        t = await client.pttl(key)

        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    apttl = pttl

    async def has_key(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        """
        Test if key exists.
        """
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        try:
            return await client.exists(key) == 1
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    ahas_key = has_key

    async def iter_keys(
        self,
        search: str,
        itersize: int | None = None,
        client: AValkey | Any | None = None,
        version: int | None = None,
    ) -> AsyncGenerator:
        """
        Same as keys, but uses cursors
        for make memory efficient keys iteration.
        """
        client = await self.get_client(write=False, client=client)
        pattern = await self.make_pattern(search, version=version)
        async with contextlib.aclosing(
            client.scan_iter(match=pattern, count=itersize)
        ) as values:
            async for item in values:
                yield self.reverse_key(item.decode())

    aiter_keys = iter_keys

    async def keys(
        self,
        search: str,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list[Any]:
        client = await self.get_client(write=False, client=client)

        pattern = await self.make_pattern(search, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in await client.keys(pattern)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    akeys = keys

    async def make_key(
        self, key, version: int | None = None, prefix: str | None = None
    ):
        if isinstance(key, CacheKey):
            return key

        if prefix is None:
            prefix = self._backend.key_prefix

        if version is None:
            version = self._backend.version

        return CacheKey(self._backend.key_func(key, prefix, version))

    amake_key = make_key

    async def make_pattern(
        self, pattern: str, version: int | None = None, prefix: str | None = None
    ):
        if isinstance(pattern, CacheKey):
            return pattern

        if prefix is None:
            prefix = self._backend.key_prefix
        prefix = glob_escape(prefix)

        if version is None:
            version = self._backend.version
        version_str = glob_escape(str(version))
        return CacheKey(self._backend.key_func(pattern, prefix, version_str))

    amake_pattern = make_pattern

    async def sadd(
        self,
        key,
        *values: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)
        key = await self.make_key(key, version=version)
        encoded_values = [await self.encode(value) for value in values]

        return await client.sadd(key, *encoded_values)

    asadd = sadd

    async def scard(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int:
        client = await self.get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        return await client.scard(key)

    ascard = scard

    async def sdiff(
        self, *keys, version: int | None = None, client: AValkey | Any | None = None
    ) -> Set[Any]:
        client = await self.get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        return {await self.decode(value) for value in await client.sdiff(*nkeys)}

    asdiff = sdiff

    async def sdiffstore(
        self,
        dest,
        *keys,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)
        dest = await self.make_key(dest, version=version_dest)
        nkeys = [await self.make_key(key, version=version_keys) for key in keys]
        return await client.sdiffstore(dest, *nkeys)

    asdiffstore = sdiffstore

    async def sinter(
        self, *keys, version: int | None = None, client: AValkey | Any | None = None
    ) -> Set[Any]:
        client = await self.get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        return {await self.decode(value) for value in await client.sinter(*nkeys)}

    asinter = sinter

    async def sinterstore(
        self,
        dest,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)
        dest = await self.make_key(dest, version=version)
        nkeys = [await self.make_key(key, version=version) for key in keys]

        return await client.sinterstore(dest, *nkeys)

    asinterstore = sinterstore

    async def smismember(
        self,
        key,
        *members: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list[bool]:
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        encoded_members = [await self.encode(member) for member in members]

        return [bool(value) for value in await client.smismember(key, *encoded_members)]

    asmismember = smismember

    async def sismember(
        self,
        key,
        member: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        member = await self.encode(member)
        return bool(await client.sismember(key, member))

    asismember = sismember

    async def smembers(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> Set[Any]:
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        return {await self.decode(value) for value in await client.smembers(key)}

    asmembers = smembers

    async def smove(
        self,
        source,
        destination,
        member: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        client = await self.get_client(write=False, client=client)
        source = await self.make_key(source, version=version)
        destination = await self.make_key(destination, version=version)
        member = await self.encode(member)
        return await client.smove(source, destination, member)

    asmove = smove

    async def spop(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> Set | Any:
        client = await self.get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        result = await client.spop(nkey, count)
        return await self._decode_iterable_result(result)

    aspop = spop

    async def srandmember(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list | Any:
        client = await self.get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        result = await client.srandmember(key, count)
        return await self._decode_iterable_result(result, convert_to_set=False)

    asrandmember = srandmember

    async def srem(
        self,
        key,
        *members,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        nmembers = [await self.encode(member) for member in members]
        return await client.srem(key, *nmembers)

    asrem = srem

    async def sscan(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> Set[Any]:
        # TODO check this is correct
        if await self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        cursor, result = await client.sscan(
            key,
            match=cast(PatternT, await self.encode(match)) if match else None,
            count=count,
        )
        return {await self.decode(value) for value in result}

    asscan = sscan

    async def sscan_iter(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ):
        if await self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self.get_client(write=False, client=client)

        key = await self.make_key(key, version=version)

        async with contextlib.aclosing(
            client.sscan_iter(
                key,
                match=cast(PatternT, await self.encode(match)) if match else None,
                count=count,
            )
        ) as values:
            async for value in values:
                yield await self.decode(value)

    asscan_iter = sscan_iter

    async def sunion(
        self,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> Set[Any]:
        client = await self.get_client(write=False, client=client)

        nkeys = [await self.make_key(key, version=version) for key in keys]
        return {await self.decode(value) for value in await client.sunion(*nkeys)}

    asunion = sunion

    async def sunionstore(
        self,
        destination: Any,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self.get_client(write=True, client=client)
        destination = await self.make_key(destination, version=version)
        encoded_keys = [await self.make_key(key, version=version) for key in keys]
        return await client.sunionstore(destination, *encoded_keys)

    asunionstore = sunionstore

    async def aclose(self) -> None:
        close_flag = self._options.get(
            "CLOSE_CONNECTION",
            getattr(settings, "DJANGO_VALKEY_CLOSE_CONNECTION", False),
        )
        if close_flag:
            await self._aclose()

    close = aclose

    async def _aclose(self) -> None:
        """
        default implementation: Override in custom client
        """
        num_clients = len(self._clients)
        for index in range(num_clients):
            # TODO: check disconnect and close
            await self.disconnect(index=index)
        self._clients = [None] * num_clients

    async def touch(
        self,
        key,
        timeout: float | int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        """
        Sets a new expiration for a key.
        """
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self.get_client(write=True, client=client)

        key = await self.make_key(key, version=version)
        if timeout is None:
            return bool(await client.persist(key))

        # convert timeout to milliseconds
        timeout = int(timeout * 1000)
        return bool(await client.pexpire(key, timeout))

    atouch = touch

    async def hset(
        self,
        name: str,
        key,
        value,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        """
        Sets the value of hash name at key to value.
        Returns the number of fields added to the hash.
        """
        client = await self.get_client(write=True, client=client)

        nkey = await self.make_key(key, version)
        nvalue = await self.encode(value)

        return await client.hset(name, nkey, nvalue)

    ahset = hset

    async def hdel(
        self,
        name: str,
        key,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        """
        Remove keys from hash name.
        Returns the number of fields deleted from the hash.
        """
        client = await self.get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        return await client.hdel(name, nkey)

    ahdel = hdel

    async def hlen(self, name: str, client: AValkey | Any | None = None) -> int:
        """
        Return the number of items in hash name.
        """
        client = await self.get_client(write=False, client=client)
        return await client.hlen(name)

    ahlen = hlen

    async def hkeys(self, name: str, client: AValkey | Any | None = None) -> list[Any]:
        client = await self.get_client(write=False, client=client)

        try:
            return [self.reverse_key(k.decode()) for k in await client.hkeys(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    ahkeys = hkeys

    async def hexists(
        self,
        name: str,
        key,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = await self.get_client(write=False, client=client)
        nkey = await self.make_key(key, version=version)
        return await client.hexists(name, nkey)

    ahexists = hexists
