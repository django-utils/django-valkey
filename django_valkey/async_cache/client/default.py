import builtins
from collections.abc import Iterable, AsyncIterable
import contextlib
from contextlib import suppress
from typing import Any, cast, TYPE_CHECKING

from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from valkey.asyncio import Valkey as AValkey
from valkey.exceptions import ResponseError
from valkey.typing import PatternT

from django_valkey.base_client import (
    BaseClient,
    _main_exceptions,
    glob_escape,
)
from django_valkey.exceptions import CompressorError, ConnectionInterrupted
from django_valkey.util import CacheKey
from django_valkey.typing import KeyT

if TYPE_CHECKING:
    from valkey.asyncio.lock import Lock


class AsyncDefaultClient(BaseClient[AValkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.async_cache.pool.AsyncConnectionFactory"

    def __contains__(self, item):
        raise NotImplementedError("async client doesn't support __contain__")

    async def _decode_iterable_result(
        self, result: Any, convert_to_set: bool = True
    ) -> list[Any] | builtins.set[Any] | Any | None:
        if result is None:
            return None
        if isinstance(result, list):
            if convert_to_set:
                return {await self.decode(value) for value in result}
            return [await self.decode(value) for value in result]
        return await self.decode(result)

    async def _get_client(self, write=True, tried=None, client=None):
        if client:
            return client
        return await self.get_client(write, tried)

    async def get_client(
        self,
        write: bool = True,
        tried: list[int] | None = None,
    ) -> AValkey | Any:
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index]

    async def get_client_with_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> tuple[AValkey, int]:
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = await self.connect(index)

        return self._clients[index], index

    async def aconnect(self, index: int = 0) -> AValkey | Any:
        return await self.connection_factory.connect(self._server[index])

    connect = aconnect

    async def adisconnect(self, index: int = 0, client=None):
        if client is None:
            client = self._clients[index]

        if client is not None:
            await self.connection_factory.disconnect(client)

    disconnect = adisconnect

    async def aset(
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

    set = aset

    async def aincr_version(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: AValkey | None | Any = None,
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

    incr_version = aincr_version

    async def aadd(
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

    add = aadd

    async def aget(
        self,
        key,
        default: Any | None = None,
        version: int | None = None,
        client: AValkey | None | Any = None,
    ) -> Any:
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)

        try:
            value = await client.get(key)

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            return default

        return await self.decode(value)

    get = aget

    async def apersist(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)
        key = await self.make_key(key, version=version)

        return await client.persist(key)

    persist = apersist

    async def aexpire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.expire(key, timeout)

    expire = aexpire

    async def aexpire_at(
        self, key, when, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.expireat(key, when)

    expire_at = aexpire_at

    async def apexpire(
        self,
        key,
        timeout,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.pexpire(key, timeout)

    pexpire = apexpire

    async def apexpire_at(
        self, key, when, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return await client.pexpireat(key, when)

    pexpire_at = apexpire_at

    async def aget_lock(
        self,
        key,
        version: int | None = None,
        timeout: float | int | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        client: AValkey | Any | None = None,
        lock_class=None,
        thread_local: bool = True,
    ) -> "Lock":
        """Returns a Lock object, the object then should be used in an async context manager"""

        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)

        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            lock_class=lock_class,
            thread_local=thread_local,
        )

    # TODO: delete this in future releases
    lock = alock = get_lock = aget_lock

    async def adelete(
        self,
        key,
        version: int | None = None,
        prefix: str | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)

        try:
            return await client.delete(
                await self.make_key(key, version=version, prefix=prefix)
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    delete = adelete

    async def adelete_pattern(
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
        client = await self._get_client(write=True, client=client)

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

    delete_pattern = adelete_pattern

    async def adelete_many(
        self, keys, version: int | None = None, client: AValkey | None = None
    ) -> int:
        """Remove multiple keys at once."""
        keys = [await self.make_key(k, version=version) for k in keys]

        if not keys:
            return 0

        client = await self._get_client(write=True, client=client)

        try:
            return await client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    delete_many = adelete_many

    async def aclear(self, client: AValkey | Any | None = None) -> bool:
        """
        Flush all cache keys.
        """

        client = await self._get_client(write=True, client=client)

        try:
            return await client.flushdb()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    clear = aclear

    async def adecode(self, value: bytes) -> Any:
        """
        Decode the given value.
        """
        try:
            if value.isdigit():
                value = int(value)
            else:
                value = float(value)
        except (ValueError, TypeError):
            # Handle values that weren't compressed (small stuff)
            with suppress(CompressorError):
                value = self._compressor.decompress(value)

            value = self._serializer.loads(value)
        return value

    decode = adecode

    async def aencode(self, value) -> bytes | int | float:
        """
        Encode the given value.
        """
        if type(value) is not int and type(value) is not float:
            value = self._serializer.dumps(value)
            return self._compressor.compress(value)

        return value

    encode = aencode

    async def amget(
        self, keys, version: int | None = None, client: AValkey | Any | None = None
    ) -> dict:
        if not keys:
            return {}

        client = await self._get_client(write=False, client=client)

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

    mget = amget

    async def aget_many(
        self,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: AValkey | None = None,
    ):
        """
        non-atomic bulk method.
        get values of the provided keys.
        """
        client = await self._get_client(write=False, client=client)

        try:
            pipeline = await client.pipeline()
            for key in keys:
                key = await self.make_key(key, version=version)
                await pipeline.get(key)
            values = await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        recovered_data = {}
        for key, value in zip(keys, values):
            if not value:
                continue
            recovered_data[key] = await self.decode(value)
        return recovered_data

    get_many = aget_many

    async def aset_many(
        self,
        data: dict,
        timeout: float | int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> None:
        client = await self._get_client(write=True, client=client)

        try:
            pipeline = await client.pipeline()
            for key, value in data.items():
                await self.set(key, value, timeout, version=version, client=pipeline)
            await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    set_many = aset_many

    async def amset(
        self,
        data: dict[KeyT, Any],
        timeout: float | None = None,
        version: int | None = None,
        client: AValkey | None = None,
    ) -> None:
        client = await self._get_client(write=True, client=client)

        data = {
            await self.make_key(k, version=version): await self.encode(v)
            for k, v in data.items()
        }

        try:
            await client.mset(data)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    mset = amset

    async def _incr(
        self,
        key,
        amount: int = 1,
        version: int | None = None,
        ignore_key_check: bool = False,
        client: AValkey | None = None,
        _operation="incr",
    ):
        client = await self._get_client(write=True, client=client)
        op = client.incrby if _operation == "incr" else client.decrby
        key = await self.make_key(key, version=version)
        try:
            if ignore_key_check:
                value = await op(key, amount)
            else:
                if await client.exists(key):
                    value = await op(key, amount)
                else:
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
            if _operation == "incr":
                value = await self.get(key, version=version, client=client) + amount
            else:
                value = await self.get(key, version=version, client=client) - amount
            await self.set(key, value, version=version, timeout=timeout, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return value

    async def aincr(
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
            ignore_key_check=ignore_key_check,
            _operation="incr",
        )

    incr = aincr

    async def adecr(
        self,
        key,
        delta: int = 1,
        version: int | None = None,
        client: AValkey | Any | None = None,
        ignore_key_check: bool = False,
    ) -> int:
        """
        Decrease delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return await self._incr(
            key,
            amount=delta,
            version=version,
            client=client,
            ignore_key_check=ignore_key_check,
            _operation="decr",
        )

    decr = adecr

    async def attl(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self._get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        if not await client.exists(key):
            return 0

        t = await client.ttl(key)
        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    ttl = attl

    async def apttl(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        if not await client.exists(key):
            return 0

        t = await client.pttl(key)

        if t >= 0:
            return t
        if t == -2:
            return 0

        return None

    pttl = apttl

    async def ahas_key(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> bool:
        """
        Test if key exists.
        """
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        try:
            return await client.exists(key) == 1
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    has_key = ahas_key

    async def aiter_keys(
        self,
        search: str,
        itersize: int | None = None,
        client: AValkey | Any | None = None,
        version: int | None = None,
    ) -> AsyncIterable[KeyT]:
        """
        Same as keys, but uses cursors
        for make memory efficient keys iteration.
        """
        client = await self._get_client(write=False, client=client)
        pattern = await self.make_pattern(search, version=version)
        async with contextlib.aclosing(
            client.scan_iter(match=pattern, count=itersize)
        ) as values:
            async for item in values:
                yield self.reverse_key(item.decode())

    iter_keys = aiter_keys

    async def akeys(
        self,
        search: str,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list[Any]:
        client = await self._get_client(write=False, client=client)

        pattern = await self.make_pattern(search, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in await client.keys(pattern)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    keys = akeys

    async def ascan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        version: int | None = None,
        client: AValkey | None = None,
    ) -> tuple[int, list[str]]:
        if self._has_compression_enabled() and match:
            raise ValueError("Using match with compression enables is not supported")
        client = await self._get_client(write=False, client=client)

        try:
            cursor, result = await client.scan(
                cursor=cursor,
                match=(
                    await self.make_pattern(match, version=version) if match else None
                ),
                count=count,
                _type=_type,
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return cursor, [self.reverse_key(val.decode()) for val in result]

    scan = ascan

    async def amake_key(
        self, key, version: int | None = None, prefix: str | None = None
    ):
        if isinstance(key, CacheKey):
            return key

        if prefix is None:
            prefix = self._backend.key_prefix

        if version is None:
            version = self._backend.version

        return CacheKey(self._backend.key_func(key, prefix, version))

    make_key = amake_key

    async def amake_pattern(
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

    make_pattern = amake_pattern

    async def asadd(
        self,
        key,
        *values: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        key = await self.make_key(key, version=version)
        encoded_values = [await self.encode(value) for value in values]

        return await client.sadd(key, *encoded_values)

    sadd = asadd

    async def ascard(
        self, key, version: int | None = None, client: AValkey | Any | None = None
    ) -> int:
        client = await self._get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        return await client.scard(key)

    scard = ascard

    async def asdiff(
        self,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        return await self._decode_iterable_result(
            await client.sdiff(*nkeys), convert_to_set=return_set
        )

    sdiff = asdiff

    async def asdiffstore(
        self,
        dest,
        *keys,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        dest = await self.make_key(dest, version=version_dest)
        nkeys = [await self.make_key(key, version=version_keys) for key in keys]
        return await client.sdiffstore(dest, *nkeys)

    sdiffstore = asdiffstore

    async def asinter(
        self,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any]:
        client = await self._get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        return await self._decode_iterable_result(
            await client.sinter(*nkeys), convert_to_set=return_set
        )

    sinter = asinter

    async def asintercard(
        self,
        numkeys: int,
        keys: Iterable[str],
        limit: int = 0,
        version: int | None = None,
        client: AValkey | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        return await client.sintercard(numkeys, keys=nkeys, limit=limit)

    sintercard = asintercard

    async def asinterstore(
        self,
        dest,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        dest = await self.make_key(dest, version=version)
        nkeys = [await self.make_key(key, version=version) for key in keys]

        return await client.sinterstore(dest, *nkeys)

    sinterstore = asinterstore

    async def asmismember(
        self,
        key,
        *members: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list[bool]:
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        encoded_members = [await self.encode(member) for member in members]

        return [bool(value) for value in await client.smismember(key, *encoded_members)]

    smismember = asmismember

    async def asismember(
        self,
        key,
        member: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        member = await self.encode(member)
        return bool(await client.sismember(key, member))

    sismember = asismember

    async def asmembers(
        self,
        key,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        return await self._decode_iterable_result(
            await client.smembers(key), convert_to_set=return_set
        )

    smembers = asmembers

    async def asmove(
        self,
        source,
        destination,
        member: Any,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        client = await self._get_client(write=False, client=client)
        source = await self.make_key(source, version=version)
        destination = await self.make_key(destination, version=version)
        member = await self.encode(member)
        return await client.smove(source, destination, member)

    smove = asmove

    async def aspop(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set | list | Any:
        client = await self._get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        result = await client.spop(nkey, count)
        return await self._decode_iterable_result(result, convert_to_set=return_set)

    spop = aspop

    async def asrandmember(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> list | Any:
        client = await self._get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        result = await client.srandmember(key, count)
        return await self._decode_iterable_result(result, convert_to_set=return_set)

    srandmember = asrandmember

    async def asrem(
        self,
        key,
        *members,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        nmembers = [await self.encode(member) for member in members]
        return await client.srem(key, *nmembers)

    srem = asrem

    async def asscan(
        self,
        key,
        cursor: int = 0,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: AValkey | Any | None = None,
        return_set: bool = True,
    ) -> tuple[int, builtins.set[Any]] | tuple[int, list[Any]]:
        # TODO check this is correct
        if self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self._get_client(write=False, client=client)

        key = await self.make_key(key, version=version)
        cursor, result = await client.sscan(
            name=key,
            cursor=cursor,
            match=cast(PatternT, await self.encode(match)) if match else None,
            count=count,
        )
        return cursor, await self._decode_iterable_result(
            result, convert_to_set=return_set
        )

    sscan = asscan

    async def asscan_iter(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> AsyncIterable[Any]:
        if self._has_compression_enabled() and match:
            error_message = "Using match with compression is not supported."
            raise ValueError(error_message)

        client = await self._get_client(write=False, client=client)

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

    sscan_iter = asscan_iter

    async def asunion(
        self,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
        retrun_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = await self._get_client(write=False, client=client)

        nkeys = [await self.make_key(key, version=version) for key in keys]
        return await self._decode_iterable_result(
            await client.sunion(*nkeys), convert_to_set=retrun_set
        )

    sunion = asunion

    async def asunionstore(
        self,
        destination: Any,
        *keys,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        destination = await self.make_key(destination, version=version)
        encoded_keys = [await self.make_key(key, version=version) for key in keys]
        return await client.sunionstore(destination, *encoded_keys)

    sunionstore = asunionstore

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

    _close = _aclose

    async def atouch(
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

        client = await self._get_client(write=True, client=client)

        key = await self.make_key(key, version=version)
        if timeout is None:
            return bool(await client.persist(key))

        # convert timeout to milliseconds
        timeout = int(timeout * 1000)
        return bool(await client.pexpire(key, timeout))

    touch = atouch

    async def ahset(
        self,
        name: str,
        key=None,
        value=None,
        mapping: dict | None = None,
        items: list | None = None,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        """
        Sets the value of hash name at key to value.
        Returns the number of fields added to the hash.
        """
        client = await self._get_client(write=True, client=client)
        if key and value:
            key = await self.make_key(key, version=version)
            value = await self.encode(value)
        if mapping:
            mapping = {
                await self.make_key(key): await self.encode(value)
                for key, value in mapping.items()
            }
        if items:
            items = [
                await (self.encode if index & 1 else self.make_key)(item)
                for index, item in enumerate(items)
            ]

        return await client.hset(name, key, value, mapping=mapping, items=items)

    hset = ahset

    async def ahsetnx(
        self,
        name: str,
        key,
        value,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        nvalue = await self.encode(value)
        return await client.hsetnx(name, nkey, nvalue)

    hsetnx = ahsetnx

    async def ahdel(
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
        client = await self._get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        return await client.hdel(name, nkey)

    hdel = ahdel

    async def ahdel_many(
        self,
        name: str,
        keys: list,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        nkeys = [await self.make_key(key) for key in keys]
        return await client.hdel(name, *nkeys)

    hdel_many = ahdel_many

    async def ahget(
        self,
        name: str,
        key: str,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> str | None:
        client = await self._get_client(write=False, client=client)
        key = await self.make_key(key, version=version)
        try:
            value = await client.hget(name, key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        if value is None:
            return None
        return await self.decode(value)

    hget = ahget

    async def ahgetall(
        self, name: str, client: AValkey | Any | None = None
    ) -> dict[str, str] | dict:
        client = await self._get_client(write=False, client=client)
        try:
            _values = await client.hgetall(name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        values = {}
        for key, value in _values.items():
            values[key.decode()] = await self.decode(value)
        return values

    hgetall = ahgetall

    async def ahmget(
        self,
        name: str,
        keys: list,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> list:
        client = await self._get_client(write=False, client=client)
        nkeys = [await self.make_key(key, version=version) for key in keys]
        try:
            values = await client.hmget(name, nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        values = [await self.decode(val) for val in values]
        return values

    hmget = ahmget

    async def ahincrby(
        self,
        name: str,
        key: str,
        amount: int = 1,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        try:
            value = await client.hincrby(name, nkey, amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return value

    hincrby = ahincrby

    async def ahincrbyfloat(
        self,
        name: str,
        key: str,
        amount: float = 1.0,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> float:
        client = await self._get_client(write=True, client=client)
        nkey = await self.make_key(key, version=version)
        try:
            value = await client.hincrbyfloat(name, nkey, amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return value

    hincrbyfloat = ahincrbyfloat

    async def ahlen(self, name: str, client: AValkey | Any | None = None) -> int:
        """
        Return the number of items in hash name.
        """
        client = await self._get_client(write=False, client=client)
        return await client.hlen(name)

    hlen = ahlen

    async def ahkeys(self, name: str, client: AValkey | Any | None = None) -> list[Any]:
        client = await self._get_client(write=False, client=client)

        try:
            return [self.reverse_key(k.decode()) for k in await client.hkeys(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    aahkeys = ahkeys

    async def ahexists(
        self,
        name: str,
        key,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = await self._get_client(write=False, client=client)
        nkey = await self.make_key(key, version=version)
        return await client.hexists(name, nkey)

    hexists = ahexists

    async def ahvals(self, name: str, client: AValkey | Any | None = None) -> list:
        client = await self._get_client(write=False, client=client)
        try:
            return [await self.decode(val) for val in await client.hvals(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    hvals = ahvals

    async def ahstrlen(
        self,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: AValkey | Any | None = None,
    ) -> int:
        client = await self._get_client(write=False, client=client)
        nkey = await self.make_key(key, version=version)
        try:
            return await client.hstrlen(name, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    hstrlen = ahstrlen

    async def ahrandfield(
        self,
        name: str,
        count: int | None = None,
        withvalues: bool = False,
        client: AValkey | None = None,
    ) -> str | list | None:
        client = await self._get_client(write=False, client=client)
        try:
            result = await client.hrandfield(
                key=name, count=count, withvalues=withvalues
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if not result:
            return None
        elif count and withvalues:
            return [
                (
                    await self.decode(val)
                    if index & 1
                    else self.reverse_key(val.decode())
                )
                for index, val in enumerate(result)
            ]
        elif count:
            return [self.reverse_key(val.decode()) for val in result]
        return self.reverse_key(result.decode())

    hrandfield = ahrandfield
