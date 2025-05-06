from valkey import Valkey
from valkey.typing import KeyT, EncodableT

from django_valkey.async_cache.client import AsyncDefaultClient
from django_valkey.base_client import (
    DEFAULT_TIMEOUT,
    HerdCommonMethods,
    _main_exceptions,
)
from django_valkey.exceptions import ConnectionInterrupted


class AsyncHerdClient(HerdCommonMethods, AsyncDefaultClient):
    async def set(
        self,
        key: KeyT,
        value: EncodableT,
        timeout: int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Valkey | None = None,
        nx: bool = False,
        xx: bool = False,
    ):
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        if timeout is None or timeout <= 0:
            return await super().set(
                key,
                value,
                timeout=timeout,
                version=version,
                client=client,
                nx=nx,
                xx=xx,
            )

        packed = self._pack(value, timeout)
        real_timeout = timeout + self._herd_timeout

        return await super().set(
            key,
            packed,
            timeout=real_timeout,
            version=version,
            client=client,
            nx=nx,
            xx=xx,
        )

    async def get(self, key, default=None, version=None, client=None):
        packed = await super().get(key, default=default, version=version, client=client)
        val, refresh = self._unpack(packed)

        if refresh:
            return default

        return val

    async def get_many(self, keys, version=None, client=None):
        client = await self._get_client(write=False, client=client)

        if not keys:
            return {}

        recovered_data = {}

        new_keys = [self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        try:
            pipeline = await client.pipeline()
            for key in new_keys:
                await pipeline.get(key)
            results = await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(new_keys, results):
            if value is None:
                continue

            val, refresh = self._unpack(self.decode(value))
            recovered_data[map_keys[key]] = None if refresh else val

        return recovered_data

    async def mget(self, keys, version=None, client=None):
        if not keys:
            return {}

        client = await self._get_client(write=False, client=client)

        recovered_data = {}

        new_keys = [self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        try:
            results = await client.mget(new_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(new_keys, results):
            if value is None:
                continue

            val, refresh = self._unpack(self.decode(value))
            recovered_data[map_keys[key]] = None if refresh else val

        return recovered_data

    async def set_many(
        self, data, timeout=DEFAULT_TIMEOUT, version=None, client=None, herd=True
    ):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        client = await self._get_client(write=True, client=client)

        set_function = self.set if herd else super().set

        try:
            pipeline = await client.pipeline()
            for key, value in data.items():
                await set_function(
                    key, value, timeout, version=version, client=pipeline
                )
            await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def incr(self, *args, **kwargs):
        raise NotImplementedError

    def decr(self, *args, **kwargs):
        raise NotImplementedError

    async def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        client = await self._get_client(write=True, client=client)

        value = await self.get(key, version=version, client=client)
        if value is None:
            return False

        await self.set(key, value, timeout=timeout, version=version, client=client)
        return True
