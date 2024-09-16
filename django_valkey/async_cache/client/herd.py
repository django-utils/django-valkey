import socket
import time
from typing import Tuple, Any

from django.conf import settings
from valkey import Valkey
from valkey.exceptions import ConnectionError, ResponseError, TimeoutError
from valkey.typing import KeyT, EncodableT

from django_valkey.async_cache.client import AsyncDefaultClient
from django_valkey.client.default import DEFAULT_TIMEOUT
from django_valkey.client.herd import Marker, _is_expired
from django_valkey.exceptions import ConnectionInterrupted

_main_exceptions = (ConnectionError, ResponseError, TimeoutError, socket.timeout)


class AsyncHerdClient(AsyncDefaultClient):
    def __init__(self, *args, **kwargs):
        self._marker = Marker()
        self._herd_timeout: int = getattr(settings, "CACHE_HERD_TIMEOUT", 60)
        super().__init__(*args, **kwargs)

    async def _pack(self, value: Any, timeout) -> Tuple[Marker, Any, int]:
        herd_timeout = (timeout or self._backend.default_timeout) + int(time.time())
        return self._marker, value, herd_timeout

    async def _unpack(self, value: Tuple[Marker, Any, int]) -> Tuple[Any, bool]:
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
            return await super().aset(
                key,
                value,
                timeout=timeout,
                version=version,
                client=client,
                nx=nx,
                xx=xx,
            )

        packed = await self._pack(value, timeout)
        real_timeout = timeout + self._herd_timeout

        return await super().aset(
            key,
            packed,
            timeout=real_timeout,
            version=version,
            client=client,
            nx=nx,
            xx=xx,
        )

    aset = set

    async def get(self, key, default=None, version=None, client=None):
        packed = await super().aget(
            key, default=default, version=version, client=client
        )
        val, refresh = await self._unpack(packed)

        if refresh:
            return default

        return val

    aget = get

    async def get_many(self, keys, version=None, client=None):
        client = await self.get_client(write=False, client=client)

        if not keys:
            return {}

        recovered_data = {}

        new_keys = [await self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        try:
            results = await client.mget(*new_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        for key, value in zip(new_keys, results):
            if value is None:
                continue

            val, refresh = await self._unpack(await self.decode(value))
            recovered_data[map_keys[key]] = None if refresh else val

        return recovered_data

    aget_many = get_many

    async def set_many(
        self, data, timeout=DEFAULT_TIMEOUT, version=None, client=None, herd=True
    ):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        client = await self.get_client(write=True, client=client)

        set_function = self.aset if herd else super().aset

        try:
            pipeline = await client.pipeline()
            for key, value in data.items():
                await set_function(
                    key, value, timeout, version=version, client=pipeline
                )
            await pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    ast_many = set_many

    def incr(self, *args, **kwargs):
        raise NotImplementedError

    aincr = incr

    def decr(self, *args, **kwargs):
        raise NotImplementedError

    adecr = decr

    async def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        client = await self.get_client(write=True, client=client)

        value = await self.aget(key, version=version, client=client)
        if value is None:
            return False

        await self.aset(key, value, timeout=timeout, version=version, client=client)
        return True

    atouch = touch
