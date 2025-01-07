import builtins
from collections.abc import Iterator
import re
from datetime import datetime, timedelta
from typing import Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT

from valkey import Valkey
from valkey.exceptions import ConnectionError
from valkey.typing import EncodableT

from django_valkey.client.default import DefaultClient
from django_valkey.exceptions import ConnectionInterrupted
from django_valkey.hash_ring import HashRing
from django_valkey.typing import KeyT


class ShardClient(DefaultClient):
    _findhash = re.compile(r".*\{(.*)\}.*", re.I)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(self._server, (list, tuple, set)):
            self._server = [self._server]

        self._ring = HashRing(self._server)
        self._server_dict = self.connect()

    def get_client(self, *args, **kwargs):
        raise NotImplementedError

    def connect(self, index: int = 0) -> dict:
        connection_dict = {}
        for name in self._server:
            connection_dict[name] = self.connection_factory.connect(name)
        return connection_dict

    def get_server_name(self, _key: KeyT) -> str:
        key = str(_key)
        g = self._findhash.match(key)
        if g is not None and len(g.groups()) > 0:
            key = g.groups()[0]
        return self._ring.get_node(key)

    def get_server(self, key: KeyT):
        name = self.get_server_name(key)
        return self._server_dict[name]

    def add(
        self,
        key: KeyT,
        value: EncodableT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().add(
            key=key, value=value, version=version, client=client, timeout=timeout
        )

    def get(
        self,
        key: KeyT,
        default: Any | None = None,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> Any:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().get(key=key, default=default, version=version, client=client)

    def get_many(
        self,
        keys: KeyT,
        version: int | None = None,
        _client: Valkey | Any | None = None,
    ) -> dict:
        if not keys:
            return {}

        recovered_data = {}

        new_keys = [self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        for key in new_keys:
            client = self.get_server(key)
            value = self.get(key=key, version=version, client=client)

            if value is None:
                continue

            recovered_data[map_keys[key]] = value
        return recovered_data

    def mget(self, *args, **kwargs):
        raise NotImplementedError

    def set(
        self,
        key: KeyT,
        value: EncodableT,
        timeout: int | float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Valkey | Any | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().set(
            key=key,
            value=value,
            timeout=timeout,
            version=version,
            client=client,
            nx=nx,
            xx=xx,
        )

    def set_many(
        self,
        data: dict[KeyT, EncodableT],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> None:
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        for key, value in data.items():
            self.set(key, value, timeout, version=version, client=client)

    def mset(self, *args, **kwargs):
        raise NotImplementedError

    def has_key(
        self, key: KeyT, version: int | None = None, client: Valkey | Any | None = None
    ) -> bool:
        """
        Test if key exists.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        key = self.make_key(key, version=version)
        try:
            return client.exists(key) == 1
        except ConnectionError as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete(
        self,
        key: KeyT,
        version: int | None = None,
        prefix: str | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().delete(key=key, version=version, client=client)

    def ttl(
        self, key: KeyT, version: int | None = None, client: Valkey | Any | None = None
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().ttl(key=key, version=version, client=client)

    def pttl(
        self, key: KeyT, version: int | None = None, client: Valkey | Any | None = None
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key
        in milliseconds. If key is a non-volatile key, it returns None.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pttl(key=key, version=version, client=client)

    def persist(
        self, key: KeyT, version: int | None = None, client: Valkey | Any | None = None
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().persist(key=key, version=version, client=client)

    def expire(
        self,
        key: KeyT,
        timeout: int | timedelta,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().expire(key=key, timeout=timeout, version=version, client=client)

    def pexpire(
        self,
        key: KeyT,
        timeout: int | timedelta,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pexpire(key=key, timeout=timeout, version=version, client=client)

    def pexpire_at(
        self,
        key: KeyT,
        when: datetime | int,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when`` on a shard client.
        ``when`` which can be represented as an integer indicating unix
        time or a Python datetime object.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pexpire_at(key=key, when=when, version=version, client=client)

    def expire_at(
        self,
        key: KeyT,
        when: datetime | int,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when`` on a shard client.
        ``when`` which can be represented as an integer indicating unix
        time or a Python datetime object.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().expire_at(key=key, when=when, version=version, client=client)

    def get_lock(
        self,
        key,
        version=None,
        timeout=None,
        sleep=0.1,
        blocking: bool = True,
        blocking_timeout=None,
        client=None,
        lock_class=None,
        thread_local=True,
    ):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            client=client,
            blocking_timeout=blocking_timeout,
            lock_class=lock_class,
            thread_local=thread_local,
        )

    # TODO: delete in future.
    lock = get_lock

    def delete_many(
        self, keys, version=None, _client: Valkey | Any | None = None
    ) -> int:
        """
        Remove multiple keys at once.
        """
        res = 0
        for key in [self.make_key(k, version=version) for k in keys]:
            client = self.get_server(key)
            res += self.delete(key, client=client)
        return res

    def incr_version(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        new_key, old_key, value, ttl, version = self._incr_version(
            key, delta, version, client
        )
        self.set(new_key, value, timeout=ttl, client=self.get_server(new_key))
        self.delete(old_key, client=client)
        return version + delta

    def incr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Valkey | Any | None = None,
        **kwargs,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().incr(key=key, delta=delta, version=version, client=client)

    def decr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().decr(key=key, delta=delta, version=version, client=client)

    def iter_keys(
        self,
        search: str,
        itersize: int | None = None,
        client: Valkey | Any | None = None,
        version: int | None = None,
    ):
        """Not Implemented"""
        error_message = "iter_keys not supported on sharded client"
        raise NotImplementedError(error_message)

    def keys(
        self,
        search: str,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> list[str]:
        pattern = self.make_pattern(search, version=version)
        keys = []
        try:
            for connection in self._server_dict.values():
                keys.extend(connection.keys(pattern))
        except ConnectionError as e:
            # FIXME: technically all clients should be passed as `connection`.
            client = self.get_server(pattern)
            raise ConnectionInterrupted(connection=client) from e

        return [self.reverse_key(k.decode()) for k in keys]

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        client: Valkey | Any | None = None,
        itersize: int | None = None,
        prefix: str | None = None,
    ) -> int:
        """
        Remove all keys matching pattern.
        """
        pattern = self.make_pattern(pattern, version=version, prefix=prefix)
        kwargs = {"match": pattern}
        if itersize:
            kwargs["count"] = itersize

        keys = []
        for connection in self._server_dict.values():
            keys.extend(key for key in connection.scan_iter(**kwargs))

        res = 0
        if keys:
            for connection in self._server_dict.values():
                res += connection.delete(*keys)
        return res

    def _close(self) -> None:
        for client in self._server_dict.values():
            self.disconnect(client=client)

    def touch(
        self,
        key: KeyT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().touch(key=key, timeout=timeout, version=version, client=client)

    def clear(self, client=None) -> None:
        for connection in self._server_dict.values():
            connection.flushdb()

    def sadd(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sadd(key, *values, version=version, client=client)

    def scard(
        self,
        key: KeyT,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().scard(key=key, version=version, client=client)

    def smembers(
        self,
        key: KeyT,
        version: int | None = None,
        client: Valkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().smembers(
            key=key, version=version, client=client, return_set=return_set
        )

    def smove(
        self,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            source = self.make_key(source, version=version)
            client = self.get_server(source)
            destination = self.make_key(destination, version=version)

        return super().smove(
            source=source,
            destination=destination,
            member=member,
            version=version,
            client=client,
        )

    def srem(
        self,
        key: KeyT,
        *members,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().srem(key, *members, version=version, client=client)

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Valkey | Any | None = None,
        return_set: bool = True,
    ) -> tuple[int, builtins.set[Any]] | tuple[int, list[Any]]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sscan(
            key=key,
            cursor=cursor,
            match=match,
            count=count,
            version=version,
            client=client,
            return_set=return_set,
        )

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Valkey | Any | Any | None = None,
    ) -> Iterator[Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sscan_iter(
            key=key, match=match, count=count, version=version, client=client
        )

    def srandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Valkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set | list | Any:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().srandmember(
            key=key, count=count, version=version, client=client, return_set=return_set
        )

    def sismember(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sismember(key, member, version=version, client=client)

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Valkey | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set | list | Any:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().spop(
            key=key, count=count, version=version, client=client, return_set=return_set
        )

    def smismember(
        self,
        key: KeyT,
        *members,
        version: int | None = None,
        client: Valkey | Any | None = None,
    ) -> list[bool]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().smismember(key, *members, version=version, client=client)
