import re
from collections import OrderedDict
from typing import Any, List, Dict

from valkey import Valkey
from valkey.typing import EncodableT, KeyT

from django_valkey.base_client import DEFAULT_TIMEOUT, _main_exceptions
from django_valkey.client.default import DefaultClient
from django_valkey.exceptions import ConnectionInterrupted
from django_valkey.hash_ring import HashRing

"""
supported methods:

`add()`
`get()`
`get_many()`
`set()`
`set_many()`
`has_key()`
`delete()`
`delete_many()`
`delete_pattern()`
`ttl()`
`pttl()`
`persist()`
`expire()`
`expire_at()`
`pexpire()`
`pexpire_at()`
`get_lock()`
`incr_version()`
`incr()`
`decr()`
`keys()`
`close()`
`touch()`
`clear()`
`sadd()`
`scard()`
`smembers()`
`smove()`
`srem()`
`sscan()`
`sscan_iter()`
`srandmember()`
`sismember()`
`spop()`
`smismember()`
"""


class ShardClient(DefaultClient):
    _findhash = re.compile(r".*\{(.*)\}.*", re.I)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(self._server, (list, tuple, set)):
            self._server = [self._server]

        self._ring = HashRing(self._server)
        self._server_dict = self.connect()

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

    def get_client(self, key: KeyT = None, **kwargs):
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
            client = self.get_client(key=key)

        return super().add(
            key=key, value=value, version=version, client=client, timeout=timeout
        )

    def get_many(
        self,
        keys: KeyT,
        version: int | None = None,
        _client: Valkey | Any | None = None,
    ) -> OrderedDict:
        if not keys:
            return OrderedDict()

        recovered_data = OrderedDict()

        new_keys = [self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        for key in new_keys:
            client = self.get_client(key=key)
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
            client = self.get_client(key=key)

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
        data: Dict[KeyT, EncodableT],
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

    def delete_many(
        self, keys, version=None, _client: Valkey | Any | None = None
    ) -> int:
        """
        Remove multiple keys at once.
        """
        res = 0
        for key in [self.make_key(k, version=version) for k in keys]:
            client = self.get_client(key=key)
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
            client = self.get_client(key=key)

        new_key, old_key, value, ttl, version = self._incr_version(
            key, delta, version, client
        )
        self.set(new_key, value, timeout=ttl, client=self.get_client(key=new_key))
        self.delete(old_key, client=client)
        return version + delta

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
    ) -> List[str]:
        pattern = self.make_pattern(search, version=version)
        keys = []
        try:
            for connection in self._server_dict.values():
                keys.extend(connection.keys(pattern))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._server_dict.values()) from e

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
        try:
            for connection in self._server_dict.values():
                keys.extend(key for key in connection.scan_iter(**kwargs))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._server_dict.values()) from e

        res = 0
        if keys:
            try:
                for connection in self._server_dict.values():
                    res += connection.delete(*keys)
            except _main_exceptions as e:
                raise ConnectionInterrupted(
                    connection=self._server_dict.values()
                ) from e
        return res

    def _close(self) -> None:
        for client in self._server_dict.values():
            self.disconnect(client=client)

    def clear(self, client=None) -> None:
        for connection in self._server_dict.values():
            try:
                connection.flushdb()
            except _main_exceptions as e:
                raise ConnectionInterrupted(connection=connection) from e
