from typing import Dict

from valkey.cluster import ValkeyCluster
from valkey.typing import KeyT, EncodableT

from django_valkey.base_client import BaseClient, ClientCommands, _main_exceptions
from django_valkey.exceptions import ConnectionInterrupted


class DefaultClusterClient(ClientCommands, BaseClient[ValkeyCluster]):
    CONNECTION_FACTORY_PATH = (
        "django_valkey.cluster_cache.pool.ClusterConnectionFactory"
    )

    def readonly(self, target_nodes=None, client=None):
        client = self._get_client(write=True, client=client)
        return client.readonly(target_nodes)

    def readwrite(self, target_nodes=None, client=None):
        client = self._get_client(write=True, client=client)
        return client.readwrite(target_nodes)

    def keys(
        self,
        pattern="*",
        target_nodes=ValkeyCluster.DEFAULT_NODE,
        version=None,
        client=None,
        **kwargs,
    ):
        client = self._get_client(client=client)
        pattern = self.make_pattern(pattern, version=version)

        try:
            keys = client.keys(pattern=pattern, target_nodes=target_nodes, **kwargs)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return {self.reverse_key(key.decode()) for key in keys}

    def mset(
        self,
        data: Dict[KeyT, EncodableT],
        version=None,
        client=None,
        nx=False,
        atomic=True,
    ) -> bool | list[bool]:
        """
        Access valkey's mset method.
        it is important to take care of cluster limitations mentioned here: https://valkey-py.readthedocs.io/en/latest/clustering.html#multi-key-commands
        """
        data = {
            self.make_key(k, version=version): self.encode(v) for k, v in data.items()
        }
        client = self._get_client(write=True, client=client)
        if not atomic:
            return client.mset_nonatomic(data)
        if nx:
            return client.msetnx(data)
        try:
            return client.mset(data)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def msetnx(self, data: Dict[KeyT, EncodableT], version=None, client=None) -> bool:
        try:
            return self.mset(data, version=version, client=client, nx=True)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def mset_nonatomic(
        self, data: Dict[KeyT, EncodableT], version=None, client=None
    ) -> list[bool]:
        try:
            return self.mset(data, version=version, client=client, atomic=False)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    set_many = mset_nonatomic

    def mget_nonatomic(self, keys, version=None, client=None):
        client = self._get_client(client=client)
        map_keys = {self.make_key(k, version=version): k for k in keys}
        try:
            values = client.mget_nonatomic(map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        recovered_data = {}
        for key, value in zip(keys, values):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)
        return recovered_data

    def keyslot(self, key, version=None, client=None):
        client = self._get_client(client=client)
        key = self.make_key(key, version=version)
        return client.keyslot(key)

    def flush_cache(self, client=None):
        client = self._get_client(client=client)
        return client.flush_cache()

    def invalidate_key_from_cache(self, client=None):
        client = self._get_client(client=client)
        return client.invalidate_key_from_cache()
