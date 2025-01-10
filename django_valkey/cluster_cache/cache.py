from valkey.cluster import ValkeyCluster

from django_valkey.base import (
    BaseValkeyCache,
    SyncCacheCommands,
    CONNECTION_INTERRUPTED,
)
from django_valkey.cluster_cache.client import DefaultClusterClient


class ClusterSyncCacheCommands(SyncCacheCommands):
    def set(self, *args, **kwargs):
        return self.client.set(*args, **kwargs)

    def incr(self, *args, **kwargs):
        return self.client.incr(*args, **kwargs)

    def decr(self, *args, **kwargs):
        return self.client.decr(*args, **kwargs)

    def incr_version(self, *args, **kwargs):
        return self.client.incr_version(*args, **kwargs)

    def add(self, *args, **kwargs):
        return self.client.add(*args, **kwargs)

    def get(self, key, default=None, version=None, client=None):
        value = self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default

        return value

    def _get(self, key, default=None, version=None, client=None):
        return self.client.get(key, default, version, client)

    def delete(self, *args, **kwargs):
        result = self.client.delete(*args, **kwargs)
        return bool(result)

    def delete_pattern(self, *args, **kwargs):
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return self.client.delete_pattern(*args, **kwargs)

    def delete_many(self, *args, **kwargs):
        return self.client.delete_many(*args, **kwargs)

    def clear(self):
        return self.client.clear()

    def get_many(self, *args, **kwargs):
        return self.client.get_many(*args, **kwargs)

    def mget(self, *args, **kwargs):
        return self.client.mget(*args, **kwargs)

    def mget_nonatomic(self, *args, **kwargs):
        return self.client.mget_nonatomic(*args, **kwargs)

    def set_many(self, *args, **kwargs):
        return self.client.set_many(*args, **kwargs)

    def mset(self, *args, **kwargs):
        return self.client.mset(*args, **kwargs)

    def mset_nonatomic(self, *args, **kwargs):
        return self.client.mset_nonatomic(*args, **kwargs)

    def close(self):
        self.client.close()

    def has_key(self, *args, **kwargs):
        return self.client.has_key(*args, **kwargs)

    def touch(self, *args, **kwargs):
        return self.client.touch(*args, **kwargs)


class ClusterValkeyCache(
    BaseValkeyCache[DefaultClusterClient, ValkeyCluster], ClusterSyncCacheCommands
):
    DEFAULT_CLIENT_CLASS = "django_valkey.cluster_cache.client.DefaultClusterClient"
