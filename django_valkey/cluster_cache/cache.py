from valkey.cluster import ValkeyCluster

from django_valkey.base import BaseValkeyCache, BackendCommands
from django_valkey.cluster_cache.client import DefaultClusterClient


class ClusterCommands:
    def mget_nonatomic(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.mget_nonatomic(*args, **kwargs)

    def mset_nonatomic(self: "ClusterValkeyCache", *args, **kwargs):
        return self.client.mset_nonatomic(*args, **kwargs)


class ClusterValkeyCache(
    BaseValkeyCache[DefaultClusterClient, ValkeyCluster],
    ClusterCommands,
    BackendCommands,
):
    DEFAULT_CLIENT_CLASS = "django_valkey.cluster_cache.client.DefaultClusterClient"


for name, value in vars(BackendCommands).items():
    if original := getattr(value, "original", None):
        setattr(ClusterValkeyCache, name, original)
