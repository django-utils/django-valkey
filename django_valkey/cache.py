from valkey import Valkey

from django_valkey.base import BaseValkeyCache, SyncCacheCommands
from django_valkey.client import DefaultClient


class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey], SyncCacheCommands):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"

    def __contains__(self, item):
        return self.has_key(item)
