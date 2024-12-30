from valkey.asyncio.client import Valkey as AValkey

from django_valkey.base import BaseValkeyCache, AsyncCacheCommands
from django_valkey.async_cache.client.default import AsyncDefaultClient


class AsyncValkeyCache(
    BaseValkeyCache[AsyncDefaultClient, AValkey], AsyncCacheCommands
):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"
