from valkey.asyncio.client import Valkey as AValkey

from django_valkey.base import BaseValkeyCache, AsyncBackendCommands
from django_valkey.async_cache.client.default import AsyncDefaultClient


class AsyncValkeyCache(
    BaseValkeyCache[AsyncDefaultClient, AValkey], AsyncBackendCommands
):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"
    is_async = True
