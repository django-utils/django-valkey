from valkey.asyncio import Valkey as AValkey

from django_valkey.base_client import BaseClient, AsyncClientCommands


class AsyncDefaultClient(BaseClient[AValkey], AsyncClientCommands[AValkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.async_cache.pool.AsyncConnectionFactory"
