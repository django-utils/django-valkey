from urllib.parse import urlparse, parse_qs

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey.asyncio import Valkey as AValkey
from valkey.asyncio.connection import ConnectionPool, DefaultParser
from valkey.asyncio.sentinel import Sentinel
from valkey._parsers.url_parser import to_bool

from django_valkey.base_pool import BaseConnectionFactory
from django_valkey.typings import AsyncDefaultParserT


class AsyncConnectionFactory(BaseConnectionFactory[AValkey]):
    path_pool_cls = "valkey.asyncio.connection.ConnectionPool"
    path_base_cls = "valkey.asyncio.client.Valkey"

    async def disconnect(self, connection: type[AValkey]) -> None:
        await connection.connection_pool.disconnect()

    def get_parser_cls(self) -> AsyncDefaultParserT:
        cls = self.options.get("PARSER_CLS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    async def connect(self, url: str) -> AValkey:
        params = self.make_connection_params(url)
        return await self.get_connection(params)

    async def get_connection(self, params: dict) -> AValkey:
        pool = self.get_or_create_connection_pool(params)
        return await self.base_client_cls(
            connection_pool=pool, **self.base_client_cls_kwargs
        )


class AsyncSentinelConnectionFactory(AsyncConnectionFactory):
    def __init__(self, options: dict):
        try:
            self.sentinels = options["SENTINELS"]
        except KeyError:
            e = "SENTINELS must be provided as a list of (host, port)."
            raise ImproperlyConfigured(e)

        options.setdefault(
            "CONNECTION_POOL_CLASS", "valkey.asyncio.sentinel.SentinelConnectionPool"
        )
        super().__init__(options)
        connection_kwargs = self.make_connection_params(None)
        connection_kwargs.pop("url")
        connection_kwargs.update(self.pool_cls_kwargs)
        self._sentinel = Sentinel(
            self.sentinels,
            sentinel_kwargs=options.get("SENTINEL_KWARGS"),
            **connection_kwargs,
        )

    def get_connection_pool(self, params: dict) -> ConnectionPool:
        url = urlparse(params["url"])
        cp_params = params
        cp_params.update(service_name=url.hostname, sentinel_manager=self._sentinel)
        pool = super().get_connection_pool(cp_params)

        is_master: list[str] | None = parse_qs(url.query).get("is_master")
        if is_master:
            pool.is_master = to_bool(is_master[0])

        return pool
