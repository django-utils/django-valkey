from typing import Any

from django.utils.module_loading import import_string
from valkey.cluster import ValkeyCluster
from valkey.connection import ConnectionPool, DefaultParser

from django_valkey.base_pool import BaseConnectionFactory


class ClusterConnectionFactory(BaseConnectionFactory[ValkeyCluster, ConnectionPool]):
    path_base_cls = "valkey.cluster.ValkeyCluster"

    def __init__(self, options: dict):
        base_client_cls = options.get("BASE_CLIENT_CLASS", self.path_base_cls)
        self.base_client_cls: type[ValkeyCluster] = import_string(base_client_cls)
        self.base_client_cls_kwargs = options.get("BASE_CLIENT_CLS_KWARGS", {})

        self.options = options

    def disconnect(self, connection: ValkeyCluster) -> None:
        connection.disconnect_connection_pools()

    def get_parser_cls(self):
        cls = self.options.get("PARSER_CLS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    def connect(self, url: str) -> ValkeyCluster:
        params = self.make_connection_params(url)
        return self.get_connection(params)

    def get_connection(self, params: dict) -> ValkeyCluster | Any:
        return self.base_client_cls(
            url=params["url"],
            parser_class=params["parser_class"],
            **self.base_client_cls_kwargs
        )
