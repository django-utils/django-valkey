from valkey import Valkey

from django_valkey.base_client import BaseClient, ClientCommands


class DefaultClient(BaseClient[Valkey], ClientCommands[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
