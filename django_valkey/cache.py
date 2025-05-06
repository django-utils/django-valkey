from valkey import Valkey

from django_valkey.base import BaseValkeyCache, BackendCommands
from django_valkey.client import DefaultClient


class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey], BackendCommands):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"
