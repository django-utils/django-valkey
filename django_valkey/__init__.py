from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from django_valkey.exceptions import InvalidValkeyBackendError

VERSION = (0, 1, 8)
__version__ = ".".join(map(str, VERSION))


def get_valkey_connection(alias="default", write=True):
    """
    Helper used for obtaining a raw valkey client.
    """

    from django.core.cache import caches

    cache = caches[alias]

    error_message = "This backend does not support this feature"
    if not hasattr(cache, "client"):
        raise NotImplementedError(error_message)

    if not hasattr(cache.client, "get_client"):
        raise NotImplementedError(error_message)

    return cache.client.get_client(write)


DEFAULT_VALKEY_ALIAS = "default"


class ValkeyHandler(BaseConnectionHandler):
    settings_name = "VALKEY"
    exception_class = InvalidValkeyBackendError

    def create_connection(self, alias):
        params = self.settings[alias].copy()
        backend = params.pop("BACKEND")
        location = params.pop("LOCATION", "")
        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidValkeyBackendError(f"Could not find backend {backend}: {e}")
        return backend_cls(location, params)


servers = ValkeyHandler()
valkey = ConnectionProxy(servers, DEFAULT_VALKEY_ALIAS)
