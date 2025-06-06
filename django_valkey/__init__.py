VERSION = (0, 2, 1)
__version__ = ".".join(map(str, VERSION))


def get_valkey_connection(alias="default", write=True, key=None):
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

    if hasattr(cache.client, "get_server_name"):
        return cache.client.get_client(key=key)

    return cache.client.get_client(write)
