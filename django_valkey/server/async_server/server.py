from django_valkey.async_cache.cache import AsyncValkeyCache


class AsyncValkeyServer(AsyncValkeyCache):
    DEFAULT_CLIENT_CLASS = (
        "django_valkey.server.async_server.client.default.AsyncDefaultClient"
    )
