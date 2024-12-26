from django_valkey.cache import ValkeyCache


class ValkeyServer(ValkeyCache):
    DEFAULT_CLIENT_CLASS = "django_valkey.sync_server.client.default.DefaultClient"
