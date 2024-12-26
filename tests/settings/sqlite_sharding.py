SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:6379?db=1", "valkey://127.0.0.1:6379?db=2"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.ShardClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:56379?db=1", "valkey://127.0.0.1:56379?db=2"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.ShardClient"},
    },
    "sample": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1,valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.ShardClient"},
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.ShardClient"},
        "KEY_PREFIX": "test-prefix",
    },
}


VALKEY = {
    "default": {
        "BACKEND": "django_valkey.server.sync_server.ValkeyServer",
        "LOCATION": ["valkey://127.0.0.1:6379?db=1", "valkey://127.0.0.1:6379?db=2"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.server.sync_server.client.ShardClient"
        },
    }
}


INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False
