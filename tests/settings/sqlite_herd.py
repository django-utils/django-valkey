SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:6379?db=5"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.HerdClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.HerdClient"},
    },
    "sample": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1,valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.HerdClient"},
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.HerdClient"},
        "KEY_PREFIX": "test-prefix",
    },
}

VALKEY = {
    "default": {
        "BACKEND": "django_valkey.server.sync_server.ValkeyServer",
        "LOCATION": ["valkey://127.0.0.1:6379?db=1", "valkey://127.0.0.1:6379?db=1"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.server.sync_server.client.HerdClient",
        },
    }
}

INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False

CACHE_HERD_TIMEOUT = 2
