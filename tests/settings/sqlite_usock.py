SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["unix:///tmp/valkey.sock?db=1", "unix:///tmp/valkey.sock?db=1"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "sample": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1,valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
        "KEY_PREFIX": "test-prefix",
    },
}

VALKEY = {
    "default": {
        "BACKEND": "django_valkey.server.sync_server.ValkeyServer",
        "LOCATION": ["unix:///tmp/valkey.sock?db=1", "unix:///tmp/valkey.sock?db=1"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.server.sync_server.client.DefaultClient",
        },
    }
}

INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False
