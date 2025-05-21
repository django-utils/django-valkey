SECRET_KEY = "django_tests_secret_key"

SENTINELS = [("127.0.0.1", 26379)]

conn_factory = "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": ["valkey://mymaster?db=5"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_FACTORY": conn_factory,
        },
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://missing_service?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_FACTORY": conn_factory,
        },
    },
    "sample": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://mymaster?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncSentinelClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_FACTORY": conn_factory,
        },
    },
    "with_prefix": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://mymaster?db=1",
        "KEY_PREFIX": "test-prefix",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_FACTORY": conn_factory,
        },
    },
}

INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False

ROOT_URLCONF = "tests.settings.urls"
