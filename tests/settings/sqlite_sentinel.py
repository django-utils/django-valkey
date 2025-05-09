SECRET_KEY = "django_tests_secret_key"

DJANGO_VALKEY_CONNECTION_FACTORY = "django_valkey.pool.SentinelConnectionFactory"

SENTINELS = [("127.0.0.1", 26379)]

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["valkey://mymaster?db=1"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.SentinelClient",
            "SENTINELS": SENTINELS,
        },
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://missing_service?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.DefaultClient",
            "SENTINELS": SENTINELS,
        },
    },
    "sample": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://mymaster?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.SentinelClient",
            "SENTINELS": SENTINELS,
        },
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://mymaster?db=1",
        "KEY_PREFIX": "test-prefix",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.DefaultClient",
            "SENTINELS": SENTINELS,
        },
    },
}

INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False
