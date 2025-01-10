SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:16379", "valkey://127.0.0.1:16380"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.cluster_cache.client.DefaultClusterClient"
        },
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=0",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.cluster_cache.client.DefaultClusterClient"
        },
    },
    "sample": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkey://127.0.0.1:16381:0,valkey://127.0.0.1:16379:0",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.cluster_cache.client.DefaultClusterClient"
        },
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkey://127.0.0.1:16380?db=0",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.cluster_cache.client.DefaultClusterClient"
        },
        "KEY_PREFIX": "test-prefix",
    },
}

# Include `django.contrib.auth` and `django.contrib.contenttypes` for mypy /
# django-stubs.

# See:
# - https://github.com/typeddjango/django-stubs/issues/318
# - https://github.com/typeddjango/django-stubs/issues/534
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
]

USE_TZ = False
