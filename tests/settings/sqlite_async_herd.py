SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:6379?db=5"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient"},
    },
    "sample": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379:1,valkey://127.0.0.1:6379:1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient"},
    },
    "with_prefix": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient"},
        "KEY_PREFIX": "test-prefix",
    },
}

# Include `django.contrib.auth` and `django.contrib.contenttypes` for mypy /
# django-stubs.

# See:
# - https://github.com/typeddjango/django-stubs/issues/318
# - https://github.com/typeddjango/django-stubs/issues/534
INSTALLED_APPS = [
    "django.contrib.sessions",
]

CACHE_HERD_TIMEOUT = 2

USE_TZ = False

ROOT_URLCONF = "tests.settings.urls"
