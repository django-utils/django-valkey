SECRET_KEY = "django_tests_secret_key"
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:6379?db=1", "valkey://127.0.0.1:6379?db=1"],
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "sample": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379:1,valkey://127.0.0.1:6379:1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
    },
    "with_prefix": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"},
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

ROOT_URLCONF = "tests.settings.urls"
