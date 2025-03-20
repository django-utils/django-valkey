# Sentinel configuration

a sentinel configuration has these parts:

1. `DJANGO_VALKEY_CONNECTION_FACTORY`: you can use the ConnectionFactory or SentinelConnectionFactory. the sentinel client uses SentinelConnectionFactory by default.
   SentinelConnectionFactory inherits from ConnectionFactory but adds checks to see if configuration is correct, also adds features to make configuration more robust.

2. `CACHES["default"]["OPTIONS"]["CONNECTION_FACTORY"]`: does what the above option does, but only in the scope of the cache server it was defined in.

3. `CACHES["default"]["OPTIONS"]["CLIENT_CLASS"]`: setting the client class to SentinelClient will add some checks to ensure proper configs and makes working with primary and replica pools easier
   you can get by just using the DefaultClient but using SentinelClient is recommended.
4. `CACHES["default"]["OPTIONS"]["CONNECTION_POOL_CLASS"]`: if you have configured the above settings to use Sentinel friendly options you don't have to set this, otherwise you might want to set this to `valkey.sentinel.SentinelConnectionPool`.

5. `CACHES["default"]["OPTIONS"]["SENTINELS"]`: a list of (host, port) providing the sentinel's connection information.

6. `CACHES["default"]["OPTIONS"]["SENTINEL_KWARGS"]`: a dictionary of arguments sent down to the underlying Sentinel client

the below code is a bit long but comprehensive example of different ways to configure a sentinel backend.
*Note* that depending on how you configured your backend, you might need to adjust the `LOCATION` to fit other configs

```python
DJANGO_VALKEY_CONNECTION_FACTORY = "django_valkey.pool.SentinelConnectionFactory"

# SENTINELS is a list of (host name, port) tuples
# These sentinels are shared between all the examples, and are passed
# directly to valkey Sentinel. These can also be defined inline.
SENTINELS = [
  ('sentinel-1', 26379),
  ('sentinel-2', 26379),
  ('sentinel-3', 26379),
]

CACHES = {
    "default": {
    # ...
    "LOCATION": "valkey://service_name/db",  # note you should pass in valkey service name, not address
    "OPTIONS": {
        # While the default client will work, this will check you
        # have configured things correctly, and also create a
        # primary and replica pool for the service specified by
        # LOCATION rather than requiring two URLs.
        "CLIENT_CLASS": "django_valkey.client.SentinelClient",

        # these are passed directly to valkey sentinel
        "SENTINELS": SENTINELS,

        # optional
        "SENTINEL_KWARGS": {},

        # you can override the connection pool (optional)
        # (it is originally defined in connection factory)
        "CONNECTION_POOL_CLASS": "valkey.sentinel.SentinelConnectionPool",
        },
    },

    # a minimal example using the SentinelClient
    "minimal": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://minimal_service_name/db",

        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.SentinelClient",
            "SENTINELS": SENTINELS,
            },
        },

    # a minimal example using the DefaultClient
    "other": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": [
            # The DefaultClient is [primary, replicas], but with the
            # SentinelConnectionPool it only requires "is_master=1" for primary and "is_master=0" for replicas.
            "valkey://other_service_name/db?is_master=1",
            "valkey://other_service_name/db?is_master=0",
            ]
        "OPTIONS": {"SENTINELS": SENTINELS},
        },

    # a minimal example only using replicas in read only mode
    # (and the DefaultClient).
    "readonly": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://readonly_service_name/db?is_master=0",
        "OPTIONS": {"SENTINELS": SENTINELS},
     },
}
```


### Use sentinel and normal servers together
it is also possible to set some caches as sentinels and some as not:

```python
SENTINELS = [
   ('sentinel-1', 26379),
   ('sentinel-2', 26379),
   ('sentinel-3', 26379),
]
CACHES = {
    "sentinel": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.SentinelClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_POOL_CLASS": "valkey.sentinel.SentinelConnectionPool",
            "CONNECTION_FACTORY": "django_valkey.pool.SentinelConnectionFactory",
        },
    },
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.DefaultClient",
        },
    },
}
```
