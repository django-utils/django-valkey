# Advanced Async Configuration

most of the subject discussed in [Advanced configuration](../configure/advanced_configurations.md) apply to async mode as well, just don't use a sync client :)

also all the compressor details we talked about in [Compressor support](../configure/compressors.md) work as is in async mode

**Important**: the async clients are not compatible with django's cache middleware.
if you need those middlewares, consider using a sync client or implement a new middleware

## Clients

We have three async client, `AsyncDefaultClient`, available in `django_valkey.async_cache.client.default`, `AsyncHerdClient` available in `django_valkey.async_cache.client.herd` and `AsyncSentinelClient` at `django_valkey.async_cache.client.sentinel`.
the default client can also be used with sentinels, as we'll discuss later.

note that all clients are imported and available at `django_valkey.async_cache.client`

### Default client

the `AsyncDefaultClient` is configured by default by `AsyncValkeyCache`, so if you have configured that as your backend you are all set, but if you want to be explicit or use the client with a different backend you can write it like this:

```python
CACHES = {
    "async": {
        "BACKEND": "path.to.backend",
        "LOCATION": [
            "valkey://user:pass@127.0.0.1:6379",
        ]
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient",
        }
    }
}
```

or you can replace the client with your own like that.

### Sentinel Client

to support sentinels, django_valkey comes with a client and a connection factory, technically you don't need the connection factory, but it provides you with some nice features.
a dedicated page on sentinel client has been written in [Sentinel configuration](../configure/sentinel_configurations.md), tho that is for the sync version, the principle is the same.

the connection factory is at `django_valkey.async_cache.pool.AsyncSentinelConnectionFactory`.

to configure the async sentinel client you can write your settings like this:

```python
SENTINELS = [
    ("127.0.0.1", 26379),  # a list of (host name, port) tuples.
]

CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.SentinelClient",
            "SENTINELS": SENTINELS,

            # optional
            "SENTINEL_KWARGS": {}
        }
    }
}
```

*note*: the sentinel client uses the sentinel connection factory by default, you can change it by setting `DJANGO_VALKEY_CONNECTION_FACTORY` in your django settings or `CONNECTION_FACTORY` in your `CACHES` OPTIONS.

### Herd client

the herd client needs to be configured, but it's as simple as this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": ["valkey://127.0.0.1:6379"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient",
        }
    }
}
```        

## Connection Factory

django_valkey's async library comes with two connection factories, `AsyncConnectionFactory` for general uses and `AsyncSentinelConnectionFactory` for sentinel uses.

the default connection factory is `AsyncConnectionFactory`, so if you are using a sentinel server you should configure your caches like this:

```python
CACHES = {
    "async": {
        # ...
        "OPTIONS": {
            "CONNECTION_FACTORY": "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory"
        }
    }
}

CACHE_HERD_TIMEOUT = 20  # if not set, it's default to 60
```

or set it as the global connection factory like this:

```python
DJANGO_VALKEY_CONNECTION_FACTORY = "django_valkey.async_cache.client.default.AsyncDefaultClient"
```    

note that `"CONNECTION_FACTORY"` overrides `DJANGO_VALKEY_CONNECTION_FACTORY` for the specified server.

if you want to use another factory you can use the same code with the path to your class.

