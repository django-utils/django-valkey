# Advanced configurations

## Configure the database URL

as we discussed in [Basic Configuration](basic_configurations.md) you can configure your database URL like this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379",
        "OPTIONS": {...}
    }
}
```

or if you are using ACL, you can do something like this:

```python
CACHES = {
    "default": {
        # ...
        "LOCATION": "valkey://django@localhost:6379/0",
        "OPTIONS": {
            "PASSWORD": "mysecret"
        }
    }
}
```

Now, lets look at other ways to configure this:

1. valkey://[[username]:[password]]@localhost:6379
2. valkeys://[[username]:[password]@localhost:6379
3. unix://[[username]:[password]@/path/to/socket.sock
4. unix://[username@]/path/to/socket.sock?db=0[&password=password]


These three URL schemes are supported:
1. ``valkey://``: creates a normal TCP socket connection
2. ``valkeys://``: creates a SSL wrapped TCP socket connection
3. ``unix://``: creates a Unix Domain Socket connection

### Specify a database number:

you can specify a database number in your URL like this:
* A `db` querystring option, e.g. `valkey://localhost:6379?db=0`
* if using the `valkey://` scheme, the path argument of the URL, e.g. `valkey://localhost:6379/0`


## RESP3 support

to enable RESP3, like other connections you can configure your server like this:

```python
CACHE = {
    "default": {
        # ...
        "LOCATION": "valkey://django@localhost:6379?protocol=3",
    }
}
```

## Configure as session backend

django can by default use any cache backend as a session backend and you benefit from that by using django-valkey as backend for session storage without installing any additional backends:
just add these settings to your settings.py

```python
SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "default
```

## Configure the client

by default django-valkey uses `django_valkey.client.default.DefaultClient` to work with the underlying API
you can, however, plug in another client, either one that we provide or one of your own

the DefaultClient is used by default, but if you want to be explicit you can set it like this:

```python
CACHE = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": [
            "valkey://127.0.0.1:6379",
        ],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.DefaultClient"
        }
    }
}
```

### Use Sentinel client

In order to facilitate using [Valkey Sentinels](https://valkey.io/topics/sentinel), django-valkey comes with a built-in sentinel client and a connection factory

since this is a big topic, you can find detailed explanation in [Sentinel Configuration](sentinel_configurations.md)
but for a simple configuration this will work:

```python
SENTINELS = [
    ("127.0.0.1", 26379),  # a list of (host name, port) tuples.
]

CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
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

*note*: the sentinel client uses the sentinel connection factory by default.
you can change this behaviour by setting `DJANGO_VALKEY_CONNECTION_FACTORY` in your django settings or `CONNECTION_FACTORY` in your `CACHES` OPTIONS.

### Use Shard client

this pluggable client implements client-side sharding. to use it, change you cache settings to look like this:
*WARNING*: sharded client is experimental

```python
CACHE = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": [
            "valkey://127.0.0.1:6379/1",
            "valkey://127.0.0.1:6379/2",
        ],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.ShardClient"
        }
    }
}
```

### Use Herd client

This pluggable client help dealing with the thundering herd problem. you can read more about it on [Wikipedia](https://en.wikipedia.org/wiki/Thundering_herd_problem)
to use this client change your configs to look like this:

```python
CACHES = {
    "default": {
    # ...
    "OPTIONS": {
        "CLIENT_CLASS": "django_valkey.client.HerdClient",
        }
    }
}
# optional:
CACHE_HERD_TIMEOUT = 60  # default is 60
```

## Configure the serializer

**IMPORTANT NOTE:** if you are using the cache server with django's cache middleware or `cache_page` decorator, don't change the serializer.

by default django-valkey uses python's pickle library to serialize data.
you can stick to pickle, use one of the alternative serializes we provide, or write your own and plug it in.

django-valkey's pickle serializer uses pickle.DEFAULT_PROTOCOL as the default protocol version, but if you want to change it you can do it like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "PICKLE_VERSION": 5
        }
    }
}
```

*note*: the pickle version shouldn't be higher that `pickle.HIGHEST_PROTOCOL`

### Use Json serializer

if you want to use the json serializer instead of pickle, add it to the configuration like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "SERIALIZER": "django_valkey.serializer.json.JSONSerializer",
            # ...
        }
    }
}
```    

and you're good to go

### Use Msgpack serializer

to use the msgpack serializer you should first install the msgpack package as explained in :ref:`msgpack`
then configure your settings like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "SERIALIZER": "django_valkey.serializer.msgpack.MSGPackSerializer",
            # ...
        }
    }
}
```

and done

### Fun fact
you can serialize every type in the python built-ins, and probably non built-ins, but you have to check which serializer supports that type.

## Pluggable Compressors

by default django-valkey uses the `django_valkey.compressors.identity.IdentityCompressor` class as compressor, however you should *note* that this class doesn't compress anything;
it only returns the same value it's been passed to, but why do we have it then?
the reason is that this class works as a placeholder, so when we want to use a compressor, we can swap the classes.

django valkey comes with a number of built-in compressors (some of them need a 3rd-party package to be installed)
as of now we have these compressors available:

* [Brotli Compression](compressors.md#brotli-compression)
* [Bz2 Compression](compressors.md#bz2-compression)
* [Gzip Compression](compressors.md#gzip-compression)
* [Lz4 Compression](compressors.md#lz4-compression)
* [Lzma Compression](compressors.md#lzma-compression)
* [Zlib Compression](compressors.md#zlib-compression)
* [Zstd Compression](compressors.md#zstd-compression)

and you can easily write your own compressor and use that instead if you want.

since the list is long we'll look into compressor configs in [Compressor Support](compressors.md)

## Pluggable parsers

valkey-py (the valkey client used by django-valkey) comes with a pure python parser that works well for most common tasks, but if you want some performance boost you can use libvalkey.

libvalkey is a Valkey client written in C, and it has its own parser that can be used with django-valkey.

the only thing you need to do is install libvalkey:

```shell
pip install django-valkey[libvalkey]
```

and valkey-py will take care of te rest

### Use a custom parser

if you want to use your own parser just add it to the `OPTIONS` like so:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "PARSER_CLASS": "path.to.parser",
        }
    }
}
```

## Pluggable Base Client

django valkey uses the Valkey client `valkey.client.Valkey` as a base client by default.
But It is possible to use an alternative client.

You can customize the client used by django-valkey by setting `BASE_CLIENT_CLASS` in you settings.
optionally you can provide arguments to be passed to this class by setting `BASE_CLIENT_KWARGS`.

```python
CACHES = {
    "default": {
        "OPTIONS": {
            "BASE_CLIENT_CLASS": "path.to.client",
            "BASE_CLIENT_KWARGS": {"something": True},
        }
    }
}
```

## Connection Factory

django valkey has two connection factories built-in, `django-valkey.pool.ConnectionFactory` and `django_valkey.pool.SentinelConnectionFactory`.
if you need to use another one, you can configure it globally by setting `DJANGO_VALKEY_CONNECTION_FACTORY` or per server by setting `CONNECTION_FACTORY` in `OPTIONS`
it could look like this:

```python
DJANGO_VALKEY_CONNECTION_FACTORY = "path.to.my.factory"

# or:

CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "CONNECTION_FACTORY": "path.to.it",
        }
    },
    "another_service": {
        # ...
        "OPTIONS": {
            "CONNECTION_FACTORY": "path.to.another",
        }
    }
}
```

a connection factory could look like this:

```python
class ConnectionFactory(object):
    def get_connection_pool(self, params: dict):
        # Given connection parameters in the `params` argument, return new
        # connection pool. It should be overwritten if you want do
        # something before/after creating the connection pool, or return
        # your own connection pool.
        pass

    def get_connection(self, params: dict):
        # Given connection parameters in the `params` argument, return a
        # new connection. It should be overwritten if you want to do
        # something before/after creating a new connection. The default
        # implementation uses `get_connection_pool` to obtain a pool and
        # create a new connection in the newly obtained pool.
        pass

    def get_or_create_connection_pool(self, params: dict):
        # This is a high layer on top of `get_connection_pool` for
        # implementing a cache of created connection pools. It should be
        # overwritten if you want change the default behavior.
        pass

    def make_connection_params(self, url: str) -> dict:
        # The responsibility of this method is to convert basic connection
        # parameters and other settings to fully connection pool ready
        # connection parameters.
        pass

    def connect(self, url: str):
        # This is really a public API and entry point for this factory
        # class. This encapsulates the main logic of creating the
        # previously mentioned `params` using `make_connection_params` and
        # creating a new connection using the `get_connection` method.
        pass
```

## Connection pools

Behind the scenes, django-valkey uses the underlying valkey-py connection pool
implementation, and exposes a simple way to configure it. Alternatively, you
can directly customize a connection/connection pool creation for a backend.

The default valkey-py behavior is to not close connections, recycling them when
possible.

### Configure default connection pool

The default connection pool is simple. For example, you can customize the
maximum number of connections in the pool by setting `CONNECTION_POOL_KWARGS`
in the `CACHES` setting:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        # ...
        "OPTIONS": {
            "CONNECTION_POOL_KWARGS": {"max_connections": 100}
        }
    }
}
```

Since the default connection pool passes all keyword arguments it doesn't use
to its connections, you can also customize the connections that the pool makes
by adding those options to `CONNECTION_POOL_KWARGS`:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "CONNECTION_POOL_KWARGS": {"max_connection": 100, "retry_on_timeout": True}
        }
    }
}
```

you can check [Access the connection pool](../commands/connection_pool_commands.md) to see how you can access the connection pool directly and see information about it

### Use your own connection pool

to use your own connection pool, set `CONNECTION_POOL_CLASS`  in your backends `OPTIONS`
it could look like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "CONNECTION_POOL_CLASS": "path.to.mypool",
        }
    }
}
```

for simplicity you can subclass the connection pool provided by valkey-py package:

```python
from valkey.connection import ConnectionPool

class MyOwnPool(ConnectionPool):
    pass
```

## Closing connection
by default django-valkey keeps the connection to valkey server after a `close()` call.
you can change this behaviour for all cache servers (globally) by `DJANGO_VALKEY_CLOSE_CONNECTION = True` in the django settings
or by setting `"CLOSE_CONNECTION": True` (at cache level) in the `OPTIONS` for each configured cache server.

```python
DJANGO_VALKEY_CLOSE_CONNECTION = True

# or:

CACHE = {
    "default": {
        # ...
        "OPTIONS": {
            "CLOSE_CONNECTION": True,
        }
    }
}
```

## SSL/TLS self-signed certificate

In case you encounter a Valkey server offering a TLS connection using a
self-signed certificate you may disable certification verification with the
following:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkeys://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.client.DefaultClient",
            "CONNECTION_POOL_KWARGS": {"ssl_cert_reqs": None}
        }
    }
}
```
