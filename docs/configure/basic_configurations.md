# Basic configuration

if you haven't installed django-valkey yet, head out to [Installation guide](../installation.md).


## Configure as cache backend

to start using django-valkey, change your django cache setting to something like this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379",
        "OPTIONS": {...}
        }
    }
```

django-valkey uses the valkey-py native URL notation for connection strings, it allows better interoperability and has a connection string in more "standard" way. will explore this more in [Advanced Configurations](advanced_configurations.md).

when using [Valkey's ACLs](https://valkey.io/topics/acl) you will need to add the username and password to the URL.
the login for the user `django` would look like this:

```python
CACHES = {
    "default": {
        # ...
        "LOCATION": "valkey://django:mysecret@localhost:6379/0",
        # ...
    }
}
```

you can also provide the password in the `OPTIONS` dictionary
this is specially useful if you have a password that is not URL safe
but *notice* that if a password is provided by the URL, it won't be overridden by the password in `OPTIONS`.

```python
CACHES = {
    "default": {
        "BACKEND": "django-valkey.cache.ValkeyCache",
        "LOCATION": "valkey://django@localhost:6379/0",
        "OPTIONS": {
            "PASSWORD": "mysecret"
        }
    }
}
```    

**Note:** you probably should read the password from environment variables


## Memcached exception behavior

In some situations, when Valkey is only used for cache, you do not want
exceptions when Valkey is down. This is default behavior in the memcached
backend and it can be emulated in django-valkey.

For setup memcached like behaviour (ignore connection exceptions), you should
set `IGNORE_EXCEPTIONS` settings on your cache configuration:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "IGNORE_EXCEPTIONS": True,
        }
    }
}
```

Also, if you want to apply the same settings to all configured caches, you can set the global flag in
your settings:

```python
DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
```

### Log exceptions when ignored

when ignoring exceptions with `IGNORE_EXCEPTIONS` or `DJANGO_VALKEY_IGNORE_EXCEPTION`, you may optionally log exceptions by setting the global variable `DJANGO_VALKEY_LOG_EXCEPTION` in your settings:

```python
DJANGO_VALKEY_LOG_IGNORED_EXCEPTION = True
```    

If you wish to specify a logger in which the exceptions are outputted, set the global variable `DJANGO_VALKEY_LOGGER` to the string name or path of the desired logger.
the default value is `__name__` if no logger was specified

```python
DJANGO_VALKEY_LOGGER = "some.logger"
```

## Socket timeout

Socket timeout can be set using `SOCKET_TIMEOUT` and
`SOCKET_CONNECT_TIMEOUT` options:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "SOCKET_CONNECT_TIMEOUT": 5,  # seconds
            "SOCKET_TIMEOUT": 5,  # seconds
        }
    }
}
```

`SOCKET_CONNECT_TIMEOUT` is the timeout for the connection to be established
and `SOCKET_TIMEOUT` is the timeout for read and write operations after the
connection is established.
