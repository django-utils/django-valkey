# Customizing django-valkey

The basics of how to introduce your own classes to be used by django-valkey has been discussed in length in [Advanced Configuration](configure/advanced_configurations.md).

in this section we're going to look at the base classes that django-valkey provides, and you can use them to write your classes faster.

django-valkey comes with three base classes: `django_valkey.base.BaseValkeyCache`, `django_valkey.base_client.BaseClient` and `django_valkey.base_pool.BaseConnectionFactory`.

## BaseValkeyCache

`BaseValkeyCache` inherits from django's `BaseCache` class and `typing.Generic`.
`BaseCache` adds basic cache functionality, such as `get()` and `set()`, and `typing.Generic` allows for a more robust type hinting.
`BaseValkeyCache` adds more valkey oriented methods to BaseCache, things like `expire()` and `get_lock()`, and uses Generic to type hint two things:
1. the client, such as `django_valkey.client.default.DefaultClient`.
2. the underlying backend, such as `valkey.Valkey`.

to inherit from this base class you can take the example of our own cache backend:

```python
from valkey import Valkey

from django_valkey.base import BaseValkeyCache
from django_valkey.client import DefaultClient

class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey]):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"
    ...
```        

the class attribute defined in the example is **mandatory**, it is so we can have imports in other modules.

`BaseValkeyCache` has both *sync* and *async* methods implemented, but there is no logic in them, most methods need to be overwritten.


## BaseClient
`BaseClient` inherits from `typing.Generic` to make cleaner type hints.
this class has all the logic necessary for a cache client (it is a copy of the old DefaultClient class), it finds the different servers and connects to them, add has all the commands that valkey supports.

the `typing.Generic` needs a backend to be passed in, e.g: `valkey.Valkey`

the base class also needs the subclasses to have a `CONNECTION_FACTORY_PATH` class variable pointing to the connection factory class.

an example code would look like this:

```python
from valkey import Valkey

from django_valkey.base_client import BaseClient

class DefaultClient(BaseClient[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
```

*note* that CONNECTION_FACTORY_PATH is only used if `DJANGO_VALKEY_CONNECTION_FACTORY` is not set.


## BaseConnectionFactory

the `BaseConnectionFactory` inherits from `typing.Generic` to give more robust type hinting, and allow our four connection pools to have cleaner codebase.

to inherit from this class you need to pass in the underlying backend that you are using and the connection pool, for example this is one of the connection pools in this project:

```python
from valkey import Valkey
from valkey.connection import ConnectionPool

from django_valkey.base_pool import BaseConnectionFactory


class ConnectionFactory(BaseConnectionFactory[Valkey, ConnectionPool]):
    path_pool_cls = "valkey.connection.ConnectionPool"
    path_base_cls = "valkey.client.Valkey"
```

the two class attributes defined there are also **mandatory** since they are passed to other modules.

this base class has eight methods implemented, but four of them raise `NotImplementedError`, so let's have a look at those:

1. `connect()` this method can be both sync and async, depending on your work.
2. `disconnect()` this method, as well, can be both sync and async.
3. `get_connection()` in our implementation, connect() calls this method to get the connection, it also can be both sync and async, you can omit this one tho
4. `get_parser_cls()` this method can only be sync, it returns a parser class (and not object)
