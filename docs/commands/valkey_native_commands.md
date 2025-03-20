# valkey commands

if you need to use valkey operations, you can access the client as follows:

```pycon
>>> from django.core.cache import cache
>>> cache.set("key", "value1", nx=True)
True
>>> cache.set("key", "value2", nx=True)
False
>>> cache.get("key")
"value1"
```

the list of supported commands is very long, if needed you can check the methods at ``django_valkey.cache.ValkeyCache```

### Infinite timeout

django-valkey comes with infinite timeouts supported out of the box. And it
behaves in the same way as django backend contract specifies:

- `timeout=0` expires the value immediately.
- `timeout=None` infinite timeout.

```python
cache.set("key", "value", timeout=None)
```

### Get and Set in bulk

django-valkey has two different kind of method for bulk get/set: atomic and non-atomic.

atomic operations are done with `mget()` and `mset()`

```pycon
>>> from django.core.cache import cache
>>> cache.mset({"a": 1, "b": 2})
>>> cache.mget(["a", "b"])
{"a": 1, "b": 2}
```

the non-atomic operations are done with `get_many()` and `set_many()`:

```pycon
>>> from django.core.cache import cache
>>> cache.set_many({"a": 1, "b": 2})
>>> cache.get_many(["a", "b"])
{"a": 1, "b": 2}
```

**Note**: django-redis users should note that in django redis `get_many()` is an atomic operation, but `set_many()` is non-atomic, but in `django-valkey` they are both non-atomic.

### Scan and Delete in bulk

when you need to search for keys that have similar patterns, or delete them, you can use the helper methods that come with django-valkey:

```pycon
>>> from django.core.cache import cache
>>> cache.keys("foo_*")
["foo_1", "foo_2"]
```

if you are looking for a very large amount of data, this is **not** suitable; instead use `iter_keys`.
this will return a generator that you can iterate over more efficiently.

```pycon
>>> from django.core.cache import cache
>>> cache.iter_keys("foo_*")
<generator object algo at 0x7ff432
>>> next(cache.iter_keys("foo_*))
'foo_1'
>>> foos = cache.iter_keys("foo_*")
>>> for i in foos:
...     print(i)
'foo_1'
'foo_2'
```

to delete keys, you should use `delete_pattern` which has the same glob pattern syntax as `keys` and returns the number of deleted keys.

```pycon
>>> from django.core.cache import cache
>>> cache.delete_pattern("foo_*")
2
```

To achieve the best performance while deleting many keys, you should set `DJANGO_VALKEY_SCAN_ITERSIZE` to a relatively
high number (e.g., 100_000) by default in Django settings or pass it directly to the `delete_pattern`.

```pycon
>>> from django.core.cache import cache
>>> cache.delete_pattern("foo_*", itersize=100_000)
```

### Get ttl (time-to-live) from key

with valkey you can access to ttl of any sorted key, to do so, django-valky exposes the `ttl` method.

the ttl method returns:

- `0` if key does not exist (or already expired).
- `None` for keys that exist but does not have expiration.
- the ttl value for any volatile key (any key that has expiration).

```pycon
>>> from django.core.cache import cache
>>> cache.set("foo", "value", timeout=25)
>>> cache.ttl("foo")
25
>>> cache.ttl("not-exists")
0
```

you can also access the ttl of any sorted key in milliseconds, use the `pttl` method to do so:

```pycon
>>> from django.core.cache import cache
>>> cache.set("foo", "value", timeout=25)
>>> cache.pttl("foo")
25000
>>> cache.pttl("non-existent")
0
```

### Expire & Persist

in addition to the `ttl` and `pttl` methods, you can use the `persist` method so the key would have infinite timeout:

```pycon
>>> cache.set("foo", "bar", timeout=22)
>>> cache.ttl("foo")
22
>>> cache.persist("foo")
True
>>> cache.ttl("foo")
None
```

you can also use `expire` to set a new timeout on the key:

```pycon
>>> cache.set("foo", "bar", timeout=22)
>>> cache.expire("foo", timeout=5)
True
>>> cache.ttl("foo")
5
```    

The `pexpire` method can be used to set new timeout in millisecond precision:


```pycon
>>> cache.set("foo", "bar", timeout=22)
>>> cache.pexpire("foo", timeout=5505)
True
>>> cache.pttl("foo")
5505
```

The `expire_at` method can be used to make the key expire at a specific moment in time:

```pycon
>>> cache.set("foo", "bar", timeout=22)
>>> cache.expire_at("foo", datetime.now() + timedelta(hours=1))
True
>>> cache.ttl("foo")
3600
```

The `pexpire_at` method can be used to make the key expire at a specific moment in time, with milliseconds precision:

```pycon
>>> cache.set("foo", "bar", timeout=22)
>>> cache.pexpire_at("foo", datetime.now() + timedelta(milliseconds=900, hours=1))
True
>>> cache.ttl("foo")
3601
>>> cache.pttl("foo")
3600900
```

### Locks

django-valkey also supports locks.
valkey has distributed named locks which are identical to `threading.Lock` so you can useit as replacement.

```python
with cache.get_lock("somekey"):
    do_something())
```

this command is also available as `cache.lock()` but will be removed in the future.

### Access Raw client

if the commands provided by django-valkey backend is not enough, or you want to use them in a different way, you can access the underlying client as follows:

```pycon
>>> from django-valkey import get_valkey_connection
>>> con = get_valkey_connection("default")
>>> con
<valkey.client.Valkey object at 0x2dc4510>
```

**Warning**: not all clients support this feature:
ShardClient will raise an exception if tried to be used like this.
