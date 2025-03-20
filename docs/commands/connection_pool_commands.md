# Access the connection pool

you can get the connection pool using this code:

```python
from django_valkey import get_valkey_connection

r = get_valkey_connection("default")  # use the name defined in ``CACHES`` settings
connection_pool = r.connection_pool
print(f"created connections so far: {connection_pool._created_connections}")
```

this will verify how many connections the pool has opened.
