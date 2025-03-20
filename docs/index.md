# django-valkey

[valkey](https://valkey.io/) is an open source BSD Licensed high-performance key/value database that supports a variety of workloads such as *caching*, message queues, and can act as a primary database.

django-valkey is a customizable valkey backend to be used in django.
this project was initially a fork of the wonderful `django-redis` project.


## django-valkey Features

1. Uses native valkey-py url notation connection strings
2. Pluggable clients:
    1. Default Client
    2. Herd Client
    3. Sentinel Client
    4. Sharded Client
    5. Async client
    6. or just plug in your own client
3. Pluggable serializers:
    1. Pickle Serializer
    2. Json Serializer
    3. msgpack serializer
    4. or plug in your own serializer
4. Pluggable compression:
    1. brotli compression
    2. bz2 compression (bzip2)
    3. gzip compression
    4. lz4 compression
    5. lzma compression
    6. zlib compression
    7. zstd compression
    8. plug in your own
5. Pluggable parsers
    1. Valkey's default parser
    2. plug in your own
6. Pluggable connection pool
    1. Valkey's default connection pool
    2. plug in your own
7. Comprehensive test suite
8. Supports infinite timeouts
9. Facilities for raw access to Valkey client/connection pool
10. Highly configurable (really, just look around)
11. Unix sockets supported by default

## Requirements

[Python](https://www.python.org/downloads/)  3.10+

[Django](https://www.djangoproject.com/download/)  4.2.20+

[valkey-py](https://pypi.org/project/valkey/) 6.0.1+

[Valkey](https://valkey.io/download/)  7.2.6+