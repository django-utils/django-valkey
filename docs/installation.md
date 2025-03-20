# Installation guide


## Basic Installation:

```shell
pip install django-valkey
```

## Install with C-bindings for maximum performance:

```shell
pip install django-valkey[libvalkey]
```

## Install with 3rd-party serializers


### Install with msgpack serializer:

```shell
pip install django-valkey[msgpack]
```

## Install with 3rd party compression libraries:

### lz4 library:

```shell
pip install django-valkey[lz4]
``` 

### pyzstd library:

```shell
pip install django-valkey[pyzstd]
```   

### brotli library:

```shell
pip install django-valkey[brotli]
```   

## Coming from django-redis?

check out our migration guide [Migration from django-redis](migration_from_django_redis.md)