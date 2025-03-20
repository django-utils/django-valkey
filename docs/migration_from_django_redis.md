# Migration from django-redis

If you have django-redis setup and want to migrate to using django-valkey these are the steps you might need to take:


## Install django-valkey

As explained in [Installation guide](installation.md) you can easily install django-valkey.
this project can easily live alongside django-redis so you don't need to delete that if you don't want to.


## Different configuration

The `REDIS_CLIENT_CLASS` has been renamed to `BASE_CLIENT_CLASS`.  
The `REDIS_CLIENT_KWARGS` has been renamed to `BASE_CLIENT_KWARGS`.  
so if you have any one these two configured in your project you should change that.  


other than the above change the rest of the API is consistent,
but any of the configurations that have any form of `redis` in it has been changed to valkey.
you can easily fix this by running this commands:

```shell
sed -i 's/REDIS/VALKEY/' settings.py
sed -i 's/redis/valkey/' settings.py
sed -i 's/Redis/Valkey/' settings.py
```

where settings.py is the file you have your configs in, change the file name if you are using a different name.


## Different commands

in django-redis, `get_many()` is an atomic operation, but `set_many()` is non-atomic.

in django-valkey `mget()` and `mset()` are atomic, and `get_many()` and `set_many()` are non-atomic.


## More options

Although the above steps are completely enough to get you going, if you want you can now easily customize your compression behaviour.
Check out [Compressor support](configure/compressors.md) for complete explanation.
