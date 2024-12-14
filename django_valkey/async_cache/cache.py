from valkey.asyncio.client import Valkey as AValkey

from django_valkey.base import BaseValkeyCache
from django_valkey.cache import omit_exception, CONNECTION_INTERRUPTED
from django_valkey.async_cache.client.default import AsyncDefaultClient


class AsyncValkeyCache(BaseValkeyCache[AsyncDefaultClient, AValkey]):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"

    mset = BaseValkeyCache.amset

    keys = BaseValkeyCache.akeys

    iter_keys = BaseValkeyCache.aiter_keys

    ttl = BaseValkeyCache.attl

    pttl = BaseValkeyCache.apttl

    persist = BaseValkeyCache.apersist

    expire = BaseValkeyCache.aexpire

    expire_at = BaseValkeyCache.aexpire_at

    pexpire = BaseValkeyCache.apexpire

    pexpire_at = BaseValkeyCache.apexpire_at

    lock = alock = get_lock = BaseValkeyCache.aget_lock

    sadd = BaseValkeyCache.asadd

    scard = BaseValkeyCache.ascard

    sdiff = BaseValkeyCache.asdiff

    sdiffstore = BaseValkeyCache.asdiffstore

    sinter = BaseValkeyCache.asinter

    sinterstore = BaseValkeyCache.asinterstore

    sismember = BaseValkeyCache.asismember

    smembers = BaseValkeyCache.asmembers

    smove = BaseValkeyCache.asmove

    spop = BaseValkeyCache.aspop

    srandmember = BaseValkeyCache.asrandmember

    srem = BaseValkeyCache.asrem

    sscan = BaseValkeyCache.asscan

    sscan_iter = BaseValkeyCache.asscan_iter

    smismember = BaseValkeyCache.asmismember

    sunion = BaseValkeyCache.asunion

    sunionstore = BaseValkeyCache.asunionstore

    hset = BaseValkeyCache.ahset
    hsetnx = BaseValkeyCache.ahsetnx

    hdel = BaseValkeyCache.ahdel
    hdel_many = BaseValkeyCache.ahdel_many

    hget = BaseValkeyCache.ahget
    hgetall = BaseValkeyCache.ahgetall

    hlen = BaseValkeyCache.ahlen

    hkeys = BaseValkeyCache.ahkeys

    hexists = BaseValkeyCache.ahexists

    @omit_exception
    async def set(self, *args, **kwargs):
        return await self.client.aset(*args, **kwargs)

    aset = set

    @omit_exception
    async def incr_version(self, *args, **kwargs):
        return await self.client.aincr_version(*args, **kwargs)

    aincr_version = incr_version

    @omit_exception
    async def add(self, *args, **kwargs):
        return await self.client.aadd(*args, **kwargs)

    aadd = add

    async def get(self, key, default=None, version=None, client=None):
        value = await self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    aget = get

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    async def _get(self, key, default=None, version=None, client=None):
        return await self.client.aget(key, default, version, client)

    async def delete(self, *args, **kwargs):
        result = await self.client.adelete(*args, **kwargs)
        return bool(result)

    adelete = delete

    @omit_exception
    async def delete_many(self, *args, **kwargs):
        return await self.client.adelete_many(*args, **kwargs)

    adelete_many = delete_many

    @omit_exception
    async def clear(self):
        return await self.client.aclear()

    aclear = clear

    @omit_exception(return_value={})
    async def get_many(self, *args, **kwargs):
        return await self.client.aget_many(*args, **kwargs)

    aget_many = get_many

    @omit_exception
    async def set_many(self, *args, **kwargs):
        return await self.client.aset_many(*args, **kwargs)

    aset_many = set_many

    @omit_exception
    async def incr(self, *args, **kwargs):
        return await self.client.aincr(*args, **kwargs)

    aincr = incr

    @omit_exception
    async def decr(self, *args, **kwargs):
        return await self.client.adecr(*args, **kwargs)

    adecr = decr

    @omit_exception
    async def has_key(self, *args, **kwargs):
        return await self.client.ahas_key(*args, **kwargs)

    ahas_key = has_key

    @omit_exception
    async def aclose(self, *args, **kwargs):
        return await self.client.aclose()

    @omit_exception
    async def touch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    atouch = touch
