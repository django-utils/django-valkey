from valkey.asyncio.client import Valkey as AValkey

from django_valkey.base import BaseValkeyCache
from django_valkey.cache import omit_exception, CONNECTION_INTERRUPTED
from django_valkey.async_cache.client.default import AsyncDefaultClient


class AsyncValkeyCache(BaseValkeyCache[AsyncDefaultClient, AValkey]):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"

    mset = BaseValkeyCache.amset

    keys = BaseValkeyCache.akeys

    iter_keys = BaseValkeyCache.aiter_keys

    scan = BaseValkeyCache.ascan

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

    sintercard = BaseValkeyCache.asintercard

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
    hmget = BaseValkeyCache.ahmget

    hincrby = BaseValkeyCache.ahincrby
    hincrbyfloat = BaseValkeyCache.ahincrbyfloat

    hlen = BaseValkeyCache.ahlen

    hkeys = BaseValkeyCache.ahkeys

    hexists = BaseValkeyCache.ahexists

    hvals = BaseValkeyCache.ahvals

    hstrlen = BaseValkeyCache.ahstrlen

    hrandfield = BaseValkeyCache.ahrandfield

    @omit_exception
    async def aset(self, *args, **kwargs):
        return await self.client.aset(*args, **kwargs)

    set = aset

    @omit_exception
    async def aincr_version(self, *args, **kwargs):
        return await self.client.aincr_version(*args, **kwargs)

    incr_version = aincr_version

    @omit_exception
    async def aadd(self, *args, **kwargs):
        return await self.client.aadd(*args, **kwargs)

    add = aadd

    async def aget(self, key, default=None, version=None, client=None):
        value = await self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    get = aget

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    async def _get(self, key, default=None, version=None, client=None):
        return await self.client.aget(key, default, version, client)

    async def adelete(self, *args, **kwargs):
        result = await self.client.adelete(*args, **kwargs)
        return bool(result)

    delete = adelete

    @omit_exception
    async def adelete_many(self, *args, **kwargs):
        return await self.client.adelete_many(*args, **kwargs)

    delete_many = adelete_many

    @omit_exception
    async def aclear(self):
        return await self.client.aclear()

    clear = aclear

    @omit_exception(return_value={})
    async def aget_many(self, *args, **kwargs):
        return await self.client.aget_many(*args, **kwargs)

    get_many = aget_many

    @omit_exception
    async def aset_many(self, *args, **kwargs):
        return await self.client.aset_many(*args, **kwargs)

    set_many = aset_many

    @omit_exception
    async def aincr(self, *args, **kwargs):
        return await self.client.aincr(*args, **kwargs)

    incr = aincr

    @omit_exception
    async def adecr(self, *args, **kwargs):
        return await self.client.adecr(*args, **kwargs)

    decr = adecr

    @omit_exception
    async def ahas_key(self, *args, **kwargs):
        return await self.client.ahas_key(*args, **kwargs)

    has_key = ahas_key

    @omit_exception
    async def aclose(self, *args, **kwargs):
        return await self.client.aclose()

    @omit_exception
    async def atouch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    touch = atouch
