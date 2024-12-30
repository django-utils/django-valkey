import builtins
import time
from collections.abc import Iterator, AsyncIterator, Callable
import contextlib
import functools
import logging
from asyncio import iscoroutinefunction
from typing import Any, TypeVar, Generic, TYPE_CHECKING

from django.conf import settings
from django.core.cache.backends.base import get_key_func, DEFAULT_TIMEOUT
from django.utils.module_loading import import_string

from django_valkey.exceptions import ConnectionInterrupted
from django_valkey.typing import KeyT

if TYPE_CHECKING:
    from valkey.lock import Lock
    from valkey.asyncio.lock import Lock as ALock

Client = TypeVar("Client")
Backend = TypeVar("Backend")

CONNECTION_INTERRUPTED = object()


def omit_exception(method: Callable | None = None, return_value: Any | None = None):
    """
    Simple decorator that intercepts connection
    errors and ignores these if settings specify this.
    """

    if method is None:
        return functools.partial(omit_exception, return_value=return_value)

    @functools.wraps(method)
    def _decorator(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self.logger.exception("Exception ignored")

                return return_value
            raise e.__cause__  # noqa: B904

    @functools.wraps(method)
    async def _async_decorator(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self.logger.exception("Exception ignored")
                return return_value
            raise e.__cause__

    return _async_decorator if iscoroutinefunction(method) else _decorator


class BaseValkeyCache(Generic[Client, Backend]):
    def __init__(self, server: str, params: dict[str, Any]) -> None:
        self._server = server
        self._params = params
        self._default_scan_itersize = getattr(
            settings, "DJANGO_VALKEY_SCAN_ITERSIZE", 10
        )

        options: dict = params.get("OPTIONS", {})
        self._client_cls = self.get_client_class()
        self._client = None

        self._ignore_exceptions = options.get(
            "IGNORE_EXCEPTIONS",
            getattr(settings, "DJANGO_VALKEY_IGNORE_EXCEPTIONS", False),
        )
        self._log_ignored_exceptions = options.get(
            "LOG_IGNORE_EXCEPTIONS",
            getattr(settings, "DJANGO_VALKEY_LOG_IGNORED_EXCEPTIONS", False),
        )
        self.logger = (
            logging.getLogger(getattr(settings, "DJANGO_VALKEY_LOGGER", __name__))
            if self._log_ignored_exceptions
            else None
        )

        timeout = params.get("timeout", params.get("TIMEOUT", 300))
        if timeout is not None:
            try:
                timeout = int(timeout)
            except (ValueError, TypeError):
                timeout = 300
        self.default_timeout = timeout

        options = params.get("OPTIONS", {})
        max_entries = params.get("max_entries", options.get("MAX_ENTRIES", 300))
        try:
            self._max_entries = int(max_entries)
        except (ValueError, TypeError):
            self._max_entries = 300

        cull_frequency = params.get("cull_frequency", options.get("CULL_FREQUENCY", 3))
        try:
            self._cull_frequency = int(cull_frequency)
        except (ValueError, TypeError):
            self._cull_frequency = 3

        self.key_prefix = params.get("KEY_PREFIX", "")
        self.version = params.get("VERSION", 1)
        self.key_func = get_key_func(params.get("KEY_FUNCTION"))

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        """
        Return the timeout value usable by this backend based upon the provided
        timeout.
        """
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        elif timeout == 0:
            # ticket 21147 - avoid time.time() related precision issues
            timeout = -1
        return None if timeout is None else time.time() + timeout

    def get_client_class(self) -> type[Client] | type:
        options = self._params.get("OPTIONS", {})
        _client_cls = options.get("CLIENT_CLASS", self.DEFAULT_CLIENT_CLASS)
        return import_string(_client_cls)

    @property
    def client(self) -> Client:
        """
        Lazy client connection property.
        """
        if self._client is None:
            self._client = self._client_cls(self._server, self._params, self)
        return self._client


class SyncCacheCommands:
    @omit_exception
    def set(self, *args, **kwargs) -> bool:
        return self.client.set(*args, **kwargs)

    @omit_exception
    def incr_version(self, *args, **kwargs) -> int:
        return self.client.incr_version(*args, **kwargs)

    @omit_exception
    def add(self, *args, **kwargs) -> bool:
        return self.client.add(*args, **kwargs)

    def get(self, key, default=None, version=None, client=None) -> Any:
        value = self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    def _get(self, key, default, version, client) -> Any:
        return self.client.get(key, default=default, version=version, client=client)

    @omit_exception
    def delete(self, *args, **kwargs) -> int:
        result = self.client.delete(*args, **kwargs)
        return bool(result)

    @omit_exception
    def delete_many(self, *args, **kwargs) -> int:
        return self.client.delete_many(*args, **kwargs)

    @omit_exception
    def delete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return self.client.delete_pattern(*args, **kwargs)

    @omit_exception
    def clear(self) -> bool:
        return self.client.clear()

    @omit_exception(return_value={})
    def get_many(self, *args, **kwargs) -> dict:
        return self.client.get_many(*args, **kwargs)

    @omit_exception
    def set_many(self, *args, **kwargs) -> None:
        return self.client.set_many(*args, **kwargs)

    @omit_exception
    def incr(self, *args, **kwargs) -> int:
        return self.client.incr(*args, **kwargs)

    @omit_exception
    def decr(self, *args, **kwargs) -> int:
        return self.client.decr(*args, **kwargs)

    @omit_exception
    def has_key(self, *args, **kwargs) -> bool:
        return self.client.has_key(*args, **kwargs)

    @omit_exception
    def keys(self, *args, **kwargs) -> list[Any]:
        return self.client.keys(*args, **kwargs)

    @omit_exception
    def iter_keys(self, *args, **kwargs) -> Iterator[Any]:
        return self.client.iter_keys(*args, **kwargs)

    @omit_exception
    def close(self) -> None:
        self.client.close()

    @omit_exception
    def touch(self, *args, **kwargs) -> bool:
        return self.client.touch(*args, **kwargs)

    @omit_exception
    def mset(self, *args, **kwargs) -> bool:
        return self.client.mset(*args, **kwargs)

    @omit_exception
    def mget(self, *args, **kwargs) -> dict | list[Any]:
        return self.client.mget(*args, **kwargs)

    @omit_exception
    def persist(self, *args, **kwargs) -> bool:
        return self.client.persist(*args, **kwargs)

    @omit_exception
    def expire(self, *args, **kwargs) -> bool:
        return self.client.expire(*args, **kwargs)

    @omit_exception
    def expire_at(self, *args, **kwargs) -> bool:
        return self.client.expire_at(*args, **kwargs)

    @omit_exception
    def pexpire(self, *args, **kwargs) -> bool:
        return self.client.pexpire(*args, **kwargs)

    @omit_exception
    def pexpire_at(self, *args, **kwargs) -> bool:
        return self.client.pexpire_at(*args, **kwargs)

    @omit_exception
    def get_lock(self, *args, **kwargs) -> "Lock":
        return self.client.get_lock(*args, **kwargs)

    lock = get_lock

    @omit_exception
    def ttl(self, *args, **kwargs) -> int:
        return self.client.ttl(*args, **kwargs)

    @omit_exception
    def pttl(self, *args, **kwargs) -> int:
        return self.client.pttl(*args, **kwargs)

    @omit_exception
    def scan(self, *args, **kwargs) -> tuple[int, list[Any]]:
        return self.client.scan(*args, **kwargs)

    @omit_exception
    def sadd(self, *args, **kwargs) -> int:
        return self.client.sadd(*args, **kwargs)

    @omit_exception
    def scard(self, *args, **kwargs) -> int:
        return self.client.scard(*args, **kwargs)

    @omit_exception
    def sdiff(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return self.client.sdiff(*args, **kwargs)

    @omit_exception
    def sdiffstore(self, *args, **kwargs) -> int:
        return self.client.sdiffstore(*args, **kwargs)

    @omit_exception
    def sinter(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return self.client.sinter(*args, **kwargs)

    @omit_exception
    def sintercard(self, *args, **kwargs) -> int:
        return self.client.sintercard(*args, **kwargs)

    @omit_exception
    def sinterstore(self, *args, **kwargs) -> int:
        return self.client.sinterstore(*args, **kwargs)

    @omit_exception
    def smismember(self, *args, **kwargs) -> list[bool]:
        return self.client.smismember(*args, **kwargs)

    @omit_exception
    def sismember(self, *args, **kwargs) -> bool:
        return self.client.sismember(*args, **kwargs)

    @omit_exception
    def smembers(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return self.client.smembers(*args, **kwargs)

    @omit_exception
    def smove(self, *args, **kwargs) -> bool:
        return self.client.smove(*args, **kwargs)

    @omit_exception
    def spop(self, *args, **kwargs) -> builtins.set | list | Any:
        return self.client.spop(*args, **kwargs)

    @omit_exception
    def srandmember(self, *args, **kwargs) -> builtins.set | list | Any:
        return self.client.srandmember(*args, **kwargs)

    @omit_exception
    def srem(self, *args, **kwargs) -> int:
        return self.client.srem(*args, **kwargs)

    @omit_exception
    def sscan(self, *args, **kwargs) -> tuple[int, builtins.set[Any] | list[Any]]:
        return self.client.sscan(*args, **kwargs)

    @omit_exception
    def sscan_iter(self, *args, **kwargs) -> Iterator[Any]:
        return self.client.sscan_iter(*args, **kwargs)

    @omit_exception
    def sunion(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return self.client.sunion(*args, **kwargs)

    @omit_exception
    def sunionstore(self, *args, **kwargs) -> int:
        return self.client.sunionstore(*args, **kwargs)

    @omit_exception
    def hset(self, *args, **kwargs) -> int:
        return self.client.hset(*args, **kwargs)

    @omit_exception
    def hsetnx(self, *args, **kwargs) -> int:
        return self.client.hsetnx(*args, **kwargs)

    @omit_exception
    def hdel(self, *args, **kwargs) -> int:
        return self.client.hdel(*args, **kwargs)

    @omit_exception
    def hdel_many(self, *args, **kwargs) -> int:
        return self.client.hdel_many(*args, **kwargs)

    @omit_exception
    def hget(self, *args, **kwargs) -> Any | None:
        return self.client.hget(*args, **kwargs)

    @omit_exception
    def hgetall(self, *args, **kwargs) -> dict[str, Any]:
        return self.client.hgetall(*args, **kwargs)

    @omit_exception
    def hmget(self, *args, **kwargs) -> dict:
        return self.client.hmget(*args, **kwargs)

    @omit_exception
    def hincrby(self, *args, **kwargs) -> int:
        return self.client.hincrby(*args, **kwargs)

    @omit_exception
    def hincrbyfloat(self, *args, **kwargs) -> float:
        return self.client.hincrbyfloat(*args, **kwargs)

    @omit_exception
    def hlen(self, *args, **kwargs) -> int:
        return self.client.hlen(*args, **kwargs)

    @omit_exception
    def hkeys(self, *args, **kwargs) -> list[Any]:
        return self.client.hkeys(*args, **kwargs)

    @omit_exception
    def hexists(self, *args, **kwargs) -> bool:
        return self.client.hexists(*args, **kwargs)

    @omit_exception
    def hvals(self, *args, **kwargs) -> list:
        return self.client.hvals(*args, **kwargs)

    @omit_exception
    def hstrlen(self, *args, **kwargs) -> int:
        return self.client.hstrlen(*args, **kwargs)

    @omit_exception
    def hrandfield(self, *args, **kwargs) -> str | list:
        return self.client.hrandfield(*args, **kwargs)


class AsyncCacheCommands:
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
    async def aclose(self):
        return await self.client.aclose()

    @omit_exception
    async def atouch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    touch = atouch

    @omit_exception
    async def amset(self, *args, **kwargs) -> bool:
        return await self.client.amset(*args, **kwargs)

    mset = amset

    @omit_exception
    async def amget(self, *args, **kwargs) -> dict | list[Any]:
        return await self.client.amget(*args, **kwargs)

    mget = amget

    @omit_exception
    async def apersist(self, *args, **kwargs) -> bool:
        return await self.client.apersist(*args, **kwargs)

    persist = apersist

    @omit_exception
    async def aexpire(self, *args, **kwargs) -> bool:
        return await self.client.aexpire(*args, **kwargs)

    expire = aexpire

    @omit_exception
    async def aexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.aexpire_at(*args, **kwargs)

    expire_at = aexpire_at

    @omit_exception
    async def apexpire(self, *args, **kwargs) -> bool:
        return await self.client.apexpire(*args, **kwargs)

    pexpire = apexpire

    @omit_exception
    async def apexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.apexpire_at(*args, **kwargs)

    pexpire_at = apexpire_at

    @omit_exception
    async def aget_lock(self, *args, **kwargs) -> "ALock":
        return await self.client.aget_lock(*args, **kwargs)

    lock = alock = get_lock = aget_lock

    @omit_exception
    async def adelete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return await self.client.adelete_pattern(*args, **kwargs)

    delete_pattern = adelete_pattern

    @omit_exception
    async def attl(self, *args, **kwargs) -> int:
        return await self.client.attl(*args, **kwargs)

    ttl = attl

    @omit_exception
    async def apttl(self, *args, **kwargs) -> int:
        return await self.client.apttl(*args, **kwargs)

    pttl = apttl

    @omit_exception
    async def aiter_keys(self, *args, **kwargs) -> AsyncIterator[KeyT]:
        async with contextlib.aclosing(self.client.aiter_keys(*args, **kwargs)) as it:
            async for key in it:
                yield key

    iter_keys = aiter_keys

    @omit_exception
    async def akeys(self, *args, **kwargs) -> list:
        return await self.client.akeys(*args, **kwargs)

    keys = akeys

    @omit_exception
    async def ascan(self, *args, **kwargs) -> tuple[int, list[Any]]:
        return await self.client.ascan(*args, **kwargs)

    scan = ascan

    @omit_exception
    async def asadd(self, *args, **kwargs) -> int:
        return await self.client.asadd(*args, **kwargs)

    sadd = asadd

    @omit_exception
    async def ascard(self, *args, **kwargs) -> int:
        return await self.client.ascard(*args, **kwargs)

    scard = ascard

    @omit_exception
    async def asdiff(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return await self.client.asdiff(*args, **kwargs)

    sdiff = asdiff

    @omit_exception
    async def asdiffstore(self, *args, **kwargs) -> int:
        return await self.client.asdiffstore(*args, **kwargs)

    sdiffstore = asdiffstore

    @omit_exception
    async def asinter(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return await self.client.asinter(*args, **kwargs)

    sinter = asinter

    @omit_exception
    async def asintercard(self, *args, **kwargs) -> int:
        return await self.client.asintercard(*args, **kwargs)

    sintercard = asintercard

    @omit_exception
    async def asinterstore(self, *args, **kwargs) -> int:
        return await self.client.asinterstore(*args, **kwargs)

    sinterstore = asinterstore

    @omit_exception
    async def asmismember(self, *args, **kwargs) -> list[bool]:
        return await self.client.asmismember(*args, **kwargs)

    smismember = asmismember

    @omit_exception
    async def asismember(self, *args, **kwargs) -> bool:
        return await self.client.asismember(*args, **kwargs)

    sismember = asismember

    @omit_exception
    async def asmembers(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return await self.client.asmembers(*args, **kwargs)

    smembers = asmembers

    @omit_exception
    async def asmove(self, *args, **kwargs) -> bool:
        return await self.client.asmove(*args, **kwargs)

    smove = asmove

    @omit_exception
    async def aspop(self, *args, **kwargs) -> builtins.set | list | Any:
        return await self.client.aspop(*args, **kwargs)

    spop = aspop

    @omit_exception
    async def asrandmember(self, *args, **kwargs) -> builtins.set | list | Any:
        return await self.client.asrandmember(*args, **kwargs)

    srandmember = asrandmember

    @omit_exception
    async def asrem(self, *args, **kwargs) -> int:
        return await self.client.asrem(*args, **kwargs)

    srem = asrem

    @omit_exception
    async def asscan(
        self, *args, **kwargs
    ) -> tuple[int, builtins.set[Any] | list[Any]]:
        return await self.client.asscan(*args, **kwargs)

    sscan = asscan

    @omit_exception
    async def asscan_iter(self, *args, **kwargs) -> AsyncIterator[Any]:
        async with contextlib.aclosing(self.client.asscan_iter(*args, **kwargs)) as it:
            async for key in it:
                yield key

    sscan_iter = asscan_iter

    @omit_exception
    async def asunion(self, *args, **kwargs) -> builtins.set[Any] | list[Any]:
        return await self.client.asunion(*args, **kwargs)

    sunion = asunion

    @omit_exception
    async def asunionstore(self, *args, **kwargs) -> int:
        return await self.client.asunionstore(*args, **kwargs)

    sunionstore = asunionstore

    @omit_exception
    async def ahset(self, *args, **kwargs) -> int:
        return await self.client.hset(*args, **kwargs)

    hset = ahset

    @omit_exception
    async def ahsetnx(self, *args, **kwargs) -> int:
        return await self.client.hsetnx(*args, **kwargs)

    hsetnx = ahsetnx

    @omit_exception
    async def ahdel(self, *args, **kwargs) -> int:
        return await self.client.hdel(*args, **kwargs)

    hdel = ahdel

    @omit_exception
    async def ahdel_many(self, *args, **kwargs) -> int:
        return await self.client.ahdel_many(*args, **kwargs)

    hdel_many = ahdel_many

    @omit_exception
    async def ahget(self, *args, **kwargs) -> Any | None:
        return await self.client.ahget(*args, **kwargs)

    hget = ahget

    @omit_exception
    async def ahgetall(self, *args, **kwargs) -> dict[str, Any]:
        return await self.client.ahgetall(*args, **kwargs)

    hgetall = ahgetall

    @omit_exception
    async def ahmget(self, *args, **kwargs) -> dict:
        return await self.client.ahmget(*args, **kwargs)

    hmget = ahmget

    @omit_exception
    async def ahincrby(self, *args, **kwargs) -> int:
        return await self.client.ahincrby(*args, **kwargs)

    hincrby = ahincrby

    @omit_exception
    async def ahincrbyfloat(self, *args, **kwargs) -> float:
        return await self.client.ahincrbyfloat(*args, **kwargs)

    hincrbyfloat = ahincrbyfloat

    @omit_exception
    async def ahlen(self, *args, **kwargs) -> int:
        return await self.client.hlen(*args, **kwargs)

    hlen = ahlen

    @omit_exception
    async def ahkeys(self, *args, **kwargs) -> list[Any]:
        return await self.client.ahkeys(*args, **kwargs)

    hkeys = ahkeys

    @omit_exception
    async def ahexists(self, *args, **kwargs) -> bool:
        return await self.client.ahexists(*args, **kwargs)

    hexists = ahexists

    @omit_exception
    async def ahvals(self, *args, **kwargs) -> list:
        return await self.client.ahvals(*args, **kwargs)

    hvals = ahvals

    @omit_exception
    async def ahstrlen(self, *args, **kwargs) -> int:
        return await self.client.ahstrlen(*args, **kwargs)

    hstrlen = ahstrlen

    @omit_exception
    async def ahrandfield(self, *args, **kwargs) -> str | list:
        return await self.client.ahrandfield(*args, **kwargs)

    hrandfield = ahrandfield
