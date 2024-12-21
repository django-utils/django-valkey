import contextlib
import functools
import logging
from asyncio import iscoroutinefunction
from typing import Any, TypeVar, Generic, Iterator, AsyncGenerator, Set, Callable

from django.conf import settings
from django.core.cache.backends.base import BaseCache
from django.utils.module_loading import import_string

from django_valkey.exceptions import ConnectionInterrupted

Client = TypeVar("Client")
Backend = TypeVar("Backend")


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


class BaseValkeyCache(BaseCache, Generic[Client, Backend]):
    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
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

    @omit_exception
    def mset(self, *args, **kwargs):
        return self.client.mset(*args, **kwargs)

    @omit_exception
    async def amset(self, *args, **kwargs):
        return await self.client.amset(*args, **kwargs)

    @omit_exception
    def mget(self, *args, **kwargs):
        return self.client.mget(*args, **kwargs)

    @omit_exception
    async def amget(self, *args, **kwargs):
        return await self.client.amget(*args, **kwargs)

    @omit_exception
    def persist(self, *args, **kwargs) -> bool:
        return self.client.persist(*args, **kwargs)

    @omit_exception
    async def apersist(self, *args, **kwargs) -> bool:
        return await self.client.apersist(*args, **kwargs)

    @omit_exception
    def expire(self, *args, **kwargs) -> bool:
        return self.client.expire(*args, **kwargs)

    @omit_exception
    async def aexpire(self, *args, **kwargs) -> bool:
        return await self.client.aexpire(*args, **kwargs)

    @omit_exception
    def expire_at(self, *args, **kwargs) -> bool:
        return self.client.expire_at(*args, **kwargs)

    @omit_exception
    async def aexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.aexpire_at(*args, **kwargs)

    @omit_exception
    def pexpire(self, *args, **kwargs) -> bool:
        return self.client.pexpire(*args, **kwargs)

    @omit_exception
    async def apexpire(self, *args, **kwargs) -> bool:
        return await self.client.apexpire(*args, **kwargs)

    @omit_exception
    def pexpire_at(self, *args, **kwargs) -> bool:
        return self.client.pexpire_at(*args, **kwargs)

    @omit_exception
    async def apexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.apexpire_at(*args, **kwargs)

    @omit_exception
    def get_lock(self, *args, **kwargs):
        return self.client.get_lock(*args, **kwargs)

    lock = get_lock

    @omit_exception
    async def aget_lock(self, *args, **kwargs):
        return await self.client.aget_lock(*args, **kwargs)

    alock = aget_lock

    @omit_exception
    def delete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return self.client.delete_pattern(*args, **kwargs)

    @omit_exception
    async def adelete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return await self.client.adelete_pattern(*args, **kwargs)

    @omit_exception
    def ttl(self, *args, **kwargs) -> int | None:
        return self.client.ttl(*args, **kwargs)

    @omit_exception
    async def attl(self, *args, **kwargs) -> int:
        return await self.client.attl(*args, **kwargs)

    @omit_exception
    def pttl(self, *args, **kwargs) -> int:
        return self.client.pttl(*args, **kwargs)

    @omit_exception
    async def apttl(self, *args, **kwargs) -> int:
        return await self.client.apttl(*args, **kwargs)

    @omit_exception
    def iter_keys(self, *args, **kwargs) -> Iterator:
        return self.client.iter_keys(*args, **kwargs)

    @omit_exception
    async def aiter_keys(self, *args, **kwargs) -> AsyncGenerator:
        async with contextlib.aclosing(self.client.aiter_keys(*args, **kwargs)) as it:
            async for key in it:
                yield key

    @omit_exception
    def keys(self, *args, **kwargs) -> list[Any]:
        return self.client.keys(*args, **kwargs)

    @omit_exception
    async def akeys(self, *args, **kwargs) -> list[Any]:
        return await self.client.akeys(*args, **kwargs)

    @omit_exception
    def scan(self, *args, **kwargs) -> tuple[int, list[str]]:
        return self.client.scan(*args, **kwargs)

    @omit_exception
    async def ascan(self, *args, **kwargs) -> tuple[int, list[str]]:
        return await self.client.ascan(*args, **kwargs)

    @omit_exception
    def sadd(self, *args, **kwargs) -> int:
        return self.client.sadd(*args, **kwargs)

    @omit_exception
    async def asadd(self, *args, **kwargs) -> int:
        return await self.client.asadd(*args, **kwargs)

    @omit_exception
    def scard(self, *args, **kwargs) -> int:
        return self.client.scard(*args, **kwargs)

    @omit_exception
    async def ascard(self, *args, **kwargs) -> int:
        return await self.client.ascard(*args, **kwargs)

    @omit_exception
    def sdiff(self, *args, **kwargs) -> Set[Any]:
        return self.client.sdiff(*args, **kwargs)

    @omit_exception
    async def asdiff(self, *args, **kwargs) -> Set[Any]:
        return await self.client.asdiff(*args, **kwargs)

    @omit_exception
    def sdiffstore(self, *args, **kwargs) -> int:
        return self.client.sdiffstore(*args, **kwargs)

    @omit_exception
    async def asdiffstore(self, *args, **kwargs) -> int:
        return await self.client.asdiffstore(*args, **kwargs)

    @omit_exception
    def sinter(self, *args, **kwargs) -> Set[Any]:
        return self.client.sinter(*args, **kwargs)

    @omit_exception
    async def asinter(self, *args, **kwargs) -> Set[Any]:
        return await self.client.asinter(*args, **kwargs)

    @omit_exception
    def sintercard(self, *args, **kwargs) -> int:
        return self.client.sintercard(*args, **kwargs)

    @omit_exception
    async def asintercard(self, *args, **kwargs) -> int:
        return await self.client.asintercard(*args, **kwargs)

    @omit_exception
    def sinterstore(self, *args, **kwargs) -> int:
        return self.client.sinterstore(*args, **kwargs)

    @omit_exception
    async def asinterstore(self, *args, **kwargs) -> int:
        return await self.client.asinterstore(*args, **kwargs)

    @omit_exception
    def smismember(self, *args, **kwargs) -> list[bool]:
        return self.client.smismember(*args, **kwargs)

    @omit_exception
    async def asmismember(self, *args, **kwargs) -> list[bool]:
        return await self.client.asmismember(*args, **kwargs)

    @omit_exception
    def sismember(self, *args, **kwargs) -> bool:
        return self.client.sismember(*args, **kwargs)

    @omit_exception
    async def asismember(self, *args, **kwargs) -> bool:
        return await self.client.asismember(*args, **kwargs)

    @omit_exception
    def smembers(self, *args, **kwargs) -> Set[Any]:
        return self.client.smembers(*args, **kwargs)

    @omit_exception
    async def asmembers(self, *args, **kwargs) -> Set[Any]:
        return await self.client.asmembers(*args, **kwargs)

    @omit_exception
    def smove(self, *args, **kwargs) -> bool:
        return self.client.smove(*args, **kwargs)

    @omit_exception
    async def asmove(self, *args, **kwargs) -> bool:
        return await self.client.asmove(*args, **kwargs)

    @omit_exception
    def spop(self, *args, **kwargs) -> Set | Any:
        return self.client.spop(*args, **kwargs)

    @omit_exception
    async def aspop(self, *args, **kwargs) -> Set | Any:
        return await self.client.aspop(*args, **kwargs)

    @omit_exception
    def srandmember(self, *args, **kwargs) -> list | Any:
        return self.client.srandmember(*args, **kwargs)

    @omit_exception
    async def asrandmember(self, *args, **kwargs) -> list | Any:
        return await self.client.asrandmember(*args, **kwargs)

    @omit_exception
    def srem(self, *args, **kwargs) -> int:
        return self.client.srem(*args, **kwargs)

    @omit_exception
    async def asrem(self, *args, **kwargs) -> int:
        return await self.client.asrem(*args, **kwargs)

    @omit_exception
    def sscan(self, *args, **kwargs) -> Set[Any]:
        return self.client.sscan(*args, **kwargs)

    @omit_exception
    async def asscan(self, *args, **kwargs) -> Set[Any]:
        return await self.client.asscan(*args, **kwargs)

    @omit_exception
    def sscan_iter(self, *args, **kwargs) -> Iterator:
        return self.client.sscan_iter(*args, **kwargs)

    @omit_exception
    async def asscan_iter(self, *args, **kwargs) -> AsyncGenerator:
        async with contextlib.aclosing(self.client.asscan_iter(*args, **kwargs)) as it:
            async for key in it:
                yield key

    @omit_exception
    def sunion(self, *args, **kwargs) -> Set[Any]:
        return self.client.sunion(*args, **kwargs)

    @omit_exception
    async def asunion(self, *args, **kwargs) -> Set[Any]:
        return await self.client.asunion(*args, **kwargs)

    @omit_exception
    def sunionstore(self, *args, **kwargs) -> int:
        return self.client.sunionstore(*args, **kwargs)

    @omit_exception
    async def asunionstore(self, *args, **kwargs) -> int:
        return await self.client.asunionstore(*args, **kwargs)

    @omit_exception
    def hset(self, *args, **kwargs) -> int:
        return self.client.hset(*args, **kwargs)

    @omit_exception
    async def ahset(self, *args, **kwargs) -> int:
        return await self.client.hset(*args, **kwargs)

    @omit_exception
    def hsetnx(self, *args, **kwargs) -> int:
        return self.client.hsetnx(*args, **kwargs)

    @omit_exception
    async def ahsetnx(self, *args, **kwargs) -> int:
        return await self.client.hsetnx(*args, **kwargs)

    @omit_exception
    def hdel(self, *args, **kwargs) -> int:
        return self.client.hdel(*args, **kwargs)

    @omit_exception
    async def ahdel(self, *args, **kwargs) -> int:
        return await self.client.hdel(*args, **kwargs)

    @omit_exception
    def hdel_many(self, *args, **kwargs) -> int:
        return self.client.hdel_many(*args, **kwargs)

    @omit_exception
    async def ahdel_many(self, *args, **kwargs) -> int:
        return await self.client.ahdel_many(*args, **kwargs)

    @omit_exception
    def hget(self, *args, **kwargs) -> bytes | None:
        return self.client.hget(*args, **kwargs)

    @omit_exception
    async def ahget(self, *args, **kwargs) -> str | None:
        return await self.client.ahget(*args, **kwargs)

    @omit_exception
    def hgetall(self, *args, **kwargs) -> dict:
        return self.client.hgetall(*args, **kwargs)

    @omit_exception
    async def ahgetall(self, *args, **kwargs) -> dict:
        return await self.client.ahgetall(*args, **kwargs)

    @omit_exception
    def hmget(self, *args, **kwargs) -> dict:
        return self.client.hmget(*args, **kwargs)

    @omit_exception
    async def ahmget(self, *args, **kwargs) -> dict:
        return await self.client.ahmget(*args, **kwargs)

    @omit_exception
    def hincrby(self, *args, **kwargs) -> int:
        return self.client.hincrby(*args, **kwargs)

    @omit_exception
    async def ahincrby(self, *args, **kwargs) -> int:
        return await self.client.ahincrby(*args, **kwargs)

    @omit_exception
    def hincrbyfloat(self, *args, **kwargs) -> float:
        return self.client.hincrbyfloat(*args, **kwargs)

    @omit_exception
    async def ahincrbyfloat(self, *args, **kwargs) -> float:
        return await self.client.ahincrbyfloat(*args, **kwargs)

    @omit_exception
    def hlen(self, *args, **kwargs) -> int:
        return self.client.hlen(*args, **kwargs)

    @omit_exception
    async def ahlen(self, *args, **kwargs) -> int:
        return await self.client.hlen(*args, **kwargs)

    @omit_exception
    def hkeys(self, *args, **kwargs) -> list[Any]:
        return self.client.hkeys(*args, **kwargs)

    @omit_exception
    async def ahkeys(self, *args, **kwargs) -> list[Any]:
        return await self.client.ahkeys(*args, **kwargs)

    @omit_exception
    def hexists(self, *args, **kwargs) -> bool:
        return self.client.hexists(*args, **kwargs)

    @omit_exception
    async def ahexists(self, *args, **kwargs) -> bool:
        return await self.client.ahexists(*args, **kwargs)

    @omit_exception
    def hvals(self, *args, **kwargs) -> list:
        return self.client.hvals(*args, **kwargs)

    @omit_exception
    async def ahvals(self, *args, **kwargs) -> list:
        return await self.client.ahvals(*args, **kwargs)

    @omit_exception
    def hstrlen(self, *args, **kwargs) -> int:
        return self.client.hstrlen(*args, **kwargs)

    @omit_exception
    async def ahstrlen(self, *args, **kwargs) -> int:
        return await self.client.ahstrlen(*args, **kwargs)

    @omit_exception
    def hrandfield(self, *args, **kwargs) -> str | list:
        return self.client.hrandfield(*args, **kwargs)

    @omit_exception
    async def ahrandfield(self, *args, **kwargs) -> str | list:
        return await self.client.ahrandfield(*args, **kwargs)
