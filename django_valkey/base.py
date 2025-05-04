import builtins
import contextlib
import functools
import inspect
import logging
from asyncio import iscoroutinefunction
from collections.abc import AsyncGenerator, Callable, Iterator
from typing import (
    Any,
    TypeVar,
    Generic,
    TYPE_CHECKING,
)

from django.conf import settings
from django.core.cache.backends.base import get_key_func
from django.utils.module_loading import import_string

from django_valkey.exceptions import ConnectionInterrupted

if TYPE_CHECKING:
    from valkey.lock import Lock
    from valkey.asyncio.lock import Lock as ALock

Client = TypeVar("Client")
Backend = TypeVar("Backend")

CONNECTION_INTERRUPTED = object()
DEFAULT_TIMEOUT = object()
ATTR_DOES_NOT_EXIST = object()


def decorate_all_methods(decorator):
    def decorate(cls):
        for attr in vars(cls):
            # dunders and `get` should not be decorated
            # get is handled by `_get`
            if attr.startswith("__") or attr in {"get", "get_or_set"}:
                continue

            attribute = getattr(cls, attr)
            if callable(attribute):
                if attr == "_get":
                    setattr(
                        cls,
                        attr,
                        decorator(attribute, return_value=CONNECTION_INTERRUPTED),
                    )
                elif attr == "get_many":
                    setattr(cls, attr, decorator(attribute, return_value={}))
                elif attr in {"iter_keys", "sscan_iter"}:
                    setattr(cls, attr, decorator(attribute, gen=True))
                else:
                    setattr(cls, attr, decorator(attribute))
        return cls

    return decorate


def omit_exception(
    method: Callable | None = None, return_value: Any | None = None, gen: bool = False
):
    """
    Simple decorator that intercepts connection
    errors and ignores these if settings specify this.
    """

    if method is None:
        return functools.partial(omit_exception, return_value=return_value)

    def __handle_error(self, e) -> Any | None:
        if getattr(self, "_ignore_exceptions", None):
            if getattr(self, "_log_ignored_exceptions", None):
                self.logger.exception("Exception ignored")

            return return_value
        raise e.__cause__

    @functools.wraps(method)
    def _decorator(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            return __handle_error(self, e)

    @functools.wraps(method)
    def _generator_decorator(self, *args, **kwargs):
        try:
            for item in method(self, *args, **kwargs):
                yield item
        except ConnectionInterrupted as e:
            yield __handle_error(self, e)

    @functools.wraps(method)
    async def _async_decorator(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            return __handle_error(self, e)

    @functools.wraps(method)
    async def _async_generator_decorator(self, *args, **kwargs):
        try:
            async for item in method(self, *args, **kwargs):
                yield item
        except ConnectionInterrupted as e:
            yield __handle_error(self, e)

    # sync generators (iter_keys, sscan_iter) are only generator on client class
    # in the backend they are just a function (that returns a generator)
    # so inspect.isgeneratorfunction does not work
    if not inspect.isasyncgenfunction(method) and not gen:
        wrapper = _async_decorator if iscoroutinefunction(method) else _decorator

    # if method is a generator or async generator, it should be iterated over by this decorator
    # generators don't error by simply being called, they need to be iterated over.
    else:
        wrapper = (
            _async_generator_decorator
            if inspect.isasyncgenfunction(method)
            else _generator_decorator
        )

    wrapper.original = method
    return wrapper


class BaseValkeyCache(Generic[Client, Backend]):
    _missing_key = object()

    def __init__(self, server: str, params: dict[str, Any]) -> None:
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


@decorate_all_methods(omit_exception)
class BackendCommands:
    def __contains__(self, item):
        return self.has_key(item)

    def set(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.set(*args, **kwargs)

    def incr_version(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.incr_version(*args, **kwargs)

    def add(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.add(*args, **kwargs)

    def get(self, key, default=None, version=None, client=None) -> Any:
        value = self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    def _get(self: BaseValkeyCache, key, default, version, client) -> Any:
        return self.client.get(key, default=default, version=version, client=client)

    def get_or_set(self, key, default, timeout=DEFAULT_TIMEOUT, version=None) -> Any:
        """
        Fetch a given key from the cache. If the key does not exist,
        add the key and set it to the default value. The default value can
        also be any callable. If timeout is given, use that timeout for the
        key; otherwise use the default cache timeout.

        Return the value of the key stored or retrieved.
        """
        val = self.get(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            self.add(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first get() and the add() above.
            return self.get(key, default, version=version)
        return val

    def delete(self: BaseValkeyCache, *args, **kwargs) -> int:
        result = self.client.delete(*args, **kwargs)
        return bool(result)

    def delete_many(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.delete_many(*args, **kwargs)

    def delete_pattern(self: BaseValkeyCache, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return self.client.delete_pattern(*args, **kwargs)

    def clear(self: BaseValkeyCache) -> bool:
        return self.client.clear()

    def get_many(self: BaseValkeyCache, *args, **kwargs) -> dict:
        return self.client.get_many(*args, **kwargs)

    def set_many(self: BaseValkeyCache, *args, **kwargs) -> None:
        return self.client.set_many(*args, **kwargs)

    def incr(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.incr(*args, **kwargs)

    def decr(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.decr(*args, **kwargs)

    def has_key(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.has_key(*args, **kwargs)

    def keys(self: BaseValkeyCache, *args, **kwargs) -> list[Any]:
        return self.client.keys(*args, **kwargs)

    def iter_keys(self: BaseValkeyCache, *args, **kwargs) -> Iterator[Any]:
        return self.client.iter_keys(*args, **kwargs)

    def close(self: BaseValkeyCache) -> None:
        self.client.close()

    def touch(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.touch(*args, **kwargs)

    def mset(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.mset(*args, **kwargs)

    def mget(self: BaseValkeyCache, *args, **kwargs) -> dict | list[Any]:
        return self.client.mget(*args, **kwargs)

    def persist(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.persist(*args, **kwargs)

    def expire(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.expire(*args, **kwargs)

    def expire_at(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.expire_at(*args, **kwargs)

    def pexpire(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.pexpire(*args, **kwargs)

    def pexpire_at(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.pexpire_at(*args, **kwargs)

    def get_lock(self: BaseValkeyCache, *args, **kwargs) -> "Lock":
        return self.client.get_lock(*args, **kwargs)

    lock = get_lock

    def ttl(self: BaseValkeyCache, *args, **kwargs) -> int | None:
        return self.client.ttl(*args, **kwargs)

    def pttl(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.pttl(*args, **kwargs)

    def sadd(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sadd(*args, **kwargs)

    def scard(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.scard(*args, **kwargs)

    def sdiff(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sdiff(*args, **kwargs)

    def sdiffstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sdiffstore(*args, **kwargs)

    def sinter(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sinter(*args, **kwargs)

    def sinterstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sinterstore(*args, **kwargs)

    def smismember(self: BaseValkeyCache, *args, **kwargs) -> list[bool]:
        return self.client.smismember(*args, **kwargs)

    def sismember(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.sismember(*args, **kwargs)

    def smembers(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.smembers(*args, **kwargs)

    def smove(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.smove(*args, **kwargs)

    def spop(self: BaseValkeyCache, *args, **kwargs) -> builtins.set | Any:
        return self.client.spop(*args, **kwargs)

    def srandmember(self: BaseValkeyCache, *args, **kwargs) -> list | Any:
        return self.client.srandmember(*args, **kwargs)

    def srem(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.srem(*args, **kwargs)

    def sscan(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sscan(*args, **kwargs)

    def sscan_iter(self: BaseValkeyCache, *args, **kwargs) -> Iterator:
        return self.client.sscan_iter(*args, **kwargs)

    def sunion(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sunion(*args, **kwargs)

    def sunionstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sunionstore(*args, **kwargs)

    def hset(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hset(*args, **kwargs)

    def hdel(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hdel(*args, **kwargs)

    def hlen(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hlen(*args, **kwargs)

    def hkeys(self: BaseValkeyCache, *args, **kwargs) -> list[Any]:
        return self.client.hkeys(*args, **kwargs)

    def hexists(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.hexists(*args, **kwargs)


@decorate_all_methods(omit_exception)
class AsyncBackendCommands:
    def __getattr__(self, item):
        if item.startswith("a"):
            attr = getattr(self, item[1:], ATTR_DOES_NOT_EXIST)
            if attr is not ATTR_DOES_NOT_EXIST:
                return attr
        raise AttributeError(
            f"{self.__class__.__name__} object has no attribute {item}"
        )

    async def set(self: BaseValkeyCache, *args, **kwargs):
        return await self.client.aset(*args, **kwargs)

    async def incr_version(self, *args, **kwargs):
        return await self.client.aincr_version(*args, **kwargs)

    async def add(self, *args, **kwargs):
        return await self.client.aadd(*args, **kwargs)

    async def get(self, key, default=None, version=None, client=None):
        value = await self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    async def _get(self, key, default=None, version=None, client=None):
        return await self.client.aget(key, default, version, client)

    async def get_or_set(self, key, default, timeout=DEFAULT_TIMEOUT, version=None):
        """See get_or_set()."""
        val = await self.aget(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            await self.aadd(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first aget() and the aadd() above.
            return await self.aget(key, default, version=version)
        return val

    async def delete(self, *args, **kwargs):
        result = await self.client.adelete(*args, **kwargs)
        return bool(result)

    async def delete_many(self, *args, **kwargs):
        return await self.client.adelete_many(*args, **kwargs)

    async def clear(self):
        return await self.client.aclear()

    async def get_many(self, *args, **kwargs):
        return await self.client.aget_many(*args, **kwargs)

    async def set_many(self, *args, **kwargs):
        return await self.client.aset_many(*args, **kwargs)

    async def incr(self, *args, **kwargs):
        return await self.client.aincr(*args, **kwargs)

    async def decr(self, *args, **kwargs):
        return await self.client.adecr(*args, **kwargs)

    async def has_key(self, *args, **kwargs):
        return await self.client.ahas_key(*args, **kwargs)

    async def close(self, *args, **kwargs):
        return await self.client.close()

    async def touch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    async def mset(self, *args, **kwargs):
        return await self.client.mset(*args, **kwargs)

    async def mget(self, *args, **kwargs):
        return await self.client.mget(*args, **kwargs)

    async def persist(self, *args, **kwargs) -> bool:
        return await self.client.persist(*args, **kwargs)

    async def expire(self, *args, **kwargs) -> bool:
        return await self.client.expire(*args, **kwargs)

    async def expire_at(self, *args, **kwargs) -> bool:
        return await self.client.expire_at(*args, **kwargs)

    async def pexpire(self, *args, **kwargs) -> bool:
        return await self.client.pexpire(*args, **kwargs)

    async def pexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.pexpire_at(*args, **kwargs)

    async def get_lock(self, *args, **kwargs) -> "ALock":
        return await self.client.get_lock(*args, **kwargs)

    lock = get_lock

    async def delete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return await self.client.adelete_pattern(*args, **kwargs)

    async def ttl(self, *args, **kwargs) -> int:
        return await self.client.ttl(*args, **kwargs)

    async def pttl(self, *args, **kwargs) -> int:
        return await self.client.pttl(*args, **kwargs)

    async def iter_keys(self, *args, **kwargs) -> AsyncGenerator[Any, None]:
        async with contextlib.aclosing(self.client.iter_keys(*args, **kwargs)) as it:
            async for key in it:
                yield key

    async def keys(self, *args, **kwargs) -> list[Any]:
        return await self.client.keys(*args, **kwargs)

    async def sadd(self, *args, **kwargs) -> int:
        return await self.client.sadd(*args, **kwargs)

    async def scard(self, *args, **kwargs) -> int:
        return await self.client.scard(*args, **kwargs)

    async def sdiff(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.sdiff(*args, **kwargs)

    async def sdiffstore(self, *args, **kwargs) -> int:
        return await self.client.sdiffstore(*args, **kwargs)

    async def sinter(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.sinter(*args, **kwargs)

    async def sinterstore(self, *args, **kwargs) -> int:
        return await self.client.sinterstore(*args, **kwargs)

    async def smismember(self, *args, **kwargs) -> list[bool]:
        return await self.client.smismember(*args, **kwargs)

    async def sismember(self, *args, **kwargs) -> bool:
        return await self.client.sismember(*args, **kwargs)

    async def smembers(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.smembers(*args, **kwargs)

    async def smove(self, *args, **kwargs) -> bool:
        return await self.client.smove(*args, **kwargs)

    async def spop(self, *args, **kwargs) -> builtins.set | Any:
        return await self.client.spop(*args, **kwargs)

    async def srandmember(self, *args, **kwargs) -> list | Any:
        return await self.client.srandmember(*args, **kwargs)

    async def srem(self, *args, **kwargs) -> int:
        return await self.client.srem(*args, **kwargs)

    async def sscan(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.sscan(*args, **kwargs)

    async def sscan_iter(self, *args, **kwargs) -> AsyncGenerator[Any, None]:
        async with contextlib.aclosing(self.client.sscan_iter(*args, **kwargs)) as it:
            async for key in it:
                yield key

    async def sunion(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.sunion(*args, **kwargs)

    async def sunionstore(self, *args, **kwargs) -> int:
        return await self.client.sunionstore(*args, **kwargs)

    async def hset(self, *args, **kwargs) -> int:
        return await self.client.hset(*args, **kwargs)

    async def hdel(self, *args, **kwargs) -> int:
        return await self.client.hdel(*args, **kwargs)

    async def hlen(self, *args, **kwargs) -> int:
        return await self.client.hlen(*args, **kwargs)

    async def hkeys(self, *args, **kwargs) -> list[Any]:
        return await self.client.hkeys(*args, **kwargs)

    async def hexists(self, *args, **kwargs) -> bool:
        return await self.client.hexists(*args, **kwargs)


# temp fix for django's #36047
# TODO: remove this when it's fixed in django
from django.core import signals  # noqa: E402
from django.core.cache import caches, close_caches  # noqa: E402


async def close_async_caches(**kwargs):
    for conn in caches.all(initialized_only=True):
        if getattr(conn, "is_async", False):
            await conn.aclose()
        else:
            conn.close()


signals.request_finished.connect(close_async_caches)
signals.request_finished.disconnect(close_caches)
