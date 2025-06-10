import contextlib
import copy
from collections.abc import Iterable
from typing import cast

import pytest
import pytest_asyncio
from pytest import LogCaptureFixture
from pytest_django.fixtures import SettingsWrapper

from django.core.cache import caches, cache as default_cache

from valkey.exceptions import ConnectionError

from django_valkey.async_cache.cache import AsyncValkeyCache
from django_valkey.async_cache.client import AsyncHerdClient, AsyncDefaultClient

methods_with_no_parameters = {"clear", "close"}

methods_with_one_required_parameters = {
    "decr",
    "delete",
    "delete_many",
    "delete_pattern",
    "get",
    "get_many",
    "has_key",
    "hkeys",
    "hlen",
    "incr",
    "incr_version",
    "keys",
    "lock",
    "mget",
    "pttl",
    "persist",
    "scard",
    "sdiff",
    "sinter",
    "smembers",
    "spop",
    "srandmember",
    "sscan",
    "sunion",
    "touch",
    "ttl",
}
methods_with_two_required_parameters = {
    "add",
    "expire",
    "expire_at",
    "hdel",
    "hexists",
    "pexpire",
    "pexpire_at",
    "sadd",
    "sdiffstore",
    "set",
    "sinterstore",
    "sismember",
    "smismember",
    "srem",
    "sunionstore",
}
methods_with_three_required_parameters = {"hset", "smove"}
methods_taking_dictionary = {"mset", "set_many"}
iter_methods = {
    "iter_keys",
    "sscan_iter",
}

no_herd_method = {
    "incr",
    "decr",
}


@pytest.mark.asyncio(loop_scope="session")
class TestDjangoValkeyOmitException:
    @pytest_asyncio.fixture
    async def conf_cache(self, settings: SettingsWrapper):
        caches_settings = copy.deepcopy(settings.CACHES)
        # NOTE: this files raises RuntimeWarning because `conn.close` was not awaited,
        # this is expected because django calls the signal manually during this test
        # to debug, put a `raise` in django.utils.connection.BaseConnectionHandler.close_all
        settings.CACHES = caches_settings
        return caches_settings

    @pytest_asyncio.fixture
    async def conf_cache_to_ignore_exception(
        self, settings: SettingsWrapper, conf_cache
    ):
        conf_cache["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
        conf_cache["doesnotexist"]["OPTIONS"]["LOG_IGNORE_EXCEPTIONS"] = True
        settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
        settings.DJANGO_VALKEY_LOG_IGNORE_EXCEPTIONS = True

    @pytest_asyncio.fixture
    async def ignore_exceptions_cache(
        self, conf_cache_to_ignore_exception
    ) -> AsyncValkeyCache:
        return cast(AsyncValkeyCache, caches["doesnotexist"])

    async def test_methods_with_no_argument_omit_exception(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in methods_with_no_parameters:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                await method()

    async def test_methods_with_one_argument_omit_exception(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in methods_with_one_required_parameters:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                if (
                    isinstance(default_cache.client, AsyncHerdClient)
                    and m in no_herd_method
                ):
                    pytest.skip(f"herd client doesn't support {m}")
                await method("abc")

    async def test_methods_with_two_argument_omit_exception(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in methods_with_two_required_parameters:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                await method("abc", 1)

    @pytest.mark.skipif(
        not isinstance(default_cache.client, AsyncDefaultClient),
        reason="not supported by none default clients",
    )
    async def test_methods_with_three_argument_omit_exception(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in methods_with_three_required_parameters:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                await method("abc", "def", "ghi")

    async def test_methods_taking_dictionary(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in methods_taking_dictionary:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                await method({"abc": "def"})

    async def test_iterator_methods(
        self, ignore_exceptions_cache: AsyncValkeyCache, subtests
    ):
        for m in iter_methods:
            method = getattr(ignore_exceptions_cache, m)
            with subtests.test(method=method):
                async for i in method("abc"):
                    assert i is None

    async def test_get_django_omit_exceptions_many_returns_default_arg(
        self,
        ignore_exceptions_cache: AsyncValkeyCache,
    ):
        assert ignore_exceptions_cache._ignore_exceptions is True
        assert await ignore_exceptions_cache.aget_many(["key1", "key2", "key3"]) == {}

    async def test_get_django_omit_exceptions(
        self, caplog: LogCaptureFixture, ignore_exceptions_cache: AsyncValkeyCache
    ):
        assert ignore_exceptions_cache._ignore_exceptions is True
        assert ignore_exceptions_cache._log_ignored_exceptions is True

        assert await ignore_exceptions_cache.aget("key") is None
        assert await ignore_exceptions_cache.aget("key", "default") == "default"
        assert await ignore_exceptions_cache.aget("key", default="default") == "default"

        assert len(caplog.records) == 3
        assert all(
            record.levelname == "ERROR" and record.msg == "Exception ignored"
            for record in caplog.records
        )

    async def test_get_django_omit_exceptions_priority_1(
        self, settings: SettingsWrapper
    ):
        caches_setting = copy.deepcopy(settings.CACHES)
        caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
        settings.CACHES = caches_setting
        settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = False
        cache = cast(AsyncValkeyCache, caches["doesnotexist"])
        assert cache._ignore_exceptions is True
        assert await cache.aget("key") is None

    async def test_get_django_omit_exceptions_priority_2(
        self, settings: SettingsWrapper
    ):
        caches_setting = copy.deepcopy(settings.CACHES)
        caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = False
        settings.CACHES = caches_setting
        settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
        cache = cast(AsyncValkeyCache, caches["doesnotexist"])
        assert cache._ignore_exceptions is False
        with pytest.raises(ConnectionError):
            await cache.aget("key")

    async def test_error_raised_when_ignore_is_not_set(self, conf_cache):
        cache = caches["doesnotexist"]
        assert cache._ignore_exceptions is False
        assert cache._log_ignored_exceptions is False
        with pytest.raises(ConnectionError):
            await cache.get("key")


@pytest_asyncio.fixture
async def key_prefix_cache(
    cache: AsyncValkeyCache, settings: SettingsWrapper
) -> Iterable[AsyncValkeyCache]:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_PREFIX"] = "*"
    settings.CACHES = caches_setting
    yield cache


@pytest_asyncio.fixture
async def with_prefix_cache() -> Iterable[AsyncValkeyCache]:
    with_prefix = cast(AsyncValkeyCache, caches["with_prefix"])
    yield with_prefix
    await with_prefix.clear()


@pytest.mark.asyncio(loop_scope="session")
class TestDjangoValkeyCacheEscapePrefix:
    async def test_delete_pattern(
        self, key_prefix_cache: AsyncValkeyCache, with_prefix_cache: AsyncValkeyCache
    ):
        await key_prefix_cache.aset("a", "1")
        await with_prefix_cache.aset("b", "2")
        await key_prefix_cache.adelete_pattern("*")
        assert await key_prefix_cache.ahas_key("a") is False
        assert await with_prefix_cache.aget("b") == "2"

    async def test_iter_keys(
        self, key_prefix_cache: AsyncValkeyCache, with_prefix_cache: AsyncValkeyCache
    ):
        await key_prefix_cache.aset("a", "1")
        await with_prefix_cache.aset("b", "2")
        async with contextlib.aclosing(key_prefix_cache.aiter_keys("*")) as keys:
            test_list = [k async for k in keys]

        assert test_list == ["a"]

    async def test_keys(
        self, key_prefix_cache: AsyncValkeyCache, with_prefix_cache: AsyncValkeyCache
    ):
        await key_prefix_cache.aset("a", "1")
        await with_prefix_cache.aset("b", "2")
        keys = await key_prefix_cache.akeys("*")
        assert "a" in keys
        assert "b" not in keys


@pytest.mark.asyncio(loop_scope="session")
async def test_custom_key_function(cache: AsyncValkeyCache, settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_FUNCTION"] = "tests.test_cache_options.make_key"
    caches_setting["default"][
        "REVERSE_KEY_FUNCTION"
    ] = "tests.test_cache_options.reverse_key"
    settings.CACHES = caches_setting

    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        await cache.aset(key, "foo")

    res = await cache.adelete_pattern("*foo-a*")
    assert bool(res) is True

    keys = await cache.akeys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}
    # ensure our custom function was actually called
    client = await cache.client.get_client(write=False)
    assert {k.decode() for k in await client.keys("*")} == ({"#1#foo-bc", "#1#foo-bb"})
