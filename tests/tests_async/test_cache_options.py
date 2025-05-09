import contextlib
import copy
from collections.abc import Iterable
from typing import cast

import pytest
from pytest import LogCaptureFixture
import pytest_asyncio
from pytest_django.fixtures import SettingsWrapper

from django.core.cache import caches

from valkey.exceptions import ConnectionError

from django_valkey.async_cache.cache import AsyncValkeyCache


@pytest_asyncio.fixture
async def ignore_exceptions_cache(settings: SettingsWrapper) -> AsyncValkeyCache:
    caches_settings = copy.deepcopy(settings.CACHES)
    caches_settings["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
    caches_settings["doesnotexist"]["OPTIONS"]["LOG_IGNORE_EXCEPTIONS"] = True
    settings.CACHES = caches_settings
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
    settings.DJANGO_VALKEY_LOG_IGNORE_EXCEPTIONS = True
    return cast(AsyncValkeyCache, caches["doesnotexist"])


@pytest.mark.asyncio(loop_scope="session")
async def test_get_django_omit_exceptions_many_returns_default_arg(
    ignore_exceptions_cache: AsyncValkeyCache,
):
    assert ignore_exceptions_cache._ignore_exceptions is True
    assert await ignore_exceptions_cache.aget_many(["key1", "key2", "key3"]) == {}


@pytest.mark.asyncio(loop_scope="session")
async def test_get_django_omit_exceptions(
    caplog: LogCaptureFixture, ignore_exceptions_cache: AsyncValkeyCache
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


@pytest.mark.asyncio(loop_scope="session")
async def test_get_django_omit_exceptions_priority_1(settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
    settings.CACHES = caches_setting
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = False
    cache = cast(AsyncValkeyCache, caches["doesnotexist"])
    assert cache._ignore_exceptions is True
    assert await cache.aget("key") is None


@pytest.mark.asyncio(loop_scope="session")
async def test_get_django_omit_exceptions_priority_2(settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = False
    settings.CACHES = caches_setting
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
    cache = cast(AsyncValkeyCache, caches["doesnotexist"])
    assert cache._ignore_exceptions is False
    with pytest.raises(ConnectionError):
        await cache.aget("key")


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
