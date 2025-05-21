import copy
from typing import cast

import pytest
from pytest import LogCaptureFixture
from pytest_django.fixtures import SettingsWrapper

from django.core.cache import caches, cache as default_cache

from valkey.exceptions import ConnectionError

from django_valkey.cache import ValkeyCache
from django_valkey.client import ShardClient
from django_valkey.cluster_cache.client import DefaultClusterClient


def make_key(key: str, prefix: str, version: str) -> str:
    return f"{prefix}#{version}#{key}"


def reverse_key(key: str) -> str:
    return key.split("#", 2)[2]


@pytest.fixture
def ignore_exceptions_cache(settings: SettingsWrapper) -> ValkeyCache:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
    caches_setting["doesnotexist"]["OPTIONS"]["LOG_IGNORED_EXCEPTIONS"] = True
    settings.CACHES = caches_setting
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
    settings.DJANGO_VALKEY_LOG_IGNORED_EXCEPTIONS = True
    return cast(ValkeyCache, caches["doesnotexist"])


@pytest.mark.skipif(
    isinstance(default_cache.client, DefaultClusterClient),
    reason="cluster client doesn't support ignore exception",
)
def test_get_django_omit_exceptions_many_returns_default_arg(
    ignore_exceptions_cache: ValkeyCache,
):
    assert ignore_exceptions_cache._ignore_exceptions is True
    assert ignore_exceptions_cache.get_many(["key1", "key2", "key3"]) == {}


@pytest.mark.skipif(
    isinstance(default_cache.client, DefaultClusterClient),
    reason="cluster client doesn't support ignore exception",
)
def test_get_django_omit_exceptions(
    caplog: LogCaptureFixture, ignore_exceptions_cache: ValkeyCache
):
    assert ignore_exceptions_cache._ignore_exceptions is True
    assert ignore_exceptions_cache._log_ignored_exceptions is True

    assert ignore_exceptions_cache.get("key") is None
    assert ignore_exceptions_cache.get("key", "default") == "default"
    assert ignore_exceptions_cache.get("key", default="default") == "default"

    assert len(caplog.records) == 3
    assert all(
        record.levelname == "ERROR" and record.msg == "Exception ignored"
        for record in caplog.records
    )


@pytest.mark.skipif(
    isinstance(default_cache.client, DefaultClusterClient),
    reason="cluster client doesn't support ignore exception",
)
def test_get_django_omit_exceptions_priority_1(settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = True
    settings.CACHES = caches_setting
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = False
    cache = cast(ValkeyCache, caches["doesnotexist"])
    assert cache._ignore_exceptions is True
    assert cache.get("key") is None


@pytest.mark.skipif(
    isinstance(default_cache.client, DefaultClusterClient),
    reason="cluster client doesn't support ignore exception",
)
def test_get_django_omit_exceptions_priority_2(settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"]["OPTIONS"]["IGNORE_EXCEPTIONS"] = False
    settings.CACHES = caches_setting
    settings.DJANGO_VALKEY_IGNORE_EXCEPTIONS = True
    cache = cast(ValkeyCache, caches["doesnotexist"])
    assert cache._ignore_exceptions is False
    with pytest.raises(ConnectionError):
        cache.get("key")


class TestDjangoValkeyCacheEscapePrefix:
    def test_delete_pattern(
        self, key_prefix_cache: ValkeyCache, with_prefix_cache: ValkeyCache
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        key_prefix_cache.delete_pattern("*")
        assert key_prefix_cache.has_key("a") is False
        assert with_prefix_cache.get("b") == "2"

    def test_iter_keys(
        self, key_prefix_cache: ValkeyCache, with_prefix_cache: ValkeyCache
    ):
        if isinstance(key_prefix_cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support iter_keys")

        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        assert list(key_prefix_cache.iter_keys("*")) == ["a"]

    @pytest.mark.skipif(
        isinstance(default_cache.client, DefaultClusterClient),
        reason="cluster client doesn't support ignore exception",
    )
    def test_keys(self, key_prefix_cache: ValkeyCache, with_prefix_cache: ValkeyCache):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        keys = key_prefix_cache.keys("*")
        assert "a" in keys
        assert "b" not in keys


@pytest.mark.skipif(
    isinstance(default_cache.client, DefaultClusterClient),
    reason="cluster client doesn't support ignore exception",
)
def test_custom_key_function(cache: ValkeyCache, settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_FUNCTION"] = "tests.test_cache_options.make_key"
    caches_setting["default"][
        "REVERSE_KEY_FUNCTION"
    ] = "tests.test_cache_options.reverse_key"
    settings.CACHES = caches_setting

    if isinstance(cache.client, ShardClient):
        pytest.skip("ShardClient doesn't support get_client")

    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*foo-a*")
    assert bool(res) is True

    keys = cache.keys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}
    # ensure our custom function was actually called
    assert {k.decode() for k in cache.client.get_client(write=False).keys("*")} == (
        {"#1#foo-bc", "#1#foo-bb"}
    )
