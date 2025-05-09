from collections.abc import Iterable
from unittest.mock import patch

import pytest
from django.core.cache import caches
from django.test import override_settings
from pytest_django.fixtures import SettingsWrapper
from valkey.cluster import ValkeyCluster

from django_valkey.cluster_cache.cache import ClusterValkeyCache


@pytest.fixture
def patch_itersize_setting() -> Iterable[None]:
    # destroy cache to force recreation with overriden settings
    del caches["default"]
    with override_settings(DJANGO_VALKEY_SCAN_ITERSIZE=30):
        yield
    # destroy cache to force recreation with original settings
    del caches["default"]


class TestDjangoValkeyCache:
    def test_mget(self, cache: ClusterValkeyCache):
        cache.set("{foo}a", 1)
        cache.set("{foo}b", 2)
        cache.set("{foo}c", 3)

        res = cache.mget(["{foo}a", "{foo}b", "{foo}c"])
        assert res == {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}

    def test_mget_unicode(self, cache: ClusterValkeyCache):
        cache.set("{foo}a", "1")
        cache.set("{foo}ب", "2")
        cache.set("{foo}c", "الف")

        res = cache.mget(["{foo}a", "{foo}ب", "{foo}c"])
        assert res == {"{foo}a": "1", "{foo}ب": "2", "{foo}c": "الف"}

    def test_mset(self, cache: ClusterValkeyCache):
        cache.mset({"a{foo}": 1, "b{foo}": 2, "c{foo}": 3})
        res = cache.mget(["a{foo}", "b{foo}", "c{foo}"])
        assert res == {"a{foo}": 1, "b{foo}": 2, "c{foo}": 3}

    def test_delete_pattern(self, cache: ClusterValkeyCache):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*", target_nodes=ValkeyCluster.ALL_NODES)
        assert set(keys) == {"foo-bb", "foo-bc"}

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is False

    @patch("django_valkey.cluster_cache.cache.ClusterValkeyCache.client")
    def test_delete_pattern_with_custom_count(
        self, client_mock, cache: ClusterValkeyCache
    ):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        cache.delete_pattern("*foo-a*", itersize=2)

        client_mock.delete_pattern.assert_called_once_with("*foo-a*", itersize=2)

    @patch("django_valkey.cluster_cache.cache.ClusterValkeyCache.client")
    def test_delete_pattern_with_settings_default_scan_count(
        self,
        client_mock,
        patch_itersize_setting,
        cache: ClusterValkeyCache,
        settings: SettingsWrapper,
    ):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")
        expected_count = settings.DJANGO_VALKEY_SCAN_ITERSIZE

        cache.delete_pattern("*foo-a*")

        client_mock.delete_pattern.assert_called_once_with(
            "*foo-a*", itersize=expected_count
        )

    def test_sdiff(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiff("{foo}1", "{foo}2") == {"bar1"}

    def test_sdiffstore(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2") == 1
        assert cache.smembers("{foo}3") == {"bar1"}

    def test_sdiffstore_with_keys_version(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2", version=2)
        cache.sadd("{foo}2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 1
        assert cache.smembers("{foo}3") == {"bar1"}

    def test_sdiffstore_with_different_keys_versions_without_initial_set_in_version(
        self, cache: ClusterValkeyCache
    ):
        cache.sadd("{foo}1", "bar1", "bar2", version=1)
        cache.sadd("{foo}2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 0

    def test_sdiffstore_with_different_keys_versions_with_initial_set_in_version(
        self, cache: ClusterValkeyCache
    ):
        cache.sadd("{foo}1", "bar1", "bar2", version=2)
        cache.sadd("{foo}2", "bar2", "bar3", version=1)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 2

    def test_sinter(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinter("{foo}1", "{foo}2") == {"bar2"}

    def test_sinterstore(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinterstore("{foo}3", "{foo}1", "{foo}2") == 1
        assert cache.smembers("{foo}3") == {"bar2"}

    def test_smove(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.smove("{foo}1", "{foo}2", "bar1") is True
        assert cache.smove("{foo}1", "{foo}2", "bar4") is False
        assert cache.smembers("{foo}1") == {"bar2"}
        assert cache.smembers("{foo}2") == {"bar1", "bar2", "bar3"}

    def test_sunion(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunion("{foo}1", "{foo}2") == {"bar1", "bar2", "bar3"}

    def test_sunionstore(self, cache: ClusterValkeyCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunionstore("{foo}3", "{foo}1", "{foo}2") == 3
        assert cache.smembers("{foo}3") == {"bar1", "bar2", "bar3"}
