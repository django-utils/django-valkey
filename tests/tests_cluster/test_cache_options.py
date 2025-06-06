import copy

from pytest_django.fixtures import SettingsWrapper

from valkey import ValkeyCluster

from django_valkey.cluster_cache.cache import ClusterValkeyCache


class TestDjangoValkeyCacheEscapePrefix:
    def test_keys(
        self,
        key_prefix_cache: ClusterValkeyCache,  # noqa: F811
        with_prefix_cache: ClusterValkeyCache,  # noqa: F811
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        keys = key_prefix_cache.keys("*", target_nodes=ValkeyCluster.ALL_NODES)
        assert "a" in keys
        assert "b" not in keys


def test_custom_key_function(cache: ClusterValkeyCache, settings: SettingsWrapper):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_FUNCTION"] = "tests.test_cache_options.make_key"
    caches_setting["default"][
        "REVERSE_KEY_FUNCTION"
    ] = "tests.test_cache_options.reverse_key"
    settings.CACHES = caches_setting

    for key in ["{foo}-aa", "{foo}-ab", "{foo}-bb", "{foo}-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*{foo}-a*")
    assert bool(res) is True

    keys = cache.keys("{foo}*", target_nodes=ValkeyCluster.ALL_NODES)
    assert set(keys) == {"{foo}-bb", "{foo}-bc"}
    # ensure our custom function was actually called
    assert {
        k.decode()
        for k in cache.client.get_client(write=False).keys(
            "*", target_nodes=ValkeyCluster.ALL_NODES
        )
    } == ({"#1#{foo}-bc", "#1#{foo}-bb"})
