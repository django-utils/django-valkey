from collections.abc import Iterable
from unittest.mock import Mock, call, patch

import pytest
from django.core.cache import DEFAULT_CACHE_ALIAS
from pytest_django.fixtures import SettingsWrapper
from pytest_mock import MockerFixture

from django_valkey.cluster_cache.cache import ClusterValkeyCache
from django_valkey.cluster_cache.client import DefaultClusterClient


@pytest.fixture
def cache_client(cache: ClusterValkeyCache) -> Iterable[DefaultClusterClient]:
    client = cache.client
    client.set("TestClientClose", 0)
    yield client
    client.delete("TestClientClose")


class TestClientClose:
    def test_close_client_disconnect_default(
        self, cache_client: DefaultClusterClient, mocker: MockerFixture
    ):
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")
        cache_client.close()
        assert not mock.called

    def test_close_disconnect_settings(
        self,
        cache_client: DefaultClusterClient,
        settings: SettingsWrapper,
        mocker: MockerFixture,
    ):
        settings.DJANGO_VALKEY_CLOSE_CONNECTION = True
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")
        cache_client.close()
        assert mock.called

    def test_close_disconnect_settings_cache(
        self,
        cache_client: DefaultClusterClient,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        settings.CACHES[DEFAULT_CACHE_ALIAS]["OPTIONS"]["CLOSE_CONNECTION"] = True
        cache_client.set("TestClientClose", 0)
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")
        cache_client.close()
        assert mock.called

    def test_close_disconnect_client_options(
        self, cache_client: DefaultClusterClient, mocker: MockerFixture
    ):
        cache_client._options["CLOSE_CONNECTION"] = True
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")
        cache_client.close()
        assert mock.called


class TestDefaultClusterClient:
    @patch("tests.tests_cluster.test_client.DefaultClusterClient.get_client")
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.__init__",
        return_value=None,
    )
    def test_delete_pattern_calls_get_client_given_no_client(
        self, init_mock, get_client_mock
    ):
        client = DefaultClusterClient()
        client._backend = Mock()
        client._backend.key_prefix = ""

        client.delete_pattern(pattern="{foo}*")
        get_client_mock.assert_called_once_with(write=True, tried=None)

    @patch("tests.tests_cluster.test_client.DefaultClusterClient.make_pattern")
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.get_client",
        return_value=Mock(),
    )
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.__init__",
        return_value=None,
    )
    def test_delete_pattern_calls_make_pattern(
        self, init_mock, get_client_mock, make_pattern_mock
    ):
        client = DefaultClusterClient()
        client._backend = Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="{foo}*")

        kwargs = {"version": None, "prefix": None}
        make_pattern_mock.assert_called_once_with("{foo}*", **kwargs)

    @patch("tests.tests_cluster.test_client.DefaultClusterClient.make_pattern")
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.get_client",
        return_value=Mock(),
    )
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.__init__",
        return_value=None,
    )
    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(
        self, init_mock, get_client_mock, make_pattern_mock
    ):
        client = DefaultClusterClient()
        client._backend = Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="{foo}*", itersize=90210)

        get_client_mock.return_value.scan_iter.assert_called_once_with(
            count=90210, match=make_pattern_mock.return_value
        )

    @patch("tests.tests_cluster.test_client.DefaultClusterClient.make_pattern")
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.get_client",
        return_value=Mock(),
    )
    @patch(
        "tests.tests_cluster.test_client.DefaultClusterClient.__init__",
        return_value=None,
    )
    def test_delete_pattern_calls_pipeline_delete_and_execute(
        self, init_mock, get_client_mock, make_pattern_mock
    ):
        client = DefaultClusterClient()
        client._backend = Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = [":1:{foo}", ":1:{foo}-a"]
        get_client_mock.return_value.pipeline.return_value = Mock()
        get_client_mock.return_value.pipeline.return_value.delete = Mock()
        get_client_mock.return_value.pipeline.return_value.execute = Mock()

        client.delete_pattern(pattern="{foo}*")

        assert get_client_mock.return_value.pipeline.return_value.delete.call_count == 2
        get_client_mock.return_value.pipeline.return_value.delete.assert_has_calls(
            [call(":1:{foo}"), call(":1:{foo}-a")]
        )
        get_client_mock.return_value.pipeline.return_value.execute.assert_called_once()
