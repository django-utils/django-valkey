from collections.abc import Iterable
from unittest.mock import Mock, call, patch

import pytest

from django_valkey.cluster_cache.cache import ClusterValkeyCache
from django_valkey.cluster_cache.client import DefaultClusterClient


@pytest.fixture
def cache_client(cache: ClusterValkeyCache) -> Iterable[DefaultClusterClient]:
    client = cache.client
    client.set("TestClientClose", 0)
    yield client
    client.delete("TestClientClose")


class TestDefaultClusterClient:
    @patch("django_valkey.base_client.ClientCommands.get_client")
    @patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
    def test_delete_pattern_calls_get_client_given_no_client(
        self, init_mock, get_client_mock
    ):
        client = DefaultClusterClient()
        client._backend = Mock()
        client._backend.key_prefix = ""

        client.delete_pattern(pattern="{foo}*")
        get_client_mock.assert_called_once_with(write=True, tried=None)

    @patch("django_valkey.base_client.BaseClient.make_pattern")
    @patch("django_valkey.base_client.ClientCommands.get_client", return_value=Mock())
    @patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
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

    @patch("django_valkey.base_client.BaseClient.make_pattern")
    @patch("django_valkey.base_client.ClientCommands.get_client", return_value=Mock())
    @patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
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

    @patch("django_valkey.base_client.BaseClient.make_pattern")
    @patch("django_valkey.base_client.ClientCommands.get_client", return_value=Mock())
    @patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
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
