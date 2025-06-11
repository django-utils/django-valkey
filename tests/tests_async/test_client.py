from collections.abc import Iterable
from unittest.mock import AsyncMock

import pytest
from pytest_django.fixtures import SettingsWrapper
from pytest_mock import MockerFixture

from django.core.cache import DEFAULT_CACHE_ALIAS

from django_valkey.async_cache.cache import AsyncValkeyCache
from django_valkey.async_cache.client import AsyncDefaultClient

pytestmark = pytest.mark.anyio


@pytest.fixture
async def cache_client(cache: AsyncValkeyCache) -> Iterable[AsyncDefaultClient]:
    client = cache.client
    await client.aset("TestClientClose", 0)
    yield client
    await client.adelete("TestClientClose")


class TestClientClose:
    async def test_close_client_disconnect_default(
        self, cache_client: AsyncDefaultClient, mocker: MockerFixture
    ):
        mock = mocker.patch.object(
            cache_client.connection_factory, "disconnect", new_callable=AsyncMock
        )
        await cache_client.aclose()
        assert not mock.called

    async def test_close_disconnect_settings(
        self,
        cache_client: AsyncDefaultClient,
        settings: SettingsWrapper,
        mocker: MockerFixture,
    ):
        settings.DJANGO_VALKEY_CLOSE_CONNECTION = True
        mock = mocker.patch.object(
            cache_client.connection_factory, "disconnect", new_callable=AsyncMock
        )
        await cache_client.aclose()
        assert mock.called

    async def test_close_disconnect_settings_cache(
        self,
        cache_client: AsyncDefaultClient,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        settings.CACHES[DEFAULT_CACHE_ALIAS]["OPTIONS"]["CLOSE_CONNECTION"] = True
        await cache_client.aset("TestClientClose", 0)
        mock = mocker.patch.object(
            cache_client.connection_factory, "disconnect", new_callable=AsyncMock
        )
        await cache_client.aclose()
        assert mock.called

    async def test_close_disconnect_client_options(
        self, cache_client: AsyncDefaultClient, mocker: MockerFixture
    ):
        cache_client._options["CLOSE_CONNECTION"] = True
        mock = mocker.patch.object(
            cache_client.connection_factory, "disconnect", new_callable=AsyncMock
        )
        await cache_client.aclose()
        assert mock.called
