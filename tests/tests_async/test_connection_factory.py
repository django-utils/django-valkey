import pytest

from django.core.exceptions import ImproperlyConfigured

from django_valkey import pool as sync_pool
from django_valkey.async_cache import pool


pytestmark = pytest.mark.anyio


async def test_connection_factory_redefine_from_opts():
    cf = sync_pool.get_connection_factory(
        options={
            "CONNECTION_FACTORY": "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory",
            "SENTINELS": [("127.0.0.1", "26379")],
        },
    )
    assert cf.__class__.__name__ == "AsyncSentinelConnectionFactory"


@pytest.mark.parametrize(
    "conn_factory,expected",
    [
        (
            "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory",
            pool.AsyncSentinelConnectionFactory,
        ),
        (
            "django_valkey.async_cache.pool.AsyncConnectionFactory",
            pool.AsyncConnectionFactory,
        ),
    ],
)
async def test_connection_factory_opts(conn_factory: str, expected):
    cf = sync_pool.get_connection_factory(
        path=None,
        options={
            "CONNECTION_FACTORY": conn_factory,
            "SENTINELS": [("127.0.0.1", "26739")],
        },
    )
    assert isinstance(cf, expected)


@pytest.mark.parametrize(
    "conn_factory,expected",
    [
        (
            "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory",
            pool.AsyncSentinelConnectionFactory,
        ),
        (
            "django_valkey.async_cache.pool.AsyncConnectionFactory",
            pool.AsyncConnectionFactory,
        ),
    ],
)
async def test_connection_factory_path(conn_factory: str, expected):
    cf = sync_pool.get_connection_factory(
        path=conn_factory,
        options={
            "SENTINELS": [("127.0.0.1", "26739")],
        },
    )
    assert isinstance(cf, expected)


async def test_connection_factory_no_sentinels():
    with pytest.raises(ImproperlyConfigured):
        sync_pool.get_connection_factory(
            path=None,
            options={
                "CONNECTION_FACTORY": "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory",
            },
        )
