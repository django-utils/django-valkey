from typing import Iterable

import pytest
import pytest_asyncio
from django.core.cache import cache as default_cache

from django_valkey import valkey as default_valkey
from django_valkey.base import BaseValkeyCache


# for some reason `isawaitable` doesn't work here
try:

    @pytest_asyncio.fixture(loop_scope="session")
    async def cache():
        yield default_cache
        await default_cache.aclear()

except Exception:

    @pytest.fixture
    def cache() -> Iterable[BaseValkeyCache]:
        yield default_cache
        default_cache.clear()


try:

    @pytest_asyncio.fixture(loop_scope="session")
    async def valkey():
        yield default_valkey
        await default_valkey.aclear()

except Exception:

    @pytest.fixture
    def valkey() -> Iterable[BaseValkeyCache]:
        yield default_valkey
        default_valkey.clear()
