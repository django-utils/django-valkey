from collections.abc import Iterable
from inspect import isawaitable

import pytest
import pytest_asyncio
from django.core.cache import cache as default_cache

from django_valkey import valkey as default_valkey
from django_valkey.base import BaseValkeyCache

# for some reason `isawaitable` doesn't work here
if isawaitable(default_cache.clear()):

    @pytest_asyncio.fixture(loop_scope="session")
    async def cache():
        yield default_cache
        await default_cache.aclear()

else:

    @pytest.fixture
    def cache() -> Iterable[BaseValkeyCache]:
        yield default_cache
        default_cache.clear()


try:
    if isawaitable(default_valkey.clear()):

        @pytest_asyncio.fixture(loop_scope="session")
        async def valkey():
            yield default_valkey
            await default_valkey.aclear()

    else:

        @pytest.fixture
        def valkey() -> Iterable[BaseValkeyCache]:
            yield default_valkey
            default_valkey.clear()

except AttributeError:
    # cluster client doesn't support this feature yet
    pass
