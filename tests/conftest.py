from collections.abc import Iterable

import pytest
import pytest_asyncio

from asgiref.compatibility import iscoroutinefunction
from django.core.cache import cache as default_cache

from django_valkey.base import BaseValkeyCache


# for some reason `isawaitable` doesn't work here
if iscoroutinefunction(default_cache.clear):

    @pytest_asyncio.fixture(loop_scope="session")
    async def cache():
        yield default_cache
        await default_cache.aclear()

else:

    @pytest.fixture
    def cache() -> Iterable[BaseValkeyCache]:
        yield default_cache
        default_cache.clear()
