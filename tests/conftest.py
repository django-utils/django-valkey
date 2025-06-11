import copy
from collections.abc import Iterable
from typing import cast

import pytest
from pytest_django.fixtures import SettingsWrapper

from asgiref.compatibility import iscoroutinefunction
from django.core.cache import cache as default_cache, caches

from django_valkey.base import BaseValkeyCache
from django_valkey.cache import ValkeyCache


pytestmark = pytest.mark.anyio

# for some reason `isawaitable` doesn't work here
if iscoroutinefunction(default_cache.clear):

    @pytest.fixture(scope="function")
    async def cache():
        yield default_cache
        await default_cache.aclear()

else:

    @pytest.fixture
    def cache() -> Iterable[BaseValkeyCache]:
        yield default_cache
        default_cache.clear()


@pytest.fixture
def key_prefix_cache(
    cache: ValkeyCache, settings: SettingsWrapper
) -> Iterable[ValkeyCache]:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_PREFIX"] = "*"
    settings.CACHES = caches_setting
    yield cache


@pytest.fixture
def with_prefix_cache() -> Iterable[ValkeyCache]:
    with_prefix = cast(ValkeyCache, caches["with_prefix"])
    yield with_prefix
    with_prefix.clear()
