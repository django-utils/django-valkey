import asyncio
import contextlib
import datetime
import threading
from collections.abc import Iterable
from datetime import timedelta
from typing import List, cast
from unittest.mock import patch, AsyncMock

import pytest
from pytest_django.fixtures import SettingsWrapper
from pytest_mock import MockerFixture

from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.test import override_settings

from django_valkey.async_cache.cache import AsyncValkeyCache
from django_valkey.async_cache.client import AsyncHerdClient
from django_valkey.serializers.json import JSONSerializer
from django_valkey.serializers.msgpack import MSGPackSerializer


pytestmark = pytest.mark.anyio


@pytest.fixture
async def patch_itersize_setting() -> Iterable[None]:
    del caches["default"]
    with override_settings(DJANGO_VALKEY_SCAN_ITERSIZE=30):
        yield

    del caches["default"]


class TestAsyncDjangoValkeyCache:
    async def test_set_int(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("Herd client's set method works differently")
        await cache.aset("test_key", 1)
        result = await cache.aget("test_key")
        assert type(result) is int
        raw_client = await cache.client._get_client(write=False, client=None)
        assert await raw_client.get(":1:test_key") == b"1"

    async def test_set_float(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("Herd client's set method works differently")
        await cache.aset("test_key2", 1.1)
        result = await cache.aget("test_key2")
        assert type(result) is float
        raw_client = await cache.client._get_client(write=False, client=None)
        assert await raw_client.get(":1:test_key2") == b"1.1"

    async def test_setnx(self, cache: AsyncValkeyCache):
        await cache.delete("test_key_nx")
        res = await cache.get("test_key_nx")
        assert res is None

        res = await cache.set("test_key_nx", 1, nx=True)
        assert bool(res) is True

        res = await cache.set("test_key_nx", 2, nx=True)
        assert res is None
        res = await cache.get("test_key_nx")
        assert res == 1

        await cache.delete("test_key_nx")
        res = await cache.get("test_key_nx")
        assert res is None

    async def test_setnx_timeout(self, cache: AsyncValkeyCache):
        # test that timeout still works for nx=True
        res = await cache.aset("test_key_nx", 1, timeout=2, nx=True)
        assert res is True
        await asyncio.sleep(3)
        res = await cache.aget("test_key_nx")
        assert res is None

        # test that timeout will not affect key, if it was there
        await cache.aset("test_key_nx", 1)
        res = await cache.aset("test_key_nx", 2, timeout=2, nx=True)
        assert res is None
        await asyncio.sleep(3)
        res = await cache.aget("test_key_nx")
        assert res == 1

        await cache.adelete("test_key_nx")
        res = await cache.aget("test_key_nx")
        assert res is None

    async def test_unicode_keys(self, cache: AsyncValkeyCache):
        await cache.aset("ключ", "value")
        res = await cache.aget("ключ")
        assert res == "value"

    async def test_save_and_integer(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 2)
        res = await cache.aget("test_key", "Foo")

        assert isinstance(res, int)
        assert res == 2

    async def test_save_string(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", "hello" * 1000)
        res = await cache.aget("test_key")

        assert isinstance(res, str)
        assert res == "hello" * 1000

        await cache.aset("test_key", "2")
        res = await cache.aget("test_key")

        assert isinstance(res, str)
        assert res == "2"

    async def test_save_unicode(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", "heló")
        res = await cache.aget("test_key")

        assert isinstance(res, str)
        assert res == "heló"

        async def test_save_dict(self, cache: AsyncValkeyCache):
            if isinstance(
                cache.client._serializer, (JSONSerializer, MSGPackSerializer)
            ):
                # JSONSerializer and MSGPackSerializer use the isoformat for
                # datetimes.
                now_dt: str | datetime.datetime = datetime.datetime.now().isoformat()
            else:
                now_dt = datetime.datetime.now()

            test_dict = {"id": 1, "date": now_dt, "name": "Foo"}

            await cache.aset("test_key", test_dict)
            res = await cache.aget("test_key")

            assert isinstance(res, dict)
            assert res["id"] == 1
            assert res["name"] == "Foo"
            assert res["date"] == now_dt

    async def test_save_float(self, cache: AsyncValkeyCache):
        float_val = 1.345620002

        await cache.aset("test_key", float_val)
        res = await cache.aget("test_key")

        assert isinstance(res, float)
        assert res == float_val

    async def test_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=3)
        await asyncio.sleep(4)

        res = await cache.aget("test_key")
        assert res is None

    async def test_timeout_0(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=0)
        res = await cache.aget("test_key")
        assert res is None

    async def test_timeout_parameter_as_positional_argument(
        self, cache: AsyncValkeyCache
    ):
        await cache.aset("test_key", 222, -1)
        res = await cache.aget("test_key")
        assert res is None

        await cache.aset("test_key", 222, 1)
        res1 = await cache.aget("test_key")
        await asyncio.sleep(2)
        res2 = await cache.aget("test_key")
        assert res1 == 222
        assert res2 is None

        # nx=True should not overwrite expire of key already in db
        await cache.aset("test_key", 222, None)
        await cache.aset("test_key", 222, -1, nx=True)
        res = await cache.aget("test_key")
        assert res == 222

    async def test_timeout_negative(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=-1)
        res = await cache.aget("test_key")
        assert res is None

        await cache.aset("test_key", 222, timeout=None)
        await cache.aset("test_key", 222, timeout=-1)
        res = await cache.aget("test_key")
        assert res is None

        # nx=True should not overwrite expire of key already in db
        await cache.aset("test_key", 222, timeout=None)
        await cache.aset("test_key", 222, timeout=-1, nx=True)
        res = await cache.aget("test_key")
        assert res == 222

    async def test_timeout_tiny(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=0.00001)
        res = await cache.aget("test_key")
        assert res in (None, 222)

    async def test_set_add(self, cache: AsyncValkeyCache):
        await cache.aset("add_key", "Initial value")
        res = await cache.aadd("add_key", "New value")
        assert res is None

        res = await cache.aget("add_key")
        assert res == "Initial value"
        res = await cache.aadd("other_key", "New value")
        assert res is True

    async def test_get_many(self, cache: AsyncValkeyCache):
        await cache.aset("a", 1)
        await cache.aset("b", 2)
        await cache.aset("c", 3)

        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    async def test_mget(self, cache: AsyncValkeyCache):
        await cache.aset("a", 1)
        await cache.aset("b", 2)
        await cache.aset("c", 3)

        res = await cache.mget(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    async def test_get_many_unicode(self, cache: AsyncValkeyCache):
        await cache.aset("a", "1")
        await cache.aset("ب", "2")
        await cache.aset("c", "الف")

        res = await cache.aget_many(["a", "ب", "c"])
        assert res == {"a": "1", "ب": "2", "c": "الف"}

    async def test_mget_unicode(self, cache: AsyncValkeyCache):
        await cache.aset("a", "1")
        await cache.aset("ب", "2")
        await cache.aset("c", "الف")

        res = await cache.mget(["a", "ب", "c"])
        assert res == {"a": "1", "ب": "2", "c": "الف"}

    async def test_set_many(self, cache: AsyncValkeyCache):
        await cache.aset_many({"a": 1, "b": 2, "c": 3})
        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    async def test_mset(self, cache: AsyncValkeyCache):
        await cache.amset({"a": 1, "b": 2, "c": 3})
        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    async def test_set_call_empty_pipeline(
        self,
        cache: AsyncValkeyCache,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        tmp = await cache.client.get_client(write=True)
        pipeline = await tmp.pipeline()
        key = "key"
        value = "value"

        mocked_set = mocker.patch.object(pipeline, "set", AsyncMock())
        await cache.aset(key, value, client=pipeline)

        if isinstance(cache.client, AsyncHerdClient):
            default_timeout = cache.client._backend.default_timeout
            herd_timeout = (default_timeout + settings.CACHE_HERD_TIMEOUT) * 1000
            herd_pack_value = cache.client._pack(value, default_timeout)
            mocked_set.assert_awaited_once_with(
                cache.client.make_key(key, version=None),
                cache.client.encode(herd_pack_value),
                nx=False,
                px=herd_timeout,
                xx=False,
            )
        else:
            mocked_set.assert_awaited_once_with(
                cache.client.make_key(key, version=None),
                cache.client.encode(value),
                nx=False,
                px=cache.client._backend.default_timeout * 1000,
                xx=False,
            )

    async def test_delete(self, cache: AsyncValkeyCache):
        await cache.aset_many({"a": 1, "b": 2, "c": 3})
        res = await cache.adelete("a")
        assert res is True

        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"b": 2, "c": 3}

        res = await cache.adelete("a")
        assert res is False

    async def test_delete_return_value_type(self, cache: AsyncValkeyCache):
        await cache.aset("a", 1)
        res = await cache.adelete("a")
        assert isinstance(res, bool)
        assert res is True
        res = await cache.adelete("b")
        assert isinstance(res, bool)
        assert res is False

    async def test_delete_many(self, cache: AsyncValkeyCache):
        await cache.aset_many({"a": 1, "b": 2, "c": 3})
        res = await cache.adelete_many(["a", "b"])
        assert res == 2

        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = await cache.adelete_many(["a", "b"])
        assert res == 0

    async def test_delete_many_generator(self, cache: AsyncValkeyCache):
        await cache.aset_many({"a": 1, "b": 2, "c": 3})
        res = await cache.adelete_many(key for key in ["a", "b"])
        assert res == 2

        res = await cache.aget_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = await cache.adelete_many((key for key in ("a", "b")))
        assert res == 0

    async def test_delete_many_empty_generator(self, cache: AsyncValkeyCache):
        res = await cache.adelete_many(key for key in cast(List[str], []))
        assert res == 0

    async def test_incr(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("HerdClient doesn't support incr")

        await cache.aset("num", 1)

        await cache.aincr("num")
        res = await cache.aget("num")
        assert res == 2

        await cache.aincr("num", 10)
        res = await cache.aget("num")
        assert res == 12

        # max 64-bit signed int
        await cache.aset("num", 9223372036854775807)

        await cache.aincr("num")
        res = await cache.aget("num")
        assert res == 9223372036854775808

        await cache.aincr("num", 2)
        res = await cache.aget("num")
        assert res == 9223372036854775810

        await cache.aset("num", 3)

        await cache.aincr("num", 2)
        res = await cache.aget("num")
        assert res == 5

    async def test_incr_no_timeout(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("HerdClient doesn't support incr")

        await cache.aset("num", 1, timeout=None)

        await cache.aincr("num")
        res = await cache.aget("num")
        assert res == 2

        await cache.aincr("num", 10)
        res = await cache.aget("num")
        assert res == 12

        # max 64-bit signed int
        await cache.aset("num", 9223372036854775807, timeout=None)

        await cache.aincr("num")
        res = await cache.aget("num")
        assert res == 9223372036854775808

        await cache.aincr("num", 2)
        res = await cache.aget("num")
        assert res == 9223372036854775810

        await cache.aset("num", 3, timeout=None)

        await cache.aincr("num", 2)
        res = await cache.aget("num")
        assert res == 5

    async def test_incr_error(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("HerdClient doesn't support incr")

        with pytest.raises(ValueError):
            # key does not exist
            await cache.aincr("numnum")

    async def test_incr_ignore_check(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("HerdClient doesn't support incr")

        # key exists check will be skipped and the value will be incremented by
        # '1' which is the default delta
        await cache.aincr("num", ignore_key_check=True)
        res = await cache.aget("num")
        assert res == 1
        await cache.adelete("num")

        # since key doesn't exist it is set to the delta value, 10 in this case
        await cache.aincr("num", 10, ignore_key_check=True)
        res = await cache.aget("num")
        assert res == 10
        await cache.adelete("num")

        # following are just regression checks to make sure it still works as
        # expected with incr max 64-bit signed int
        await cache.aset("num", 9223372036854775807)

        await cache.aincr("num", ignore_key_check=True)
        res = await cache.aget("num")
        assert res == 9223372036854775808

        await cache.aincr("num", 2, ignore_key_check=True)
        res = await cache.aget("num")
        assert res == 9223372036854775810

        await cache.aset("num", 3)

        await cache.aincr("num", 2, ignore_key_check=True)
        res = await cache.aget("num")
        assert res == 5

    async def test_get_set_bool(self, cache: AsyncValkeyCache):
        await cache.aset("bool", True)
        res = await cache.aget("bool")

        assert isinstance(res, bool)
        assert res is True

        await cache.aset("bool", False)
        res = await cache.aget("bool")

        assert isinstance(res, bool)
        assert res is False

    async def test_decr(self, cache: AsyncValkeyCache):
        if isinstance(cache.client, AsyncHerdClient):
            pytest.skip("HerdClient doesn't support decr")

        await cache.aset("num", 20)

        await cache.adecr("num")
        res = await cache.aget("num")
        assert res == 19

        await cache.adecr("num", 20)
        res = await cache.aget("num")
        assert res == -1

        await cache.adecr("num", 2)
        res = await cache.aget("num")
        assert res == -3

        await cache.aset("num", 20)

        await cache.adecr("num")
        res = await cache.aget("num")
        assert res == 19

        # max 64-bit signed int + 1
        await cache.aset("num", 9223372036854775808)

        await cache.adecr("num")
        res = await cache.aget("num")
        assert res == 9223372036854775807

        await cache.adecr("num", 2)
        res = await cache.aget("num")
        assert res == 9223372036854775805

    async def test_version(self, cache: AsyncValkeyCache):
        await cache.aset("keytest", 2, version=2)
        res = await cache.aget("keytest")
        assert res is None

        res = await cache.aget("keytest", version=2)
        assert res == 2

    async def test_incr_version(self, cache: AsyncValkeyCache):
        await cache.aset("keytest", 2)
        await cache.aincr_version("keytest")

        res = await cache.aget("keytest")
        assert res is None

        res = await cache.aget("keytest", version=2)
        assert res == 2

    async def test_ttl_incr_version_no_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("my_key", "hello world!", timeout=None)

        await cache.aincr_version("my_key")

        my_value = await cache.aget("my_key", version=2)

        assert my_value == "hello world!"

    async def test_delete_pattern(self, cache: AsyncValkeyCache):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            await cache.aset(key, "foo")

        res = await cache.adelete_pattern("*foo-a*")
        assert bool(res) is True

        keys = await cache.akeys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

        res = await cache.adelete_pattern("*foo-a*")
        assert bool(res) is False

    @patch(
        "django_valkey.async_cache.cache.AsyncValkeyCache.client",
        new_callable=AsyncMock,
    )
    async def test_delete_pattern_with_custom_count(
        self, client_mock, cache: AsyncValkeyCache
    ):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            await cache.aset(key, "foo")

        await cache.adelete_pattern("*foo-a*", itersize=2)

        client_mock.adelete_pattern.assert_awaited_once_with("*foo-a*", itersize=2)

    @patch(
        "django_valkey.async_cache.cache.AsyncValkeyCache.client",
        new_callable=AsyncMock,
    )
    async def test_delete_pattern_with_settings_default_scan_count(
        self,
        client_mock,
        patch_itersize_setting,
        cache: AsyncValkeyCache,
        settings: SettingsWrapper,
    ):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            await cache.aset(key, "foo")
        expected_count = settings.DJANGO_VALKEY_SCAN_ITERSIZE

        await cache.adelete_pattern("*foo-a*")

        client_mock.adelete_pattern.assert_awaited_once_with(
            "*foo-a*", itersize=expected_count
        )

    async def test_close(self, cache: AsyncValkeyCache, settings: SettingsWrapper):
        settings.DJANGO_VALKEY_CLOSE_CONNECTION = True
        await cache.aset("f", "1")
        await cache.aclose()

    async def test_close_client(self, cache: AsyncValkeyCache, mocker: MockerFixture):
        mock = mocker.patch.object(cache.client, "close")
        await cache.close()
        assert mock.called

    async def test_ttl(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", 10)
        ttl = await cache.attl("foo")

        if isinstance(cache.client, AsyncHerdClient):
            assert pytest.approx(ttl) == 12
        else:
            assert pytest.approx(ttl) == 10

        # Test ttl None
        await cache.aset("foo", "foo", timeout=None)
        ttl = await cache.attl("foo")
        assert ttl is None

        # Test ttl with expired key
        await cache.aset("foo", "foo", timeout=-1)
        ttl = await cache.attl("foo")
        assert ttl == 0

        # Test ttl with not existent key
        ttl = await cache.attl("not-existent-key")
        assert ttl == 0

    async def test_pttl(self, cache: AsyncValkeyCache):
        # Test pttl
        await cache.aset("foo", "bar", 10)
        ttl = await cache.apttl("foo")

        # delta is set to 10 as precision error causes tests to fail
        if isinstance(cache.client, AsyncHerdClient):
            assert pytest.approx(ttl, 10) == 12000
        else:
            assert pytest.approx(ttl, 10) == 10000

        # Test pttl with float value
        await cache.aset("foo", "bar", 5.5)
        ttl = await cache.apttl("foo")

        if isinstance(cache.client, AsyncHerdClient):
            assert pytest.approx(ttl, 10) == 7500
        else:
            assert pytest.approx(ttl, 10) == 5500

        # Test pttl None
        await cache.aset("foo", "foo", timeout=None)
        ttl = await cache.apttl("foo")
        assert ttl is None

        # Test pttl with expired key
        await cache.aset("foo", "foo", timeout=-1)
        ttl = await cache.apttl("foo")
        assert ttl == 0

        # Test pttl with not existent key
        ttl = await cache.apttl("not-existent-key")
        assert ttl == 0

    async def test_persist(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", timeout=20)
        assert await cache.apersist("foo") is True

        ttl = await cache.attl("foo")
        assert ttl is None
        assert await cache.apersist("not-existent-key") is False

    async def test_expire(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", timeout=None)
        assert await cache.aexpire("foo", 20) is True
        ttl = await cache.attl("foo")
        assert pytest.approx(ttl) == 20
        assert await cache.aexpire("not-existent-key", 20) is False

    async def test_expire_with_default_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", timeout=None)
        assert await cache.aexpire("foo", DEFAULT_TIMEOUT) is True
        assert await cache.aexpire("not-existent-key", DEFAULT_TIMEOUT) is False

    async def test_pexpire(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", timeout=None)
        assert await cache.apexpire("foo", 20500) is True
        ttl = await cache.apttl("foo")
        # delta is set to 10 as precision error causes tests to fail
        assert pytest.approx(ttl, 10) == 20500
        assert await cache.apexpire("not-existent-key", 20500) is False

    async def test_pexpire_with_default_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar", timeout=None)
        assert await cache.apexpire("foo", DEFAULT_TIMEOUT) is True
        assert await cache.apexpire("not-existent-key", DEFAULT_TIMEOUT) is False

    async def test_pexpire_at(self, cache: AsyncValkeyCache):
        # Test settings expiration time 1 hour ahead by datetime.
        await cache.aset("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert await cache.apexpire_at("foo", expiration_time) is True
        ttl = await cache.apttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        await cache.aset("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert (
            await cache.apexpire_at("foo", int(expiration_time.timestamp() * 1000))
            is True
        )
        ttl = await cache.apttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=2).total_seconds() * 1000

        # Test settings expiration time 1 hour in the past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.apexpire_at("foo", expiration_time) is True
        value = await cache.aget("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert await cache.apexpire_at("not-existent-key", expiration_time) is False

    async def test_expire_at(self, cache: AsyncValkeyCache):
        # Test settings expiration time 1 hour ahead by datetime.
        await cache.aset("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert await cache.aexpire_at("foo", when=expiration_time) is True
        ttl = await cache.attl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        await cache.aset("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert await cache.aexpire_at("foo", int(expiration_time.timestamp())) is True
        ttl = await cache.attl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds() * 2

        # Test settings expiration time 1 hour in the past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.aexpire_at("foo", expiration_time) is True
        value = await cache.aget("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert await cache.aexpire_at("not-existent-key", expiration_time) is False

    async def test_lock(self, cache: AsyncValkeyCache):
        lock = await cache.aget_lock("foobar")
        await lock.acquire(blocking=True)

        assert await cache.ahas_key("foobar")
        await lock.release()
        assert not await cache.ahas_key("foobar")

    @pytest.mark.filterwarnings("ignore")
    async def test_lock_released_by_thread(self, cache: AsyncValkeyCache):
        lock = await cache.lock("foobar", thread_local=False)
        await lock.acquire(blocking=True)

        async def release_lock(lock_):
            await lock_.release()

        t = threading.Thread(target=asyncio.run, args=[release_lock(lock)])
        t.start()
        t.join()

        assert not await cache.has_key("foobar")

    async def test_iter_keys(self, cache: AsyncValkeyCache):
        await cache.aset("foo1", 1)
        await cache.aset("foo2", 1)
        await cache.aset("foo3", 1)

        # Test simple result
        result = set()
        async with contextlib.aclosing(cache.aiter_keys("foo*")) as keys:
            async for k in keys:
                result.add(k)
        assert result == {"foo1", "foo2", "foo3"}

    async def test_iter_keys_itersize(self, cache: AsyncValkeyCache):
        await cache.aset("foo1", 1)
        await cache.aset("foo2", 1)
        await cache.aset("foo3", 1)

        # Test limited result
        result = []
        async with contextlib.aclosing(cache.aiter_keys("foo*", itersize=2)) as keys:
            async for k in keys:
                result.append(k)
        assert len(result) == 3

    async def test_iter_keys_generator(self, cache: AsyncValkeyCache):
        await cache.aset("foo1", 1)
        await cache.aset("foo2", 1)
        await cache.aset("foo3", 1)

        # Test generator object
        result = cache.aiter_keys("foo*")
        next_value = anext(result)  # noqa: F821
        assert await next_value is not None

    async def test_primary_replica_switching(self, cache: AsyncValkeyCache):
        cache = cast(AsyncValkeyCache, caches["sample"])
        client = cache.client
        client._server = ["foo", "bar"]
        client._clients = ["Foo", "Bar"]

        assert await client.get_client(write=True) == "Foo"
        assert await client.get_client(write=False) == "Bar"

    async def test_primary_replica_switching_with_index(self, cache: AsyncValkeyCache):
        cache = cast(AsyncValkeyCache, caches["sample"])
        client = cache.client
        client._server = ["foo", "bar"]
        client._clients = ["Foo", "Bar"]

        assert await client.get_client_with_index(write=True) == ("Foo", 0)
        assert await client.get_client_with_index(write=False) == ("Bar", 1)

    async def test_touch_zero_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=10)

        assert await cache.atouch("test_key", 0) is True
        res = await cache.aget("test_key")
        assert res is None

    async def test_touch_positive_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=10)

        assert await cache.atouch("test_key", 2) is True
        assert await cache.aget("test_key") == 222
        await asyncio.sleep(3)
        assert await cache.aget("test_key") is None

    async def test_touch_negative_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", 222, timeout=10)

        assert await cache.atouch("test_key", -1) is True
        res = await cache.aget("test_key")
        assert res is None

    async def test_touch_missed_key(self, cache: AsyncValkeyCache):
        assert await cache.atouch("test_key_does_not_exist", 1) is False

    async def test_touch_forever(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", "foo", timeout=1)
        result = await cache.atouch("test_key", None)
        assert result is True
        assert await cache.attl("test_key") is None
        await asyncio.sleep(2)
        assert await cache.aget("test_key") == "foo"

    async def test_touch_forever_nonexistent(self, cache: AsyncValkeyCache):
        result = await cache.atouch("test_key_does_not_exist", None)
        assert result is False

    async def test_touch_default_timeout(self, cache: AsyncValkeyCache):
        await cache.aset("test_key", "foo", timeout=1)
        result = await cache.atouch("test_key")
        assert result is True
        await asyncio.sleep(2)
        assert await cache.aget("test_key") == "foo"

    async def test_clear(self, cache: AsyncValkeyCache):
        await cache.aset("foo", "bar")
        value_from_cache = await cache.aget("foo")
        assert value_from_cache == "bar"
        await cache.aclear()
        value_from_cache_after_clear = await cache.aget("foo")
        assert value_from_cache_after_clear is None

    async def test_hset(self, cache: AsyncValkeyCache):
        assert await cache.ahset("foo_hash1", "foo1", "bar1") == 1
        await cache.ahset("foo_hash1", "foo2", "bar2")
        assert await cache.ahlen("foo_hash1") == 2
        assert await cache.ahexists("foo_hash1", "foo1")
        assert await cache.ahexists("foo_hash1", "foo2")

    async def test_hdel(self, cache: AsyncValkeyCache):
        await cache.ahset("foo_hash2", "foo1", "bar1")
        await cache.ahset("foo_hash2", "foo2", "bar2")
        assert await cache.ahlen("foo_hash2") == 2
        deleted_count = await cache.ahdel("foo_hash2", "foo1")
        assert deleted_count == 1
        assert await cache.ahlen("foo_hash2") == 1
        assert not await cache.ahexists("foo_hash2", "foo1")
        assert await cache.ahexists("foo_hash2", "foo2")

    async def test_hlen(self, cache: AsyncValkeyCache):
        assert await cache.ahlen("foo_hash3") == 0
        await cache.ahset("foo_hash3", "foo1", "bar1")
        assert await cache.ahlen("foo_hash3") == 1
        await cache.ahset("foo_hash3", "foo2", "bar2")
        assert await cache.ahlen("foo_hash3") == 2

    async def test_hkeys(self, cache: AsyncValkeyCache):
        await cache.ahset("foo_hash4", "foo1", "bar1")
        await cache.ahset("foo_hash4", "foo2", "bar2")
        await cache.ahset("foo_hash4", "foo3", "bar3")
        keys = await cache.ahkeys("foo_hash4")
        assert len(keys) == 3
        for i in range(len(keys)):
            assert keys[i] == f"foo{i + 1}"

    async def test_hexists(self, cache: AsyncValkeyCache):
        await cache.ahset("foo_hash5", "foo1", "bar1")
        assert await cache.ahexists("foo_hash5", "foo1")
        assert not await cache.ahexists("foo_hash5", "foo")

    async def test_sadd(self, cache: AsyncValkeyCache):
        assert await cache.asadd("foo", "bar") == 1
        assert await cache.asmembers("foo") == {"bar"}

    async def test_sadd_int(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", 1)
        assert await cache.asmembers("foo") == {1}
        raw_client = await cache.client._get_client(write=False, client=None)
        assert await raw_client.smembers(":1:foo") == [b"1"]

    async def test_sadd_float(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", 1.2)
        assert await cache.asmembers("foo") == {1.2}
        raw_client = await cache.client._get_client(write=False, client=None)
        assert await raw_client.smembers(":1:foo") == [b"1.2"]

    async def test_scard(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar", "bar2")
        assert await cache.ascard("foo") == 2

    async def test_sdiff(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asdiff("foo1", "foo2") == {"bar1"}

    async def test_sdiffstore(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asdiffstore("foo3", "foo1", "foo2") == 1
        assert await cache.asmembers("foo3") == {"bar1"}

    async def test_sdiffstore_with_keys_version(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2", version=2)
        await cache.asadd("foo2", "bar2", "bar3", version=2)
        assert await cache.asdiffstore("foo3", "foo1", "foo2", version_keys=2) == 1
        assert await cache.asmembers("foo3") == {"bar1"}

    async def test_sdiffstore_with_different_keys_versions_without_initial_set_in_version(
        self, cache: AsyncValkeyCache
    ):
        await cache.asadd("foo1", "bar1", "bar2", version=1)
        await cache.asadd("foo2", "bar2", "bar3", version=2)
        assert await cache.asdiffstore("foo3", "foo1", "foo2", version_keys=2) == 0

    async def test_sdiffstore_with_different_keys_versions_with_initial_set_in_version(
        self, cache: AsyncValkeyCache
    ):
        await cache.asadd("foo1", "bar1", "bar2", version=2)
        await cache.asadd("foo2", "bar2", "bar3", version=1)
        assert await cache.asdiffstore("foo3", "foo1", "foo2", version_keys=2) == 2

    async def test_sinter(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asinter("foo1", "foo2") == {"bar2"}

    async def test_interstore(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asinterstore("foo3", "foo1", "foo2") == 1
        assert await cache.asmembers("foo3") == {"bar2"}

    async def test_sismember_str(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar")
        assert await cache.asismember("foo", "bar") is True
        assert await cache.asismember("foo", "bar2") is False

    async def test_sismember_int(self, cache: AsyncValkeyCache):
        await cache.asadd("baz", 3)
        assert await cache.asismember("baz", 3) is True
        assert await cache.asismember("baz", 2) is False

    async def test_smove(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asmove("foo1", "foo2", "bar1") is True
        assert await cache.asmove("foo1", "foo2", "bar4") is False
        assert await cache.asmembers("foo1") == {"bar2"}
        assert await cache.asmembers("foo2") == {"bar1", "bar2", "bar3"}

    async def test_spop_default_count(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        assert await cache.aspop("foo") in {"bar1", "bar2"}
        assert await cache.asmembers("foo") in [{"bar1"}, {"bar2"}]

    async def test_spop(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        assert await cache.aspop("foo", 1) in [{"bar1"}, {"bar2"}]
        assert await cache.asmembers("foo") in [{"bar1"}, {"bar2"}]

    async def test_srandmember_default_count(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        assert await cache.asrandmember("foo") in {"bar1", "bar2"}

    async def test_srandmember(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        assert await cache.asrandmember("foo", 1) in [["bar1"], ["bar2"]]

    async def test_srem(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        assert await cache.asrem("foo", "bar1") == 1
        assert await cache.asrem("foo", "bar3") == 0

    async def test_sscan(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        items = await cache.asscan("foo")
        assert items == {"bar1", "bar2"}

    async def test_sscan_with_match(self, cache: AsyncValkeyCache):
        if cache.client._has_compression_enabled():
            pytest.skip("Compression is enabled, sscan with match is not supported")
        await cache.asadd("foo", "bar1", "bar2", "zoo")
        items = await cache.asscan("foo", match="zoo")
        assert items == {"zoo"}

    async def test_sscan_iter(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2")
        items = set()
        async with contextlib.aclosing(cache.asscan_iter("foo")) as values:
            async for value in values:
                items.add(value)
        assert items == {"bar1", "bar2"}

    async def test_sscan_iter_with_match(self, cache: AsyncValkeyCache):
        if cache.client._has_compression_enabled():
            pytest.skip(
                "Compression is enabled, sscan_iter with match is not supported"
            )
        await cache.asadd("foo", "bar1", "bar2", "zoo")

        items = set()
        async with contextlib.aclosing(
            cache.asscan_iter("foo", match="bar*")
        ) as values:
            async for value in values:
                items.add(value)

        assert set(items) == {"bar1", "bar2"}

    async def test_smismember(self, cache: AsyncValkeyCache):
        await cache.asadd("foo", "bar1", "bar2", "bar3")
        assert await cache.asmismember("foo", "bar1", "bar2", "xyz") == [
            True,
            True,
            False,
        ]

    async def test_sunion(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asunion("foo1", "foo2") == {"bar1", "bar2", "bar3"}

    async def test_sunionstore(self, cache: AsyncValkeyCache):
        await cache.asadd("foo1", "bar1", "bar2")
        await cache.asadd("foo2", "bar2", "bar3")
        assert await cache.asunionstore("foo3", "foo1", "foo2") == 3
        assert await cache.asmembers("foo3") == {"bar1", "bar2", "bar3"}
