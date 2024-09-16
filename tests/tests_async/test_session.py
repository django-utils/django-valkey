import base64
import unittest
from datetime import timedelta
from typing import Optional, Type

import django
import pytest
from django.conf import settings
from django.contrib.sessions.backends.base import SessionBase
from django.contrib.sessions.backends.cache import SessionStore as CacheSession
from django.core.cache import DEFAULT_CACHE_ALIAS, caches
from django.test import override_settings
from django.utils import timezone

from django_valkey.serializers.msgpack import MSGPackSerializer

SessionType = Type[SessionBase]


# Copied from Django's sessions test suite. Keep in sync with upstream.
# https://github.com/django/django/blob/main/tests/sessions_tests/tests.py
class SessionTestsMixin:
    # This does not inherit from TestCase to avoid any tests being run with this
    # class, which wouldn't work, and to allow different TestCase subclasses to
    # be used.

    backend: Optional[SessionType] = None  # subclasses must specify

    async def setUp(self):
        self.session = self.backend()

    async def tearDown(self):
        # NB: be careful to delete any sessions created; stale sessions fill up
        # the /tmp (with some backends) and eventually overwhelm it after lots
        # of runs (think buildbots)
        await self.session.adelete()

    async def test_new_session(self):
        self.assertIs(self.session.modified, False)
        self.assertIs(self.session.accessed, False)

    async def test_get_empty(self):
        self.assertIsNone(await self.session.aget("cat"))

    async def test_store(self):
        self.session["cat"] = "dog"
        self.assertIs(self.session.modified, True)
        self.assertEqual(await self.session.apop("cat"), "dog")

    async def test_pop(self):
        self.session["some key"] = "exists"
        # Need to reset these to pretend we haven't accessed it:
        self.accessed = False
        self.modified = False

        self.assertEqual(await self.session.apop("some key"), "exists")
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, True)
        self.assertIsNone(await self.session.aget("some key"))

    async def test_pop_default(self):
        self.assertEqual(
            await self.session.apop("some key", "does not exist"), "does not exist"
        )
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_pop_default_named_argument(self):
        self.assertEqual(
            await self.session.apop("some key", default="does not exist"),
            "does not exist",
        )
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_pop_no_default_keyerror_raised(self):
        with self.assertRaises(KeyError):
            await self.session.apop("some key")

    async def test_setdefault(self):
        self.assertEqual(await self.session.asetdefault("foo", "bar"), "bar")
        self.assertEqual(await self.session.asetdefault("foo", "baz"), "bar")
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, True)

    async def test_update(self):
        await self.session.aupdate({"update key": 1})
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, True)
        self.assertEqual(await self.session.aget("update key"), 1)

    async def test_has_key(self):
        self.session["some key"] = 1
        self.session.modified = False
        self.session.accessed = False
        self.assertIn("some key", self.session)
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_values(self):
        self.assertEqual(list(await self.session.avalues()), [])
        self.assertIs(self.session.accessed, True)
        self.session["some key"] = 1
        self.session.modified = False
        self.session.accessed = False
        self.assertEqual(list(await self.session.avalues()), [1])
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_keys(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        self.assertEqual(list(await self.session.akeys()), ["x"])
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_items(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        self.assertEqual(list(await self.session.aitems()), [("x", 1)])
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, False)

    async def test_clear(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        self.assertEqual(list(await self.session.aitems()), [("x", 1)])
        self.session.clear()
        self.assertEqual(list(await self.session.aitems()), [])
        self.assertIs(self.session.accessed, True)
        self.assertIs(self.session.modified, True)

    async def test_save(self):
        await self.session.asave()
        self.assertIs(await self.session.aexists(self.session.session_key), True)

    async def test_delete(self):
        await self.session.asave()
        await self.session.adelete(self.session.session_key)
        self.assertIs(await self.session.aexists(self.session.session_key), False)

    async def test_flush(self):
        self.session["foo"] = "bar"
        await self.session.asave()
        prev_key = self.session.session_key
        await self.session.aflush()
        self.assertIs(await self.session.aexists(prev_key), False)
        self.assertNotEqual(self.session.session_key, prev_key)
        self.assertIsNone(self.session.session_key)
        self.assertIs(self.session.modified, True)
        self.assertIs(self.session.accessed, True)

    async def test_cycle(self):
        self.session["a"], self.session["b"] = "c", "d"
        await self.session.asave()
        prev_key = self.session.session_key
        prev_data = list(await self.session.aitems())
        await self.session.acycle_key()
        self.assertIs(await self.session.aexists(prev_key), False)
        self.assertNotEqual(self.session.session_key, prev_key)
        self.assertEqual(list(await self.session.aitems()), prev_data)

    async def test_cycle_with_no_session_cache(self):
        self.session["a"], self.session["b"] = "c", "d"
        await self.session.asave()
        prev_data = await self.session.aitems()
        self.session = self.backend(self.session.session_key)
        self.assertIs(hasattr(self.session, "_session_cache"), False)
        await self.session.acycle_key()
        self.assertCountEqual(await self.session.aitems(), prev_data)

    async def test_save_doesnt_clear_data(self):
        self.session["a"] = "b"
        await self.session.asave()
        self.assertEqual(self.session["a"], "b")

    async def test_invalid_key(self):
        # Submitting an invalid session key (either by guessing, or if the db has
        # removed the key) results in a new key being generated.
        try:
            session = self.backend("1")
            await session.asave()
            self.assertNotEqual(session.session_key, "1")
            self.assertIsNone(await session.aget("cat"))
            await session.adelete()
        finally:
            # Some backends leave a stale cache entry for the invalid
            # session key; make sure that entry is manually deleted
            await session.adelete("1")

    async def test_session_key_empty_string_invalid(self):
        """Falsey values (Such as an empty string) are rejected."""
        self.session._session_key = ""
        self.assertIsNone(self.session.session_key)

    async def test_session_key_too_short_invalid(self):
        """Strings shorter than 8 characters are rejected."""
        self.session._session_key = "1234567"
        self.assertIsNone(self.session.session_key)

    async def test_session_key_valid_string_saved(self):
        """Strings of length 8 and up are accepted and stored."""
        self.session._session_key = "12345678"
        self.assertEqual(self.session.session_key, "12345678")

    async def test_session_key_is_read_only(self):
        async def set_session_key(session):
            session.session_key = await session._aget_new_session_key()

        with self.assertRaises(AttributeError):
            await set_session_key(self.session)

    # Custom session expiry
    async def test_default_expiry(self):
        # A normal session has a max age equal to settings
        self.assertEqual(
            await self.session.aget_expiry_age(), settings.SESSION_COOKIE_AGE
        )

        # So does a custom session with an idle expiration time of 0 (but it'll
        # expire at browser close)
        await self.session.aset_expiry(0)
        self.assertEqual(
            await self.session.aget_expiry_age(), settings.SESSION_COOKIE_AGE
        )

    async def test_custom_expiry_seconds(self):
        modification = timezone.now()

        await self.session.aset_expiry(10)

        date = await self.session.aget_expiry_date(modification=modification)
        self.assertEqual(date, modification + timedelta(seconds=10))

        age = await self.session.aget_expiry_age(modification=modification)
        self.assertEqual(age, 10)

    async def test_custom_expiry_timedelta(self):
        modification = timezone.now()

        # Mock timezone.now, because set_expiry calls it on this code path.
        original_now = timezone.now
        try:
            timezone.now = lambda: modification
            await self.session.aset_expiry(timedelta(seconds=10))
        finally:
            timezone.now = original_now

        date = await self.session.aget_expiry_date(modification=modification)
        self.assertEqual(date, modification + timedelta(seconds=10))

        age = await self.session.aget_expiry_age(modification=modification)
        self.assertEqual(age, 10)

    async def test_custom_expiry_datetime(self):
        modification = timezone.now()

        await self.session.aset_expiry(modification + timedelta(seconds=10))

        date = await self.session.aget_expiry_date(modification=modification)
        self.assertEqual(date, modification + timedelta(seconds=10))

        age = await self.session.aget_expiry_age(modification=modification)
        self.assertEqual(age, 10)

    async def test_custom_expiry_reset(self):
        await self.session.aset_expiry(None)
        await self.session.aset_expiry(10)
        await self.session.aset_expiry(None)
        self.assertEqual(
            await self.session.aget_expiry_age(), settings.SESSION_COOKIE_AGE
        )

    async def test_get_expire_at_browser_close(self):
        # Tests get_expire_at_browser_close with different settings and different
        # set_expiry calls
        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=False):
            await self.session.aset_expiry(10)
            self.assertIs(await self.session.aget_expire_at_browser_close(), False)

            await self.session.aset_expiry(0)
            self.assertIs(await self.session.aget_expire_at_browser_close(), True)

            await self.session.aset_expiry(None)
            self.assertIs(await self.session.aget_expire_at_browser_close(), False)

        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=True):
            await self.session.aset_expiry(10)
            self.assertIs(await self.session.aget_expire_at_browser_close(), False)

            await self.session.aset_expiry(0)
            self.assertIs(await self.session.aget_expire_at_browser_close(), True)

            await self.session.aset_expiry(None)
            self.assertIs(await self.session.aget_expire_at_browser_close(), True)

    async def test_decode(self):
        # Ensure we can decode what we encode
        data = {"a test key": "a test value"}
        encoded = self.session.encode(data)
        self.assertEqual(self.session.decode(encoded), data)

    async def test_decode_failure_logged_to_security(self):
        bad_encode = base64.b64encode(b"flaskdj:alkdjf").decode("ascii")
        with self.assertLogs("django.security.SuspiciousSession", "WARNING") as cm:
            self.assertEqual({}, self.session.decode(bad_encode))
        # The failed decode is logged.
        self.assertIn("corrupted", cm.output[0])

    async def test_actual_expiry(self):
        # this doesn't work with JSONSerializer (serializing timedelta)
        with override_settings(
            SESSION_SERIALIZER="django.contrib.sessions.serializers.PickleSerializer"
        ):
            self.session = self.backend()  # reinitialize after overriding settings

            # Regression test for #19200
            old_session_key = None
            new_session_key = None
            try:
                self.session["foo"] = "bar"
                await self.session.aset_expiry(-timedelta(seconds=10))
                await self.session.asave()
                old_session_key = self.session.session_key
                # With an expiry date in the past, the session expires instantly.
                new_session = self.backend(self.session.session_key)
                new_session_key = new_session.session_key
                self.assertNotIn("foo", new_session)
            finally:
                await self.session.adelete(old_session_key)
                await self.session.adelete(new_session_key)

    async def test_session_load_does_not_create_record(self):
        """
        Loading an unknown session key does not create a session record.
        Creating session records on load is a DOS vulnerability.
        """
        session = self.backend("someunknownkey")
        await session.aload()

        self.assertIsNone(session.session_key)
        self.assertIs(await session.aexists(session.session_key), False)
        # provided unknown key was cycled, not reused
        self.assertNotEqual(session.session_key, "someunknownkey")

    async def test_session_save_does_not_resurrect_session_logged_out_in_other_context(
        self,
    ):
        """
        Sessions shouldn't be resurrected by a concurrent request.
        """
        from django.contrib.sessions.backends.base import UpdateError

        # Create new session.
        s1 = self.backend()
        s1["test_data"] = "value1"
        s1.save(must_create=True)

        # Logout in another context.
        s2 = self.backend(s1.session_key)
        await s2.adelete()

        # Modify session in first context.
        s1["test_data"] = "value2"
        with self.assertRaises(UpdateError):
            # This should throw an exception as the session is deleted, not
            # resurrect the session.
            await s1.asave()

        self.assertEqual(await s1.aload(), {})


@pytest.mark.asyncio(loop_scope="session")
class SessionTests(SessionTestsMixin, unittest.TestCase):
    backend = CacheSession

    @pytest.mark.skipif(
        django.VERSION >= (4, 2),
        reason="PickleSerializer is removed as of https://code.djangoproject.com/ticket/29708",
    )
    async def test_actual_expiry(self):
        if isinstance(
            caches[DEFAULT_CACHE_ALIAS].client._serializer, MSGPackSerializer
        ):
            self.skipTest("msgpack serializer doesn't support datetime serialization")
        await super().test_actual_expiry()
