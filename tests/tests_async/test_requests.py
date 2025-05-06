import pytest

from django.core import signals
from django.core.cache import close_caches

from django_valkey.base import close_async_caches


@pytest.mark.asyncio(loop_scope="session")
class TestWithOldSignal:
    @pytest.fixture(autouse=True)
    def setup(self):
        signals.request_finished.disconnect(close_async_caches)
        signals.request_finished.connect(close_caches)
        yield
        signals.request_finished.disconnect(close_caches)
        signals.request_finished.connect(close_async_caches)

    async def test_warning_output_when_request_finished(self, async_client):
        with pytest.warns(
            RuntimeWarning,
            match="coroutine 'AsyncBackendCommands.close' was never awaited",
        ) as record:
            await async_client.get("/async/")

        assert (
            str(record[0].message)
            == "coroutine 'AsyncBackendCommands.close' was never awaited"
        )


@pytest.mark.asyncio(loop_scope="session")
class TestWithNewSignal:
    async def test_warning_output_when_request_finished(self, async_client, recwarn):
        await async_client.get("/async/")

        assert len(recwarn) == 0
