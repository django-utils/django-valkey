from django.core.cache import cache
from django.http import HttpResponse
from django.urls import path


async def async_view(request):
    res = await cache.get("a")
    return HttpResponse(res)


async def sync_view(request):
    res = cache.get("a")
    return HttpResponse(res)


urlpatterns = [
    path("async/", async_view),
    path("sync/", sync_view),
]
