import asyncio


def async_to_sync(awaitable):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(awaitable)


def sync_to_async(func):
    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
