from contextlib import asynccontextmanager


@asynccontextmanager
async def debug_client(app, app_path: str = "http://test"):
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url=app_path,
    ) as client:
        yield client
        pass
