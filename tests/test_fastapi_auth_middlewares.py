import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from src import ResponseMessages


async def token_validator(self, token: str) -> bool:
    if token == "valid_token_321":
        return True
    return False


@pytest.fixture(autouse=True)
def config():
    pass

@pytest.fixture(autouse=True)
def app(config) -> FastAPI:
    app_ = FastAPI()
    from src import BearerTokenMiddleware, BearerTokenMiddlewareSettings
    s = BearerTokenMiddlewareSettings(
        token_validator=token_validator,
        exclude_paths=[]
    )
    BearerTokenMiddleware.set_settings(s)
    app_.add_middleware(BearerTokenMiddleware)

    @app_.get('/auth')
    def auth():
        return {'auth': 'success'}

    return app_


async def test_auth_middleware_403_invalid_token(app: FastAPI, config):
    async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test',
            headers={'Authorization': 'Bearer invalid_token_123'},
    ) as client:
        response = await client.get('/auth')

    assert response.status_code == 403
    current_responce = response.json()
    assert current_responce == ResponseMessages.invalid_token


async def test_auth_middleware_401_invalid_scheme(app: FastAPI, config):
    async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test', headers={'Authorization': 'NotBearer some_token'},
    ) as client:
        response = await client.get('/auth')

    assert response.status_code == 401
    current_responce = response.json()
    assert current_responce == ResponseMessages.invalid_scheme


async def test_auth_middleware_401_header_missing(app: FastAPI, config):
    async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test', headers={},
    ) as client:
        response = await client.get('/auth')

    assert response.status_code == 401
    current_responce = response.json()
    assert current_responce == ResponseMessages.header_missing


async def test_auth_middleware_200(app: FastAPI, config):
    async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test',
            headers={'Authorization': f'Bearer valid_token_321'},
    ) as client:
        response = await client.get('/auth')

    assert response.status_code == 200
    current_responce = response.json()
    assert current_responce == {'auth': 'success'}
