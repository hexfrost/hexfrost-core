from __future__ import annotations

import dataclasses
import logging
import time
from collections.abc import Callable
from typing import Iterable

from fastapi import Request
from fastapi import Response
from fastapi.security import HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from src.middlewares.fastapi.enums import ResponseMessages

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BearerTokenMiddlewareSettings:
    token_validator: Callable
    exclude_paths: Iterable[str]


class BearerTokenMiddleware(BaseHTTPMiddleware):
    _token_validator: Callable
    _exclude_paths: Iterable[str]

    def __init__(
        self,
        app,
    ):
        super().__init__(app)
        self.security = HTTPBearer()

    @classmethod
    def set_settings(cls, settings: BearerTokenMiddlewareSettings):
        cls._exclude_paths = settings.exclude_paths
        cls._token_validator = settings.token_validator

    @classmethod
    def create_settings(cls, token_validator=Callable, exclude_paths=None):
        new_settings = BearerTokenMiddlewareSettings(token_validator=token_validator, exclude_paths=exclude_paths or [])
        cls.set_settings(new_settings)

    async def _is_token_valid(self, token: str) -> bool:
        if await self._token_validator(token):
            return True
        logger.warning(
            "Token validation logic against external storage is not implemented. "
            "Access will be denied for all tokens."
        )
        return False

    async def dispatch(self, request: Request, call_next: Callable):
        if any(request.url.path.startswith(path) for path in self._exclude_paths):
            return await call_next(request)

        authorization = request.headers.get("Authorization")
        if not authorization:
            return JSONResponse(
                status_code=401,
                content=ResponseMessages.header_missing,
            )

        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            return JSONResponse(
                status_code=401,
                content=ResponseMessages.invalid_scheme,
            )

        if not await self._is_token_valid(token):
            return JSONResponse(
                status_code=403,
                content=ResponseMessages.invalid_token,
            )

        response = await call_next(request)
        return response


class LoggerMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        path_components = request.url.path.split("/")
        bot_id = path_components[-1] if len(path_components) > 1 else "unknown"

        logger.info(
            f""""{request.method} {request.url.path}" - {bot_id} {request.client.host if request.client else 'unknown'}"""
        )

        try:
            response = await call_next(request)
            process_time = time.time() - start_time

            logger.info(
                f""""{request.method} {request.url.path}" {response.status_code} - {process_time:.3f}s - {bot_id} """
            )
            return response
        except Exception as e:
            process_time = time.time() - start_time
            logger.error(f""""{request.method} {request.url.path}" 500 - {process_time:.3f}s - {bot_id} | {str(e)} """)
            return JSONResponse(status_code=500, content=ResponseMessages.internal_server_error)
