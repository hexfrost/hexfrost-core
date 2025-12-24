import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, async_sessionmaker, create_async_engine

from src.alchemy.models import DatabaseConnectionSettings

logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    def __init__(self, settings: DatabaseConnectionSettings):
        self._settings: DatabaseConnectionSettings = settings
        self._engine: AsyncEngine | None = None
        self._async_sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._schema_mapping: dict | None = settings.SCHEMA_MAPPING

    def _get_settings(self) -> DatabaseConnectionSettings:
        if not self._settings:
            raise RuntimeError("No settings available")
        return self._settings

    def get_engine(self) -> AsyncEngine:
        if not self._engine:
            settings = self._get_settings()
            connection_string = (
                f"postgresql+asyncpg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
                f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
            )
            engine_kwargs = {}
            if self._schema_mapping:
                engine_kwargs["execution_options"] = {"schema_translate_map": self._schema_mapping}

            self._engine = create_async_engine(connection_string, **engine_kwargs)
        return self._engine

    def get_session_maker(self) -> async_sessionmaker[AsyncSession]:
        if not self._async_sessionmaker:
            engine = self.get_engine()
            self._async_sessionmaker = async_sessionmaker(
                engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )
        return self._async_sessionmaker

    @asynccontextmanager
    async def get_db_session(self) -> AsyncGenerator[AsyncSession, None]:
        session_maker = self.get_session_maker()
        async with session_maker() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}", exc_info=True)
                raise
            finally:
                await session.close()

    async def __call__(self) -> AsyncSession:
        session_maker = self.get_session_maker()
        return session_maker()

    async def __aenter__(self) -> AsyncSession:
        self._session = await self.__call__()
        return self._session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self._session.rollback()
        await self._session.close()

    async def scalar(self, query):
        async with self.get_db_session() as session:
            result = await session.scalar(query)
            return result

    async def dispose(self):
        if self._engine:
            await self._engine.dispose()
            logger.info("Database engine disposed successfully")


class DatabaseConnector(DatabaseConnectionManager):
    pass
