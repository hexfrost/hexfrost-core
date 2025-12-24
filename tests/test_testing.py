from sqlalchemy import select

from tests.fixtures.database import temp_db, db_settings
from tests.fixtures.db_connect import database_connector

import pytest
import asyncpg
from sqlalchemy import Column, Integer, String

from testing.temporary_database import temporary_database
from src import DatabaseConnectionSettings, debug_client
from sqlalchemy.orm import declarative_base

from tests.fixtures.database import db_settings as settings

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)


async def check_database_exists(settings: DatabaseConnectionSettings, db_name: str) -> bool:
    conn = None
    try:
        # Connect to a maintenance database (e.g., 'postgres') using credentials from settings
        admin_db_settings = DatabaseConnectionSettings(
            POSTGRES_USER=settings.POSTGRES_USER,
            POSTGRES_PASSWORD=settings.POSTGRES_PASSWORD,
            POSTGRES_HOST=settings.POSTGRES_HOST,
            POSTGRES_PORT=settings.POSTGRES_PORT,
            POSTGRES_DB="postgres"  # Or any other default/maintenance DB
        )
        conn = await asyncpg.connect(dsn=admin_db_settings.get_dsn())
        result = await conn.fetchval(f"SELECT 1 FROM pg_database WHERE datname = $1", db_name)
        return result == 1
    except Exception:
        return False
    finally:
        if conn:
            await conn.close()


async def check_table_exists(settings: DatabaseConnectionSettings, table_name: str) -> bool:
    conn = None
    try:
        conn = await asyncpg.connect(dsn=settings.get_dsn())
        result = await conn.fetchval(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
            table_name
        )
        return result
    except Exception:
        return False
    finally:
        if conn:
            await conn.close()


async def drop_database(settings: DatabaseConnectionSettings, db_name: str):
    conn = None
    try:
        # Connect to a maintenance database (e.g., 'postgres') using credentials from settings
        admin_db_settings = DatabaseConnectionSettings(
            POSTGRES_USER=settings.POSTGRES_USER,
            POSTGRES_PASSWORD=settings.POSTGRES_PASSWORD,
            POSTGRES_HOST=settings.POSTGRES_HOST,
            POSTGRES_PORT=settings.POSTGRES_PORT,
            POSTGRES_DB="postgres"  # Or any other default/maintenance DB
        )
        conn = await asyncpg.connect(dsn=admin_db_settings.get_dsn())
        await conn.execute(f"DROP DATABASE IF EXISTS {db_name}")
    finally:
        if conn:
            await conn.close()


@pytest.mark.asyncio
async def test_temporary_database_creates_db_and_schema(settings: DatabaseConnectionSettings):
    local_settings = DatabaseConnectionSettings(**settings.__dict__)
    original_db_name_in_fixture = settings.POSTGRES_DB

    test_specific_original_db_name = "test_creation_db_unique"
    local_settings.POSTGRES_DB = test_specific_original_db_name

    test_db_name = f"test_{test_specific_original_db_name}"

    await drop_database(local_settings, test_db_name)
    assert not await check_database_exists(local_settings, test_db_name)

    async with temporary_database(local_settings, Base, db_prefix="test"):
        assert local_settings.POSTGRES_DB == test_db_name
        assert await check_database_exists(local_settings, test_db_name)
        assert await check_table_exists(local_settings, "users")

    assert not await check_database_exists(local_settings, test_db_name)


@pytest.mark.asyncio
async def test_temporary_database_uses_existing_db_and_creates_schema(settings: DatabaseConnectionSettings):
    local_settings = DatabaseConnectionSettings(**settings.__dict__)
    original_db_name_in_fixture = settings.POSTGRES_DB

    test_specific_original_db_name = "test_existing_db_unique"
    local_settings.POSTGRES_DB = test_specific_original_db_name

    test_db_name = f"test_{test_specific_original_db_name}"

    # Ensure the database does not exist before we try to create it for the test
    await drop_database(local_settings, test_db_name)
    assert not await check_database_exists(local_settings, test_db_name)

    # Manually create the database to simulate it already existing
    admin_settings_for_create = DatabaseConnectionSettings(**{**local_settings.__dict__, "POSTGRES_DB": "postgres"})
    conn_admin = None
    try:
        conn_admin = await asyncpg.connect(dsn=admin_settings_for_create.get_dsn())
        await conn_admin.execute(f"CREATE DATABASE {test_db_name}")
    finally:
        if conn_admin:
            await conn_admin.close()

    assert await check_database_exists(local_settings, test_db_name)

    async with temporary_database(local_settings, Base, db_prefix="test"):
        assert local_settings.POSTGRES_DB == test_db_name
        assert await check_database_exists(local_settings, test_db_name)
        assert await check_table_exists(local_settings, "users")

    assert not await check_database_exists(local_settings, test_db_name)


@pytest.mark.asyncio
async def test_temporary_database_handles_existing_db_with_schema_and_drops(settings: DatabaseConnectionSettings):
    local_settings = DatabaseConnectionSettings(**settings.__dict__)
    original_db_name_in_fixture = settings.POSTGRES_DB

    test_specific_original_db_name = "test_existing_with_schema_db_unique"
    local_settings.POSTGRES_DB = test_specific_original_db_name

    test_db_name = f"test_{test_specific_original_db_name}"

    # Ensure the database does not exist before we try to create it for the test
    await drop_database(local_settings, test_db_name)
    assert not await check_database_exists(local_settings, test_db_name)

    # Create DB
    admin_settings_for_create = DatabaseConnectionSettings(**{**local_settings.__dict__, "POSTGRES_DB": "postgres"})
    conn_admin = None
    try:
        conn_admin = await asyncpg.connect(dsn=admin_settings_for_create.get_dsn())
        await conn_admin.execute(f"CREATE DATABASE {test_db_name}")
    finally:
        if conn_admin:
            await conn_admin.close()

    settings_for_schema_setup = DatabaseConnectionSettings(**{**local_settings.__dict__, "POSTGRES_DB": test_db_name})
    conn_setup = None
    try:
        conn_setup = await asyncpg.connect(dsn=settings_for_schema_setup.get_dsn())
        await conn_setup.execute("""
                                 CREATE TABLE users
                                 (
                                     id   SERIAL PRIMARY KEY,
                                     name VARCHAR(50)
                                 );
                                 """)
    finally:
        if conn_setup:
            await conn_setup.close()

    assert await check_database_exists(local_settings, test_db_name)
    assert await check_table_exists(settings_for_schema_setup, "users")

    async with temporary_database(local_settings, Base, db_prefix="test"):
        assert local_settings.POSTGRES_DB == test_db_name
        assert await check_database_exists(local_settings, test_db_name)
        assert await check_table_exists(local_settings, "users")

    assert not await check_database_exists(local_settings, test_db_name)


async def test_fastapi_depends_itegration_test(temp_db, db_settings, database_connector):
    from fastapi import Depends, FastAPI
    app = FastAPI()

    @app.get("/")
    async def index(database_conn=Depends(database_connector)):
        await database_conn.scalar(select(1))
        return {"status": "ok"}

    async with debug_client(app) as client:
        response1 = await client.get('/')
        assert response1.status_code == 200


async def test_debug_client_negative_test(temp_db, db_settings, database_connector):
    from fastapi import FastAPI
    app = FastAPI()

    from testing.client import debug_client
    async with debug_client(app) as client:
        response1 = await client.get('/')
        assert response1.status_code == 404
