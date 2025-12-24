import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
import dataclasses

from src.alchemy.models import DatabaseConnectionSettings
from src.alchemy.connection import DatabaseConnector

logger = logging.getLogger(__name__)


class TemporaryDatabaseConnector(DatabaseConnector):
    def get_engine(self) -> "AsyncEngine":
        if not self._engine:
            s = self._settings
            if not s:
                raise RuntimeError("Settings not set for TemporaryDatabaseConnector")
            db_host = s.POSTGRES_HOST

            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy.pool import NullPool

            self._engine = create_async_engine(
                f"postgresql+asyncpg://{s.POSTGRES_USER}:{s.POSTGRES_PASSWORD}@{db_host}:{s.POSTGRES_PORT}/{s.POSTGRES_DB}",
                poolclass=NullPool,
            ).execution_options(schema_translate_map=self._schema_mapping)
        return self._engine


@asynccontextmanager
async def temporary_database(settings: "DatabaseConnectionSettings", base_model, db_prefix: str = "test"):
    original_settings = settings.__class__(**settings.__dict__)
    if original_settings.POSTGRES_HOST.startswith("test_"):
        test_db_name = original_settings.POSTGRES_HOST
    else:
        test_db_name = f"{db_prefix}_{original_settings.POSTGRES_DB}"
        settings.POSTGRES_DB = test_db_name

    logger.info(f"Creating temporary database: '{test_db_name}'")

    dsn = settings.get_dsn().replace(f"/{settings.POSTGRES_DB}", "/postgres")
    async with asyncio.Lock() as lock:
        try:
            try:
                conn = await asyncpg.connect(dsn=settings.get_dsn())
                await conn.close()
                logger.info(f"Database '{test_db_name}' already exists")
            except Exception as e:
                conn = await asyncpg.connect(dsn=dsn)
                await conn.execute(f"CREATE DATABASE {settings.POSTGRES_DB}")
                logger.info(f"Successfully created database '{settings.POSTGRES_DB}'")
                await conn.close()
        except Exception as e:
            logger.error({"mgs": e})
            await conn.close()

    from sqlalchemy import create_engine

    engine = create_engine(settings.get_dsn())
    base_model.metadata.drop_all(bind=engine)
    base_model.metadata.create_all(bind=engine)
    logger.info(f"Tables created in '{test_db_name}'")
    yield
    base_model.metadata.drop_all(bind=engine)
    engine.dispose()

    conn_for_drop = None
    db_to_drop_name = settings.POSTGRES_DB

    dsn_for_maintenance_db = original_settings.get_dsn().replace(f"/{original_settings.POSTGRES_DB}", "/postgres")

    try:
        logger.info(f"Attempting to drop database: '{db_to_drop_name}' using maintenance DSN: {dsn_for_maintenance_db}")
        conn_for_drop = await asyncpg.connect(dsn=dsn_for_maintenance_db)

        await conn_for_drop.execute(f'DROP DATABASE IF EXISTS "{db_to_drop_name}"')
        logger.info(f"Successfully executed DROP DATABASE for '{db_to_drop_name}'")
    except Exception as e:
        logger.error(
            f"Failed to drop database '{db_to_drop_name}'. Error: {e.__class__.__name__}: {e}",
            exc_info=True,
        )

    finally:
        if conn_for_drop:
            try:
                await conn_for_drop.close()
                logger.info(f"Closed connection to maintenance DB used for dropping '{db_to_drop_name}'")
            except Exception as e_close:
                logger.error(
                    f"Failed to close asyncpg connection to maintenance DB after attempting to drop '{db_to_drop_name}'. Error: {e_close}",
                    exc_info=True,
                )


async def drop_all_test_databases(settings: "DatabaseConnectionSettings", db_prefix: str = "test_"):
    maintenance_db_name = "postgres"
    maintenance_settings = dataclasses.replace(settings, POSTGRES_DB=maintenance_db_name)
    maintenance_conn_str = maintenance_settings.get_dsn()

    conn = None
    dropped_dbs_count = 0

    logger.info(
        f"Connecting to {maintenance_settings.POSTGRES_HOST}:{maintenance_settings.POSTGRES_PORT}/{maintenance_db_name} to manage databases..."
    )

    try:
        conn = await asyncpg.connect(maintenance_conn_str)

        databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false;")

        target_databases = []
        for db_record in databases:
            db_name = db_record["datname"]
            if db_name.startswith(db_prefix):

                if db_name == maintenance_db_name:
                    logger.debug(f"Skipping drop of maintenance database '{db_name}' even though it matches prefix.")
                    continue
                target_databases.append(db_name)

        if not target_databases:
            logger.info(f"No databases found starting with prefix '{db_prefix}' to drop.")
            return

        logger.info(f"Found the following databases to drop: {', '.join(target_databases)}")

        for db_name_to_drop in target_databases:
            logger.info(f"Attempting to drop database: {db_name_to_drop}...")
            try:

                await conn.execute(f'DROP DATABASE IF EXISTS "{db_name_to_drop}";')
                logger.info(f"Database '{db_name_to_drop}' dropped successfully.")
                dropped_dbs_count += 1
            except asyncpg.exceptions.ObjectInUseError:
                logger.warning(
                    f"Could not drop database '{db_name_to_drop}' because it is currently in use. "
                    f"Please ensure all connections to '{db_name_to_drop}' are closed."
                )
            except Exception as e:
                logger.error(
                    f"An error occurred while dropping database '{db_name_to_drop}': {e}",
                    exc_info=True,
                )

        logger.info(f"\nDatabase drop process completed. Total databases dropped: {dropped_dbs_count}.")

    except ConnectionRefusedError:
        logger.error(
            f"Failed to connect to PostgreSQL at: {maintenance_conn_str}. "
            f"Check connection parameters and server availability."
        )
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            logger.info("PostgreSQL connection for database management closed.")
