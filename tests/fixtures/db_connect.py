import pytest

from src import DatabaseConnectionManager
from tests.fixtures.database import temp_db, db_settings


@pytest.fixture(autouse=True)
def database_connector(temp_db, db_settings):
    dc = DatabaseConnectionManager(settings=db_settings)
    return dc
