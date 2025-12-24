import pytest
from sqlalchemy import Column, Integer, String, orm, LargeBinary

from src.schemes import SensitiveDataScheme, SensitiveFieldData
from src.alchemy.repositories import AbstractDatabaseCrudManager
from tests.fixtures.database import temp_db, db_settings
from tests.fixtures.db_connect import database_connector


class BaseDatabaseModel(orm.DeclarativeBase):
    pass


class TestSQLAlchemyModel(BaseDatabaseModel):
    __tablename__ = "test_items"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    sensitive_field = Column(LargeBinary)


class TestPydanticModel(SensitiveDataScheme):
    _sensitive_attributes = ["sensitive_field"]

    name: str
    sensitive_field: SensitiveFieldData


class TestItemRepository(AbstractDatabaseCrudManager):
    _alchemy_model = TestSQLAlchemyModel
    _pydantic_model = TestPydanticModel

class MockCipherManager:
    def encrypt(self, value):
        return f"{value}_secret".encode()

    def decrypt(self, value):
        return value.decode()[:-7]


async def test_add_one_sensitive_field(temp_db, database_connector, db_settings):
    new_obj = TestPydanticModel(
        name="test_item",
        sensitive_field="test_sensitive_field"
    )

    TestPydanticModel.set_cipher_suite_manager(MockCipherManager())
    async with database_connector.get_db_session() as conn:
        from sqlalchemy.schema import CreateTable
        try:
            await conn.execute(CreateTable(TestSQLAlchemyModel.__table__))
            await conn.commit()
        except Exception:
            await conn.rollback()

        await conn.execute(TestSQLAlchemyModel.__table__.delete())
        await conn.commit()

        all_ = await TestItemRepository.get_all(conn)
        assert len(all_) == 0

        await TestItemRepository.add_one(conn, new_obj)

        import asyncpg
        another_conn = await asyncpg.connect(dsn=db_settings.get_dsn())
        raw_results = await another_conn.fetch(f"SELECT * FROM {TestSQLAlchemyModel.__table__}")
        await another_conn.close()

        assert len(raw_results) == 1
        result = dict(zip(raw_results[0].keys(), raw_results[0].values()))

        assert result["sensitive_field"] == "test_sensitive_field_secret".encode()

        all_ = await TestItemRepository.get_all(conn)
        assert len(all_) == 1
        obj = all_[0]
        assert obj.sensitive_field == "test_sensitive_field"
