import logging
from typing import Annotated

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemes import SensitiveDataScheme
from src.alchemy.models import BaseDatabaseModel

logger = logging.getLogger()

AnyPydanticModel = Annotated[SensitiveDataScheme, Depends(SensitiveDataScheme)]
AnySQLAlchemyModel = Annotated[BaseDatabaseModel, Depends(BaseDatabaseModel)]


class AbstractDatabaseCrudManager:
    _alchemy_model = type[AnySQLAlchemyModel]
    _pydantic_model = type[AnyPydanticModel]
    _unique_together_fields: tuple = None

    @classmethod
    async def get_all_by_org_id(cls, channel_id: int, conn) -> list:
        query = select(cls._alchemy_model).where(cls._alchemy_model.channel_id == channel_id)
        data = await conn.execute(query)
        raw_data = data.all()
        data_models = [cls._pydantic_model(**d[0].__dict__) for d in raw_data]
        return data_models

    @classmethod
    def to_alchemy_model(cls, pydantic_model: AnyPydanticModel) -> AnySQLAlchemyModel:
        encripted_data = pydantic_model.encrypt_fields()
        obj = cls._alchemy_model(**encripted_data.model_dump_with_secrets())
        return obj

    @classmethod
    def to_pydantic_model(cls, alchemy_model: AnySQLAlchemyModel) -> AnyPydanticModel:
        new_model = cls._pydantic_model.model_validate(alchemy_model.__dict__)
        decrypted_model = new_model.decrypt_fields()
        return decrypted_model

    @classmethod
    async def add_one(cls, conn: AsyncSession, data_model: AnyPydanticModel):
        obj = cls.to_alchemy_model(data_model)
        conn.add(obj)
        await conn.commit()
        await conn.refresh(obj)

    @classmethod
    async def get_all(cls, conn: AsyncSession, limit: int = 2000, offset: int = 0) -> list[AnyPydanticModel]:
        query = select(cls._alchemy_model).limit(limit).offset(offset)
        raw_data = (await conn.execute(query)).all()
        result = [cls.to_pydantic_model(v[0]) for v in raw_data]
        return result

    @classmethod
    async def put_one(cls, conn, data_model: AnyPydanticModel) -> AnyPydanticModel:
        query = select(cls._alchemy_model).where(
            *(getattr(cls._alchemy_model, field) == getattr(data_model, field) for field in cls._unique_together_fields)
        )
        try:
            obj = (await conn.execute(query)).scalar_one_or_none()
        except MultipleResultsFound as e:
            logger.error(f"Multiple results found for unique fields {cls._unique_together_fields} with data {data_model}: {e}")
            return None

        if obj:
            encrypted_data = data_model.encrypt_fields()
            for key, value in encrypted_data.model_dump_with_secrets().items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
            await conn.commit()
        else:
            await cls.add_one(conn, data_model)
