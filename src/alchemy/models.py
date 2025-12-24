from sqlalchemy import orm

from src.schemes import BaseScheme


def lenient_constructor(self, **kwargs):
    cls_ = type(self)
    for k in kwargs:
        if hasattr(cls_, k):
            setattr(self, k, kwargs[k])
    pass


registry = orm.registry(constructor=lenient_constructor)


class BaseDatabaseModel(orm.DeclarativeBase):
    registry = registry


class DatabaseConnectionSettings(BaseScheme):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    SCHEMA_MAPPING: dict | None = None

    def get_dsn(self):
        dsn = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        return dsn
