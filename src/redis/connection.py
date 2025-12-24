import dataclasses
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Literal, Any

logger = logging.getLogger(__name__)

BrokerType = Literal["redis", "rabbitmq", "kafka"]

@dataclasses.dataclass
class QueueConnectionSettings:
    BROKER_URL: str
    BROKER_USER: str | None = None
    BROKER_PASSWORD: str | None = None


class QueueConnectionManager:

    def __init__(self, settings: QueueConnectionSettings):
        self._settings: QueueConnectionSettings = settings
        self._broker: Any | None = None



    def _get_settings(self):
        pass

    def set_broker(self, maker):
        pass

    def get_broker(self):
        if not self._broker:
            # FIXME add logic
            pass

    @asynccontextmanager
    async def broker_context(self) -> AsyncGenerator[Any, None]:
        broker_instance = self.get_broker()
        async with broker_instance as b:
            try:
                yield b
            except Exception as e:
                logger.error({"msg": "Error within broker_context", "broker_type": self._settings.BROKER_TYPE, "error": str(e)}, exc_info=True)
                raise

    async def dispose(self):
        if self._broker:
            try:
                await self._broker.close()
                logger.info(f"Broker of type '{self._settings.BROKER_TYPE}' disposed successfully.")
            except Exception as e:
                logger.error({"msg": "Error during broker disposal", "broker_type": self._settings.BROKER_TYPE, "error": str(e)}, exc_info=True)
            finally:
                self._broker = None

__all__ = ["QueueConnectionManager", "QueueConnectionSettings", "BrokerType"]
