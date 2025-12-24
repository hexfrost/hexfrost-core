import logging
from typing import ClassVar, Any

from pydantic import BaseModel

from src.cipher import CipherSuiteManager

logger = logging.getLogger(__name__)
SensitiveFieldData = str | bytes


class BaseScheme(BaseModel):
    pass


class SensitiveDataScheme(BaseScheme):
    _sensitive_attributes: ClassVar[list[str]] = []
    _cipher_suite_manager: ClassVar[CipherSuiteManager] = None

    class Config:
        extra = "ignore"

    @property
    def cipher_suite_manager(self) -> CipherSuiteManager:
        return SensitiveDataScheme._cipher_suite_manager

    def get_cipher_suite_manager(self):
        if not self._cipher_suite_manager:
            raise RuntimeError("cipher_suite_manager not set")
        return self._cipher_suite_manager

    @classmethod
    def set_cipher_suite_manager(cls, suite: type[CipherSuiteManager]):
        SensitiveDataScheme._cipher_suite_manager = suite

    def encrypt_fields(self):
        fields = self._sensitive_attributes
        for field in fields:
            if hasattr(self, field):
                value = getattr(self, field)
                if value is not None:
                    encrypted_value: SensitiveFieldData = self.get_cipher_suite_manager().encrypt(value)
                    setattr(self, field, encrypted_value)
        return self

    def decrypt_fields(self):
        fields = self._sensitive_attributes
        for field in fields:
            if hasattr(self, field):
                value = getattr(self, field)
                if isinstance(
                    value, bytes
                ):
                    decrypted_value: SensitiveFieldData = self.get_cipher_suite_manager().decrypt(value)
                    setattr(self, field, decrypted_value)
        return self

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        exclude = kwargs.pop("exclude", [])
        exclude.extend(self._sensitive_attributes)
        kwargs["exclude"] = exclude
        logger.debug(f"Dumping model {self.__class__.__name__}, excluding: [{exclude}]")
        return super().model_dump(*args, **kwargs)

    def model_dump_with_secrets(self, *args, **kwargs) -> dict[str, Any]:
        return super().model_dump(*args, **kwargs)
