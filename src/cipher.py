import dataclasses
from typing import ClassVar, Protocol

from cryptography.fernet import Fernet


@dataclasses.dataclass
class CipherSuiteSettings:
    MASTER_KEY: str


class CipherSuiteManagerInterface(Protocol):
    @classmethod
    def set_settings(cls, settings: CipherSuiteSettings): ...

    @classmethod
    def create_settings(cls, master_key: str) -> None: ...

    @classmethod
    def get_settings(cls) -> CipherSuiteSettings: ...

    @classmethod
    def set_cipher_suite(cls, cipher_suite: Fernet): ...

    @classmethod
    def encrypt(cls, string: str) -> bytes: ...

    @classmethod
    def decrypt(cls, b_string: bytes) -> str: ...


class CipherSuiteManager:
    _cipher_suite: Fernet = None
    _settings: ClassVar[CipherSuiteSettings] = None

    @classmethod
    def set_settings(cls, settings: CipherSuiteSettings):
        cls._settings = settings

    @classmethod
    def create_settings(cls, master_key: str):
        new_settings = CipherSuiteSettings(MASTER_KEY=master_key)
        cls.set_settings(new_settings)

    @classmethod
    def get_settings(cls):
        if not cls._settings:
            raise RuntimeError("No settings available")
        return cls._settings

    @classmethod
    def set_cipher_suite(cls, cipher_suite: Fernet):
        cls._cipher_suite = cipher_suite
        return cls._cipher_suite

    @classmethod
    def _get_cipher_suite(cls):
        if not cls._cipher_suite:
            settings = cls.get_settings()
            master_key = settings.MASTER_KEY
            cls._cipher_suite = Fernet(master_key)
        return cls._cipher_suite

    @classmethod
    def encrypt(cls, string: str) -> bytes:
        cipher_suite = cls._get_cipher_suite()
        encoded_text = cipher_suite.encrypt(string.encode())
        return encoded_text

    @classmethod
    def decrypt(cls, b_string: bytes) -> str:
        cipher_suite = cls._get_cipher_suite()
        decoded_text = cipher_suite.decrypt(b_string)
        return decoded_text.decode()
