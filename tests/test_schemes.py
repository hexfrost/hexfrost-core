import pytest

from src import SensitiveDataScheme


class MockCipherSuiteManager:
    @staticmethod
    def encrypt(value: str) -> bytes:
        return f"encrypted_{value}".encode()

    @staticmethod
    def decrypt(value: bytes) -> str:
        return value.decode().replace("encrypted_", "")


@pytest.fixture
def mock_cipher_suite_manager():
    SensitiveDataScheme.set_cipher_suite_manager(MockCipherSuiteManager)


class TestSensitiveModel(SensitiveDataScheme):
    _sensitive_attributes = ["sensitive_field1", "sensitive_field2"]
    sensitive_field1: str | bytes | None = None
    sensitive_field2: str | bytes | None = None
    regular_field: str | None = None


@pytest.fixture
def test_model_instance():
    return TestSensitiveModel(
        sensitive_field1="value1",
        sensitive_field2="value2",
        regular_field="value3",
    )


def test_encrypt_fields(test_model_instance: TestSensitiveModel, mock_cipher_suite_manager):
    encrypted_model = test_model_instance.encrypt_fields()
    assert encrypted_model.sensitive_field1 == b"encrypted_value1"
    assert encrypted_model.sensitive_field2 == b"encrypted_value2"
    assert encrypted_model.regular_field == "value3"


def test_decrypt_fields(mock_cipher_suite_manager):
    encrypted_model = TestSensitiveModel(
        sensitive_field1=b"encrypted_value1",
        sensitive_field2=b"encrypted_value2",
        regular_field="value3",
    )
    decrypted_model = encrypted_model.decrypt_fields()
    assert decrypted_model.sensitive_field1 == "value1"
    assert decrypted_model.sensitive_field2 == "value2"
    assert decrypted_model.regular_field == "value3"


def test_encrypt_decrypt_cycle(test_model_instance: TestSensitiveModel, mock_cipher_suite_manager):
    original_field1 = test_model_instance.sensitive_field1
    original_field2 = test_model_instance.sensitive_field2

    encrypted_model = test_model_instance.encrypt_fields()
    decrypted_model = encrypted_model.decrypt_fields()

    assert decrypted_model.sensitive_field1 == original_field1
    assert decrypted_model.sensitive_field2 == original_field2
    assert decrypted_model.regular_field == "value3"


def test_none_sensitive_fields(mock_cipher_suite_manager):
    model_with_none = TestSensitiveModel(
        sensitive_field1=None,
        sensitive_field2="value2",
        regular_field="value3",
    )
    encrypted_model = model_with_none.encrypt_fields()
    assert encrypted_model.sensitive_field1 is None
    assert encrypted_model.sensitive_field2 == b"encrypted_value2"

    decrypted_model = encrypted_model.decrypt_fields()
    assert decrypted_model.sensitive_field1 is None
    assert decrypted_model.sensitive_field2 == "value2"


def test_empty_sensitive_fields_list(mock_cipher_suite_manager):
    class TestModelNoSensitive(SensitiveDataScheme):
        _sensitive_attributes = []
        field1: str = "test"

    instance = TestModelNoSensitive()
    encrypted_instance = instance.encrypt_fields()
    assert encrypted_instance.field1 == "test"
    decrypted_instance = encrypted_instance.decrypt_fields()
    assert decrypted_instance.field1 == "test"


class MockCipherSuiteManagerNonString:
    @staticmethod
    def encrypt(value):
        if isinstance(value, str):
            return f"encrypted_{value}"
        return value

    @staticmethod
    def decrypt(value):
        if isinstance(value, str) and value.startswith("encrypted_"):
            return value.replace("encrypted_", "")
        return value

class TestModelWithInt(SensitiveDataScheme):
    _sensitive_attributes = ["sensitive_int"]
    sensitive_int: int | None = None

def test_non_string_sensitive_field_encryption_decryption(mock_cipher_suite_manager):
    SensitiveDataScheme.set_cipher_suite_manager(MockCipherSuiteManagerNonString)

    instance = TestModelWithInt(sensitive_int=123)
    encrypted = instance.encrypt_fields()


    assert encrypted.sensitive_int == 123

    decrypted = encrypted.decrypt_fields()
    assert decrypted.sensitive_int == 123


def test_base_scheme_instantiation():
    from src import BaseScheme

    class TestBase(BaseScheme):
        field_a: str
        field_b: int

    instance = TestBase(field_a="hello", field_b=10)
    assert instance.field_a == "hello"
    assert instance.field_b == 10
