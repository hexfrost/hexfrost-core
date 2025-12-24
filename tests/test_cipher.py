import pytest
from cryptography.fernet import Fernet

from src import CipherSuiteManager, CipherSuiteSettings


@pytest.fixture
def settings():
    s = CipherSuiteSettings(
        MASTER_KEY = Fernet.generate_key().decode()
    )
    return s


@pytest.fixture
def cipher_manager(settings):
    CipherSuiteManager.set_settings(settings)

    CipherSuiteManager._cipher_suite = None
    return CipherSuiteManager


def test_set_and_get_settings(settings):
    CipherSuiteManager.set_settings(settings)
    retrieved_settings = CipherSuiteManager.get_settings()
    assert retrieved_settings == settings


def test_get_settings_not_set():
    CipherSuiteManager._settings = None
    with pytest.raises(RuntimeError, match='No settings available'):
        CipherSuiteManager.get_settings()


def test_set_cipher_suite(cipher_manager):
    custom_key = Fernet.generate_key()
    custom_cipher_suite = Fernet(custom_key)
    cipher_manager.set_cipher_suite(custom_cipher_suite)
    assert cipher_manager._get_cipher_suite() == custom_cipher_suite


def test_get_cipher_suite_uses_settings_key(cipher_manager, settings):
    cipher_suite = cipher_manager._get_cipher_suite()
    assert isinstance(cipher_suite, Fernet)

    original_cipher_suite = cipher_manager._get_cipher_suite()
    new_key = Fernet.generate_key().decode()
    settings.MASTER_KEY = new_key
    CipherSuiteManager.set_settings(settings)
    CipherSuiteManager._cipher_suite = None
    new_cipher_suite = cipher_manager._get_cipher_suite()
    assert new_cipher_suite != original_cipher_suite


def test_encrypt_decrypt(cipher_manager):
    original_string = "This is a secret message!"
    encrypted_bytes = cipher_manager.encrypt(original_string)
    assert isinstance(encrypted_bytes, bytes)
    decrypted_string = cipher_manager.decrypt(encrypted_bytes)
    assert decrypted_string == original_string


def test_encrypt_decrypt_with_manually_set_cipher_suite(cipher_manager, settings):
    original_string = "Another secret!"

    custom_key = Fernet.generate_key()
    custom_cipher = Fernet(custom_key)
    cipher_manager.set_cipher_suite(custom_cipher)

    encrypted_bytes = cipher_manager.encrypt(original_string)
    decrypted_string = cipher_manager.decrypt(encrypted_bytes)
    assert decrypted_string == original_string

    CipherSuiteManager.set_settings(settings)
    CipherSuiteManager._cipher_suite = None
    with pytest.raises(Exception):
        cipher_manager.decrypt(encrypted_bytes)


def test_decrypt_invalid_token(cipher_manager):
    invalid_bytes = b"this is not a valid fernet token"
    with pytest.raises(Exception):
        cipher_manager.decrypt(invalid_bytes)
