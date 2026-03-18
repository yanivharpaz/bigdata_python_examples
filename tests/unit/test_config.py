"""
Unit tests for app/config.py (Settings / Config layer).

Requirements covered:
  - 5.3: KRB5_REALM, KRB5_KDC, KRB5_PRINCIPAL, KRB5_KEYTAB_PATH are required
  - 5.4: IMPALA_HOST is required
  - 5.6: ValidationError names the missing field
"""

import os
import pytest
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {
    "KRB5_REALM": "TESTREALM",
    "KRB5_KDC": "kdc.example.com",
    "KRB5_PRINCIPAL": "svc@TESTREALM",
    "KRB5_KEYTAB_PATH": "/etc/krb5/svc.keytab",
    "IMPALA_HOST": "impala.example.com",
}


def _load_settings_with_env(monkeypatch, env: dict):
    """Set env vars via monkeypatch, then import a fresh Settings instance."""
    # Clear any pre-existing values for all required keys so tests are isolated
    for key in REQUIRED_FIELDS:
        monkeypatch.delenv(key, raising=False)

    for key, value in env.items():
        monkeypatch.setenv(key, value)

    # Re-import to avoid module-level caching; Settings reads env at instantiation
    from app.config import Settings
    return Settings(_env_file=None)  # disable .env file so only env vars are used


# ---------------------------------------------------------------------------
# Required field tests (Req 5.3, 5.4, 5.6)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("missing_field", list(REQUIRED_FIELDS.keys()))
def test_missing_required_field_raises_validation_error(monkeypatch, missing_field):
    """Omitting any single required field must raise ValidationError naming that field."""
    env = {k: v for k, v in REQUIRED_FIELDS.items() if k != missing_field}
    with pytest.raises(ValidationError) as exc_info:
        _load_settings_with_env(monkeypatch, env)

    # The error message must mention the missing field (case-insensitive match)
    error_text = str(exc_info.value).lower()
    assert missing_field.lower() in error_text, (
        f"Expected '{missing_field}' to appear in ValidationError, got:\n{exc_info.value}"
    )


def test_all_required_fields_present_does_not_raise(monkeypatch):
    """Providing all required fields must not raise."""
    settings = _load_settings_with_env(monkeypatch, REQUIRED_FIELDS)
    assert settings.krb5_realm == "TESTREALM"
    assert settings.krb5_kdc == "kdc.example.com"
    assert settings.krb5_principal == "svc@TESTREALM"
    assert settings.krb5_keytab_path == "/etc/krb5/svc.keytab"
    assert settings.impala_host == "impala.example.com"


# ---------------------------------------------------------------------------
# Optional field defaults (Req 5.5)
# ---------------------------------------------------------------------------

def test_optional_fields_use_correct_defaults(monkeypatch):
    """When only required fields are provided, optional fields must equal their documented defaults."""
    settings = _load_settings_with_env(monkeypatch, REQUIRED_FIELDS)

    assert settings.krb5_auth_mode == "keytab"
    assert settings.krb5_renewal_threshold_seconds == 300
    assert settings.krb5_max_retry == 3

    assert settings.impala_port == 21050
    assert settings.impala_database == "default"
    assert settings.impala_use_tls is False
    assert settings.impala_ca_cert is None
    assert settings.impala_pool_min == 2
    assert settings.impala_pool_max == 10
    assert settings.impala_query_timeout == 60


def test_optional_fields_can_be_overridden(monkeypatch):
    """Optional fields must accept overrides from environment variables."""
    env = {
        **REQUIRED_FIELDS,
        "KRB5_AUTH_MODE": "kinit",
        "KRB5_RENEWAL_THRESHOLD_SECONDS": "120",
        "KRB5_MAX_RETRY": "5",
        "IMPALA_PORT": "21051",
        "IMPALA_DATABASE": "analytics",
        "IMPALA_USE_TLS": "true",
        "IMPALA_CA_CERT": "/etc/ssl/ca.pem",
        "IMPALA_POOL_MIN": "4",
        "IMPALA_POOL_MAX": "20",
        "IMPALA_QUERY_TIMEOUT": "120",
    }
    settings = _load_settings_with_env(monkeypatch, env)

    assert settings.krb5_auth_mode == "kinit"
    assert settings.krb5_renewal_threshold_seconds == 120
    assert settings.krb5_max_retry == 5
    assert settings.impala_port == 21051
    assert settings.impala_database == "analytics"
    assert settings.impala_use_tls is True
    assert settings.impala_ca_cert == "/etc/ssl/ca.pem"
    assert settings.impala_pool_min == 4
    assert settings.impala_pool_max == 20
    assert settings.impala_query_timeout == 120


# ---------------------------------------------------------------------------
# .env file loading (Req 5.2)
# ---------------------------------------------------------------------------

def test_env_file_loading(monkeypatch, tmp_path):
    """Settings must load required fields from a .env file."""
    # Ensure no env vars interfere
    for key in REQUIRED_FIELDS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "KRB5_REALM=FILEREALM\n"
        "KRB5_KDC=kdc.file.com\n"
        "KRB5_PRINCIPAL=file_svc@FILEREALM\n"
        "KRB5_KEYTAB_PATH=/tmp/file.keytab\n"
        "IMPALA_HOST=impala.file.com\n"
    )

    from app.config import Settings
    settings = Settings(_env_file=str(env_file))

    assert settings.krb5_realm == "FILEREALM"
    assert settings.krb5_kdc == "kdc.file.com"
    assert settings.krb5_principal == "file_svc@FILEREALM"
    assert settings.krb5_keytab_path == "/tmp/file.keytab"
    assert settings.impala_host == "impala.file.com"


def test_env_file_with_optional_overrides(monkeypatch, tmp_path):
    """Optional fields in a .env file must override defaults."""
    for key in REQUIRED_FIELDS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "KRB5_REALM=FILEREALM\n"
        "KRB5_KDC=kdc.file.com\n"
        "KRB5_PRINCIPAL=file_svc@FILEREALM\n"
        "KRB5_KEYTAB_PATH=/tmp/file.keytab\n"
        "IMPALA_HOST=impala.file.com\n"
        "IMPALA_PORT=9999\n"
        "IMPALA_DATABASE=warehouse\n"
    )

    from app.config import Settings
    settings = Settings(_env_file=str(env_file))

    assert settings.impala_port == 9999
    assert settings.impala_database == "warehouse"
