"""
Configuration for the kerberos-impala-backend service.

All settings are loaded from environment variables (case-insensitive).
A `.env` file is supported for local development.

Required environment variables:
    KRB5_REALM, KRB5_KDC, KRB5_PRINCIPAL, KRB5_KEYTAB_PATH
    IMPALA_HOST

Optional environment variables (with defaults):
    KRB5_AUTH_MODE              (default: "keytab")
    KRB5_RENEWAL_THRESHOLD_SECONDS (default: 300)
    KRB5_MAX_RETRY              (default: 3)
    IMPALA_PORT                 (default: 21050)
    IMPALA_DATABASE             (default: "default")
    IMPALA_USE_TLS              (default: False)
    IMPALA_CA_CERT              (default: None)
    IMPALA_POOL_MIN             (default: 2)
    IMPALA_POOL_MAX             (default: 10)
    IMPALA_QUERY_TIMEOUT        (default: 60)
"""

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # ── Kerberos (required) ──────────────────────────────────────────────────
    krb5_realm: str
    krb5_kdc: str
    krb5_principal: str
    krb5_keytab_path: str

    # ── Kerberos (optional) ──────────────────────────────────────────────────
    krb5_auth_mode: str = "keytab"              # "keytab" | "kinit"
    krb5_renewal_threshold_seconds: int = 300
    krb5_max_retry: int = 3

    # ── Impala (required) ────────────────────────────────────────────────────
    impala_host: str

    # ── Impala (optional) ────────────────────────────────────────────────────
    impala_port: int = 21050
    impala_database: str = "default"
    impala_use_tls: bool = False
    impala_ca_cert: Optional[str] = None
    impala_pool_min: int = 2
    impala_pool_max: int = 10
    impala_query_timeout: int = 60              # seconds
    impala_auth_mechanism: str = "GSSAPI"       # "GSSAPI" | "NOSASL" | "PLAIN"
