"""
Unit tests for app/connection_pool.py.

Requirements covered:
  - 2.1: Connections use impyla with auth_mechanism='GSSAPI'
  - 2.3: Stale connections are replaced on release
  - 2.4: Pool exhaustion past timeout raises queue.Empty (→ 503)
  - 2.5: TLS parameters are passed when impala_use_tls=True
"""

import queue
from unittest.mock import MagicMock, call, patch

import pytest

from app.config import Settings
from app.connection_pool import ImpalaConnectionPool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(**overrides) -> Settings:
    """Build a minimal Settings object, bypassing env/file loading."""
    defaults = dict(
        krb5_realm="EXAMPLE.COM",
        krb5_kdc="kdc.example.com",
        krb5_principal="svc@EXAMPLE.COM",
        krb5_keytab_path="/etc/krb5/svc.keytab",
        impala_host="impala.example.com",
        impala_port=21050,
        impala_database="default",
        impala_use_tls=False,
        impala_ca_cert=None,
        impala_pool_min=1,
        impala_pool_max=5,
        impala_query_timeout=30,
        krb5_max_retry=1,
    )
    defaults.update(overrides)
    return Settings.model_construct(**defaults)


def _make_pool(settings: Settings) -> ImpalaConnectionPool:
    """Create a pool with a dummy KerberosManager."""
    krb_manager = MagicMock()
    return ImpalaConnectionPool(settings, krb_manager)


# ---------------------------------------------------------------------------
# test_acquire_release_cycle
# ---------------------------------------------------------------------------

def test_acquire_release_cycle():
    """Req 2.1: acquire returns the mock connection; release puts it back in pool."""
    mock_conn = MagicMock()
    settings = _make_settings(impala_pool_min=1, impala_pool_max=5)

    with patch("app.connection_pool.impala_dbapi.connect", return_value=mock_conn):
        pool = _make_pool(settings)

    # Pool should have 1 connection after warmup
    assert pool._pool.qsize() == 1

    conn = pool.acquire()
    assert conn is mock_conn
    assert pool._pool.qsize() == 0

    # Validate returns True so the same conn goes back
    with patch.object(pool, "_validate_connection", return_value=True):
        pool.release(conn)

    assert pool._pool.qsize() == 1
    assert pool._pool.get_nowait() is mock_conn


# ---------------------------------------------------------------------------
# test_stale_connection_replaced_on_release
# ---------------------------------------------------------------------------

def test_stale_connection_replaced_on_release():
    """Req 2.3: stale connection is replaced with a fresh one on release."""
    stale_conn = MagicMock(name="stale")
    fresh_conn = MagicMock(name="fresh")
    settings = _make_settings(impala_pool_min=1, impala_pool_max=5)

    with patch("app.connection_pool.impala_dbapi.connect", return_value=stale_conn):
        pool = _make_pool(settings)

    pool.acquire()  # drain the pool

    with patch.object(pool, "_validate_connection", return_value=False), \
         patch.object(pool, "_create_connection", return_value=fresh_conn):
        pool.release(stale_conn)

    assert pool._pool.qsize() == 1
    assert pool._pool.get_nowait() is fresh_conn


# ---------------------------------------------------------------------------
# test_pool_exhausted_raises_queue_empty
# ---------------------------------------------------------------------------

def test_pool_exhausted_raises_queue_empty():
    """Req 2.4: acquiring from an empty pool past timeout raises queue.Empty."""
    mock_conn = MagicMock()
    settings = _make_settings(impala_pool_min=1, impala_pool_max=1, impala_query_timeout=0.05)

    with patch("app.connection_pool.impala_dbapi.connect", return_value=mock_conn):
        pool = _make_pool(settings)

    pool.acquire()  # take the only connection

    with pytest.raises(queue.Empty):
        pool.acquire()  # should time out and raise


# ---------------------------------------------------------------------------
# test_tls_parameters_passed
# ---------------------------------------------------------------------------

def test_tls_parameters_passed():
    """Req 2.5: use_ssl and ca_cert are forwarded to impyla when TLS is enabled."""
    settings = _make_settings(
        impala_use_tls=True,
        impala_ca_cert="/etc/ssl/ca.pem",
        impala_pool_min=1,
    )

    with patch("app.connection_pool.impala_dbapi.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        _make_pool(settings)

    mock_connect.assert_called_with(
        host=settings.impala_host,
        port=settings.impala_port,
        database=settings.impala_database,
        auth_mechanism="GSSAPI",
        kerberos_service_name="impala",
        use_ssl=True,
        ca_cert="/etc/ssl/ca.pem",
    )


# ---------------------------------------------------------------------------
# test_gssapi_auth_mechanism
# ---------------------------------------------------------------------------

def test_gssapi_auth_mechanism():
    """Req 2.1: impyla connect is always called with auth_mechanism='GSSAPI'."""
    settings = _make_settings(impala_pool_min=1)

    with patch("app.connection_pool.impala_dbapi.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        _make_pool(settings)

    _, kwargs = mock_connect.call_args
    assert kwargs["auth_mechanism"] == "GSSAPI"
    assert kwargs["kerberos_service_name"] == "impala"


# ---------------------------------------------------------------------------
# test_context_manager_releases_on_success
# ---------------------------------------------------------------------------

def test_context_manager_releases_on_success():
    """connection() context manager returns connection to pool after successful block."""
    mock_conn = MagicMock()
    settings = _make_settings(impala_pool_min=1, impala_pool_max=5)

    with patch("app.connection_pool.impala_dbapi.connect", return_value=mock_conn):
        pool = _make_pool(settings)

    with patch.object(pool, "_validate_connection", return_value=True):
        with pool.connection() as conn:
            assert conn is mock_conn
            assert pool._pool.qsize() == 0  # held during block

    assert pool._pool.qsize() == 1  # returned after block


# ---------------------------------------------------------------------------
# test_context_manager_releases_on_exception
# ---------------------------------------------------------------------------

def test_context_manager_releases_on_exception():
    """connection() context manager still releases connection even when block raises."""
    mock_conn = MagicMock()
    settings = _make_settings(impala_pool_min=1, impala_pool_max=5)

    with patch("app.connection_pool.impala_dbapi.connect", return_value=mock_conn):
        pool = _make_pool(settings)

    with patch.object(pool, "_validate_connection", return_value=True):
        with pytest.raises(ValueError):
            with pool.connection():
                raise ValueError("something went wrong")

    assert pool._pool.qsize() == 1  # connection was released despite exception
