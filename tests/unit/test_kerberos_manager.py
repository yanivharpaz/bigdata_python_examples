"""
Unit tests for app/kerberos_manager.py.

Requirements covered:
  - 1.1: Authenticate with KDC using keytab at startup
  - 1.3: Missing keytab raises FileNotFoundError with descriptive message
  - 1.4: Retry up to krb5_max_retry times on failure; mark unhealthy after all fail
"""

import subprocess
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from app.config import Settings
from app.kerberos_manager import KerberosManager


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
        krb5_auth_mode="keytab",
        krb5_renewal_threshold_seconds=300,
        krb5_max_retry=3,
        impala_host="impala.example.com",
    )
    defaults.update(overrides)
    return Settings.model_construct(**defaults)


def _klist_output(expiry: datetime) -> str:
    """Build a realistic klist stdout string with the given expiry datetime."""
    issued = expiry - timedelta(hours=10)
    fmt = "%m/%d/%Y %H:%M:%S"
    issued_str = issued.strftime(fmt)
    expiry_str = expiry.strftime(fmt)
    return (
        "Credentials cache: API:...\n"
        "        Principal: svc@EXAMPLE.COM\n"
        "\n"
        "  Issued                Expires               Principal\n"
        f"{issued_str}  {expiry_str}  krbtgt/EXAMPLE.COM@EXAMPLE.COM\n"
    )


# ---------------------------------------------------------------------------
# authenticate() — keytab mode
# ---------------------------------------------------------------------------

def test_authenticate_keytab_success():
    """Req 1.1: keytab mode runs kinit successfully with no exception."""
    settings = _make_settings()
    manager = KerberosManager(settings)

    with patch("os.path.exists", return_value=True), \
         patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        manager.authenticate()  # must not raise

    mock_run.assert_called_once_with(
        ["kinit", "-kt", settings.krb5_keytab_path, settings.krb5_principal],
        check=True,
        capture_output=True,
    )


def test_authenticate_keytab_missing_file():
    """Req 1.3: FileNotFoundError with descriptive message when keytab is absent."""
    settings = _make_settings(krb5_keytab_path="/nonexistent/svc.keytab")
    manager = KerberosManager(settings)

    with patch("os.path.exists", return_value=False):
        with pytest.raises(FileNotFoundError) as exc_info:
            manager.authenticate()

    assert "/nonexistent/svc.keytab" in str(exc_info.value)


def test_authenticate_keytab_kinit_failure():
    """Req 1.1: CalledProcessError from kinit propagates to the caller."""
    settings = _make_settings()
    manager = KerberosManager(settings)

    error = subprocess.CalledProcessError(1, "kinit", stderr=b"KDC unreachable")

    with patch("os.path.exists", return_value=True), \
         patch("subprocess.run", side_effect=error):
        with pytest.raises(subprocess.CalledProcessError):
            manager.authenticate()


# ---------------------------------------------------------------------------
# authenticate() — kinit mode
# ---------------------------------------------------------------------------

def test_authenticate_kinit_mode_success():
    """kinit mode: klist -s succeeds → no exception."""
    settings = _make_settings(krb5_auth_mode="kinit")
    manager = KerberosManager(settings)

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        manager.authenticate()  # must not raise

    mock_run.assert_called_once_with(["klist", "-s"], check=True, capture_output=True)


def test_authenticate_kinit_mode_no_tgt():
    """kinit mode: klist -s fails → RuntimeError about missing TGT."""
    settings = _make_settings(krb5_auth_mode="kinit")
    manager = KerberosManager(settings)

    error = subprocess.CalledProcessError(1, "klist")

    with patch("subprocess.run", side_effect=error):
        with pytest.raises(RuntimeError) as exc_info:
            manager.authenticate()

    assert "TGT" in str(exc_info.value) or "kinit" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# renew_if_needed() — retry logic
# ---------------------------------------------------------------------------

def test_renew_if_needed_retries_on_failure():
    """Req 1.4: authenticate() called exactly krb5_max_retry times; _healthy set False."""
    settings = _make_settings(krb5_max_retry=3)
    manager = KerberosManager(settings)

    with patch.object(manager, "is_tgt_valid", return_value=False), \
         patch.object(manager, "authenticate", side_effect=RuntimeError("kinit failed")) as mock_auth:
        with pytest.raises(RuntimeError):
            manager.renew_if_needed()

    assert mock_auth.call_count == 3
    assert manager._healthy is False


def test_renew_if_needed_succeeds_on_second_attempt():
    """Req 1.4: _healthy stays True when authenticate() succeeds after one failure."""
    settings = _make_settings(krb5_max_retry=3)
    manager = KerberosManager(settings)

    # Fail once, then succeed
    side_effects = [RuntimeError("first failure"), None]

    with patch.object(manager, "is_tgt_valid", return_value=False), \
         patch.object(manager, "authenticate", side_effect=side_effects) as mock_auth:
        manager.renew_if_needed()

    assert mock_auth.call_count == 2
    assert manager._healthy is True


# ---------------------------------------------------------------------------
# is_tgt_valid()
# ---------------------------------------------------------------------------

def test_is_tgt_valid_returns_false_when_no_tgt():
    """is_tgt_valid() returns False when klist exits non-zero (no TGT present)."""
    settings = _make_settings()
    manager = KerberosManager(settings)

    mock_result = MagicMock(returncode=1, stdout="")
    with patch("subprocess.run", return_value=mock_result):
        assert manager.is_tgt_valid() is False


def test_is_tgt_valid_returns_false_when_within_threshold():
    """Req 1.2: returns False when TGT expires in 60s but threshold is 300s."""
    settings = _make_settings(krb5_renewal_threshold_seconds=300)
    manager = KerberosManager(settings)

    expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
    mock_result = MagicMock(returncode=0, stdout=_klist_output(expiry))

    with patch("subprocess.run", return_value=mock_result):
        assert manager.is_tgt_valid() is False


def test_is_tgt_valid_returns_true_when_outside_threshold():
    """is_tgt_valid() returns True when TGT expires in 600s and threshold is 300s."""
    settings = _make_settings(krb5_renewal_threshold_seconds=300)
    manager = KerberosManager(settings)

    expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=600)
    mock_result = MagicMock(returncode=0, stdout=_klist_output(expiry))

    with patch("subprocess.run", return_value=mock_result):
        assert manager.is_tgt_valid() is True
