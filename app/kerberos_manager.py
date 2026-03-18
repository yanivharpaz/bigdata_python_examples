"""
Kerberos TGT lifecycle management.

Handles obtaining and renewing Kerberos tickets via kinit (keytab or
pre-existing TGT) and runs a background asyncio renewal loop.
"""

import asyncio
import logging
import os
import re
import subprocess
from datetime import datetime, timezone

from app.config import Settings

logger = logging.getLogger(__name__)

# klist date formats vary by platform/version; try both common patterns.
_KLIST_DATE_PATTERNS = [
    # "MM/DD/YYYY HH:MM:SS"  (MIT Kerberos on Linux, 4-digit year)
    re.compile(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"),
    # "MM/DD/YY HH:MM:SS"   (MIT Kerberos on Debian/Ubuntu, 2-digit year)
    re.compile(r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})"),
    # "DD-Mon-YYYY HH:MM:SS" (some macOS / Heimdal variants)
    re.compile(r"(\d{2}-\w{3}-\d{4}\s+\d{2}:\d{2}:\d{2})"),
]
_KLIST_DATE_FMTS = ["%m/%d/%Y %H:%M:%S", "%m/%d/%y %H:%M:%S", "%d-%b-%Y %H:%M:%S"]


def _parse_tgt_expiry(klist_output: str) -> datetime | None:
    """
    Extract the TGT expiry datetime from ``klist`` stdout.

    klist prints one credential per line; the TGT line contains the
    krbtgt/<REALM>@<REALM> service name.  We look for that line and
    parse the second date field (expiry).  Falls back to the first
    matching date pair found in the output if the krbtgt line is not
    identifiable.
    """
    for line in klist_output.splitlines():
        # Prefer the krbtgt line for accuracy
        if "krbtgt/" not in line and line == klist_output.splitlines()[0]:
            # skip header lines that don't contain credentials
            continue
        for pattern in _KLIST_DATE_PATTERNS:
            matches = pattern.findall(line)
            if len(matches) >= 2:
                # matches[0] = issue time, matches[1] = expiry time
                expiry_str = matches[1]
                for fmt in _KLIST_DATE_FMTS:
                    try:
                        return datetime.strptime(expiry_str, fmt).replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        continue
    # Fallback: grab the first pair of dates anywhere in the output
    for pattern in _KLIST_DATE_PATTERNS:
        matches = pattern.findall(klist_output)
        if len(matches) >= 2:
            expiry_str = matches[1]
            for fmt in _KLIST_DATE_FMTS:
                try:
                    return datetime.strptime(expiry_str, fmt).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    continue
    return None


class KerberosManager:
    """Manages Kerberos TGT acquisition and renewal."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._healthy: bool = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def authenticate(self) -> None:
        """Obtain a TGT.

        - keytab mode: runs ``kinit -kt <keytab> <principal>``
        - kinit mode:  verifies an existing TGT is present in the cache

        Raises:
            FileNotFoundError: keytab file does not exist (keytab mode only).
            subprocess.CalledProcessError: kinit/klist exited non-zero.
            RuntimeError: no valid TGT found in credential cache (kinit mode).
        """
        if self._settings.krb5_auth_mode == "keytab":
            self._authenticate_keytab()
        else:
            self._authenticate_kinit()

    def is_tgt_valid(self) -> bool:
        """Return True if a TGT exists and is not within the renewal threshold.

        Runs ``klist`` and parses the expiry time.  Returns False on any
        error (missing TGT, parse failure, etc.).
        """
        try:
            result = subprocess.run(
                ["klist"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.debug("klist returned non-zero; no TGT present.")
                return False

            expiry = _parse_tgt_expiry(result.stdout)
            if expiry is None:
                logger.warning("Could not parse TGT expiry from klist output.")
                return False

            now = datetime.now(tz=timezone.utc)
            time_to_expiry = (expiry - now).total_seconds()
            threshold = self._settings.krb5_renewal_threshold_seconds
            valid = time_to_expiry > threshold
            logger.debug(
                "TGT expiry in %.0f s (threshold %d s) → valid=%s",
                time_to_expiry,
                threshold,
                valid,
            )
            return valid
        except Exception as exc:  # noqa: BLE001
            logger.warning("is_tgt_valid() encountered an error: %s", exc)
            return False

    def renew_if_needed(self) -> None:
        """Renew the TGT if it is within the expiry threshold.

        Retries up to ``krb5_max_retry`` times on failure.  Sets
        ``self._healthy`` accordingly and re-raises the last exception
        when all retries are exhausted.

        Raises:
            Exception: the last authentication error after all retries fail.
        """
        if self.is_tgt_valid():
            return

        last_exc: Exception | None = None
        for attempt in range(1, self._settings.krb5_max_retry + 1):
            try:
                logger.info(
                    "TGT renewal attempt %d/%d …",
                    attempt,
                    self._settings.krb5_max_retry,
                )
                self.authenticate()
                self._healthy = True
                logger.info("TGT renewed successfully.")
                return
            except Exception as exc:  # noqa: BLE001
                logger.error("TGT renewal attempt %d failed: %s", attempt, exc)
                last_exc = exc

        self._healthy = False
        logger.error(
            "All %d TGT renewal attempts failed; marking service unhealthy.",
            self._settings.krb5_max_retry,
        )
        raise last_exc  # type: ignore[misc]

    def start_renewal_loop(self) -> asyncio.Task:  # type: ignore[type-arg]
        """Schedule a background asyncio task that periodically renews the TGT.

        The loop runs ``renew_if_needed()`` in a thread executor (because
        subprocess calls are blocking) every
        ``krb5_renewal_threshold_seconds // 2`` seconds.

        Returns:
            The created :class:`asyncio.Task`.
        """
        loop = asyncio.get_event_loop()
        task = loop.create_task(self._renewal_loop())
        logger.info("Kerberos renewal loop started.")
        return task

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _authenticate_keytab(self) -> None:
        keytab = self._settings.krb5_keytab_path
        principal = self._settings.krb5_principal

        if not os.path.exists(keytab):
            raise FileNotFoundError(
                f"Kerberos keytab file not found: '{keytab}'. "
                "Ensure the KRB5_KEYTAB_PATH environment variable points to a "
                "valid keytab file."
            )

        logger.info("Running kinit with keytab '%s' for principal '%s'.", keytab, principal)
        try:
            subprocess.run(
                ["kinit", "-kt", keytab, principal],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode(errors="replace") if exc.stderr else ""
            logger.error("kinit failed (exit %d): %s", exc.returncode, stderr)
            raise

    def _authenticate_kinit(self) -> None:
        logger.info("Verifying existing TGT in credential cache (kinit mode).")
        try:
            subprocess.run(
                ["klist", "-s"],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError:
            raise RuntimeError(
                "No valid TGT found in credential cache. "
                "Run 'kinit' manually before starting the service in kinit mode."
            )

    async def _renewal_loop(self) -> None:
        interval = max(1, self._settings.krb5_renewal_threshold_seconds // 2)
        loop = asyncio.get_event_loop()
        while True:
            try:
                await loop.run_in_executor(None, self.renew_if_needed)
            except Exception as exc:  # noqa: BLE001
                # Already logged inside renew_if_needed; don't crash the loop.
                logger.error("Renewal loop caught unhandled exception: %s", exc)
            await asyncio.sleep(interval)
