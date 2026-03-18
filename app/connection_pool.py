"""
Impala connection pool using a thread-safe queue.

Manages a bounded pool of GSSAPI-authenticated impyla connections.
Connections are validated on release and replaced if stale.
"""

import logging
import queue
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

try:
    import impala.dbapi as impala_dbapi
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "impyla is required but not installed. "
        "Run: pip install impyla"
    ) from exc

from app.config import Settings

if TYPE_CHECKING:
    from app.kerberos_manager import KerberosManager

logger = logging.getLogger(__name__)


class ImpalaConnectionPool:
    """Thread-safe pool of impyla connections authenticated via GSSAPI."""

    def __init__(self, settings: Settings, krb_manager: "KerberosManager") -> None:
        self._settings = settings
        self._krb_manager = krb_manager
        self._pool: queue.Queue = queue.Queue(maxsize=settings.impala_pool_max)
        self._warm_pool()

    # ── Public API ────────────────────────────────────────────────────────────

    def acquire(self, timeout: float | None = None):
        """
        Get a connection from the pool, blocking up to timeout seconds
        (defaults to impala_query_timeout).

        Raises:
            queue.Empty: when no connection becomes available within the timeout.
                         Callers should map this to a 503 response.
        """
        t = timeout if timeout is not None else self._settings.impala_query_timeout
        return self._pool.get(timeout=t)

    def release(self, conn) -> None:
        """
        Return a connection to the pool.

        Validates the connection first; replaces it with a fresh one if stale.
        Silently discards the connection if the pool is already full.
        """
        if self._validate_connection(conn):
            healthy = conn
        else:
            logger.warning("Stale connection detected; replacing with a new one.")
            try:
                conn.close()
            except Exception:
                pass
            try:
                healthy = self._create_connection()
            except Exception:
                logger.error("Failed to create replacement connection; discarding slot.")
                return

        try:
            self._pool.put_nowait(healthy)
        except queue.Full:
            logger.debug("Pool is full; discarding extra connection.")
            try:
                healthy.close()
            except Exception:
                pass

    @contextmanager
    def connection(self) -> Generator:
        """Context manager: acquire → yield → release."""
        conn = self.acquire()
        try:
            yield conn
        finally:
            self.release(conn)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _create_connection(self):
        """Create a new impyla connection using the configured auth mechanism (Req 2.1, 2.5)."""
        s = self._settings
        logger.debug(
            "Creating impyla connection to %s:%s (database=%s, tls=%s, auth=%s)",
            s.impala_host, s.impala_port, s.impala_database, s.impala_use_tls,
            s.impala_auth_mechanism,
        )
        kwargs = dict(
            host=s.impala_host,
            port=s.impala_port,
            database=s.impala_database,
            auth_mechanism=s.impala_auth_mechanism,
            use_ssl=s.impala_use_tls,
            ca_cert=s.impala_ca_cert,
        )
        if s.impala_auth_mechanism == "GSSAPI":
            kwargs["kerberos_service_name"] = "impala"
        return impala_dbapi.connect(**kwargs)

    def _validate_connection(self, conn) -> bool:
        """Ping the connection by executing SELECT 1. Returns True if healthy."""
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return True
        except Exception as exc:
            logger.debug("Connection validation failed: %s", exc)
            return False
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

    def _warm_pool(self) -> None:
        """
        Pre-populate the pool with impala_pool_min connections (Req 2.2, 2.6).

        Retries each connection attempt up to krb5_max_retry times before
        logging an error and moving on.
        """
        s = self._settings
        for i in range(s.impala_pool_min):
            conn = self._create_connection_with_retry()
            if conn is not None:
                self._pool.put_nowait(conn)
            else:
                logger.error(
                    "Could not establish warm connection %d/%d after %d retries.",
                    i + 1, s.impala_pool_min, s.krb5_max_retry,
                )

    def _create_connection_with_retry(self):
        """Attempt to create a connection, retrying up to krb5_max_retry times."""
        s = self._settings
        last_exc = None
        for attempt in range(1, s.krb5_max_retry + 1):
            try:
                return self._create_connection()
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Connection attempt %d/%d failed: %s",
                    attempt, s.krb5_max_retry, exc,
                )
        logger.error(
            "All %d connection attempts failed. Last error: %s",
            s.krb5_max_retry, last_exc,
        )
        return None
