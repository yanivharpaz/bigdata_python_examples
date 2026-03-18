"""
Query executor: runs SQL against Impala via the connection pool.

Maps cursor description tuples to ColumnMeta objects and rows to dicts.
"""

import logging
from typing import TYPE_CHECKING

from app.models import ColumnMeta, QueryResponse

if TYPE_CHECKING:
    from app.connection_pool import ImpalaConnectionPool

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes SQL queries against Impala using the connection pool."""

    def __init__(self, pool: "ImpalaConnectionPool") -> None:
        self._pool = pool

    # ── Public API ────────────────────────────────────────────────────────────

    def execute(self, sql: str, timeout: float | None = None) -> QueryResponse:
        """
        Execute a SQL query and return structured results.

        Args:
            sql: The SQL string to execute.
            timeout: Optional per-query timeout in seconds.

        Returns:
            QueryResponse with columns, rows, and row_count.

        Raises:
            TimeoutError: if the query exceeds the timeout.
            Exception: propagates Impala-side errors to the caller.
        """
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            try:
                if timeout is not None:
                    cursor.execute(sql, timeout=timeout)
                else:
                    cursor.execute(sql)

                columns = self._map_columns(cursor.description or [])
                col_names = [c.name for c in columns]
                raw_rows = cursor.fetchall() or []
                rows = [dict(zip(col_names, row)) for row in raw_rows]

                return QueryResponse(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                )
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

    def list_databases(self) -> list[str]:
        """Return a list of available Impala databases (Req 4.5)."""
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SHOW DATABASES")
                return [row[0] for row in cursor.fetchall()]
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

    def list_tables(self, database: str) -> list[str]:
        """Return a list of tables in the given database (Req 4.6)."""
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SHOW TABLES IN {database}")
                return [row[0] for row in cursor.fetchall()]
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

    def ping(self, timeout: float = 3.0) -> bool:
        """
        Lightweight connectivity check for health endpoint.

        Tries to acquire from the pool first; if the pool is empty (e.g. warmup
        failed because the DB wasn't ready yet), falls back to creating a fresh
        connection directly so the health check reflects current reachability.
        Returns False on any failure.
        """
        conn = None
        from_pool = True
        try:
            conn = self._pool.acquire(timeout=timeout)
        except Exception:
            # Pool empty — try a direct connection so health reflects live state
            from_pool = False
            try:
                conn = self._pool._create_connection()
            except Exception:
                return False

        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1")
                return True
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        except Exception:
            return False
        finally:
            if conn is not None:
                if from_pool:
                    self._pool.release(conn)
                else:
                    # Return the fresh connection to the pool if there's room
                    try:
                        self._pool._pool.put_nowait(conn)
                    except Exception:
                        try:
                            conn.close()
                        except Exception:
                            pass

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _map_columns(description: list) -> list[ColumnMeta]:
        """
        Map a DB-API cursor description to a list of ColumnMeta objects.

        Each entry in description is a 7-tuple:
          (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return [
            ColumnMeta(name=col[0], type=str(col[1]) if col[1] is not None else "unknown")
            for col in description
        ]
