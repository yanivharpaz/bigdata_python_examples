"""
Unit tests for app/query_executor.py.

Requirements covered:
  - 3.1: Execute query and return JSON array of objects
  - 3.2: Empty result set returns empty rows with column metadata
  - 3.3: Timeout raises TimeoutError
  - 3.4: Impala error is propagated
  - 3.5: Column names and types included in response
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from app.query_executor import QueryExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(description=None, rows=None):
    """Build a mock cursor with given description and fetchall rows."""
    cursor = MagicMock()
    cursor.description = description
    cursor.fetchall.return_value = rows or []
    return cursor


def _make_pool(cursor):
    """Build a mock pool whose connection() context manager yields a conn with cursor."""
    conn = MagicMock()
    conn.cursor.return_value = cursor

    pool = MagicMock()

    @contextmanager
    def _connection():
        yield conn

    pool.connection = _connection
    return pool


# ---------------------------------------------------------------------------
# test_execute_maps_rows_and_columns
# ---------------------------------------------------------------------------

def test_execute_maps_rows_and_columns():
    """Req 3.1, 3.5: rows are dicts keyed by column name; columns include name+type."""
    description = [
        ("id", "int", None, None, None, None, None),
        ("name", "string", None, None, None, None, None),
    ]
    rows = [(1, "alice"), (2, "bob")]
    cursor = _make_cursor(description=description, rows=rows)
    executor = QueryExecutor(_make_pool(cursor))

    result = executor.execute("SELECT id, name FROM t")

    assert result.row_count == 2
    assert result.columns[0].name == "id"
    assert result.columns[0].type == "int"
    assert result.columns[1].name == "name"
    assert result.rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


# ---------------------------------------------------------------------------
# test_execute_empty_result_set
# ---------------------------------------------------------------------------

def test_execute_empty_result_set():
    """Req 3.2: empty result returns empty rows but column metadata is preserved."""
    description = [
        ("id", "int", None, None, None, None, None),
    ]
    cursor = _make_cursor(description=description, rows=[])
    executor = QueryExecutor(_make_pool(cursor))

    result = executor.execute("SELECT id FROM t WHERE 1=0")

    assert result.row_count == 0
    assert result.rows == []
    assert len(result.columns) == 1
    assert result.columns[0].name == "id"


# ---------------------------------------------------------------------------
# test_execute_none_description_returns_empty_columns
# ---------------------------------------------------------------------------

def test_execute_none_description_returns_empty_columns():
    """Edge case: cursor.description is None (e.g. DDL statement)."""
    cursor = _make_cursor(description=None, rows=[])
    executor = QueryExecutor(_make_pool(cursor))

    result = executor.execute("CREATE TABLE t (id INT)")

    assert result.columns == []
    assert result.rows == []
    assert result.row_count == 0


# ---------------------------------------------------------------------------
# test_execute_timeout_propagated
# ---------------------------------------------------------------------------

def test_execute_timeout_propagated():
    """Req 3.3: TimeoutError from cursor.execute is propagated to caller."""
    cursor = MagicMock()
    cursor.execute.side_effect = TimeoutError("query timed out")
    executor = QueryExecutor(_make_pool(cursor))

    with pytest.raises(TimeoutError):
        executor.execute("SELECT 1", timeout=1)


# ---------------------------------------------------------------------------
# test_execute_impala_error_propagated
# ---------------------------------------------------------------------------

def test_execute_impala_error_propagated():
    """Req 3.4: Impala-side errors are propagated to the caller."""
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("AnalysisException: table not found")
    executor = QueryExecutor(_make_pool(cursor))

    with pytest.raises(Exception, match="AnalysisException"):
        executor.execute("SELECT * FROM nonexistent")


# ---------------------------------------------------------------------------
# test_execute_passes_timeout_to_cursor
# ---------------------------------------------------------------------------

def test_execute_passes_timeout_to_cursor():
    """Req 3.3: timeout value is forwarded to cursor.execute."""
    description = [("x", "int", None, None, None, None, None)]
    cursor = _make_cursor(description=description, rows=[(42,)])
    executor = QueryExecutor(_make_pool(cursor))

    executor.execute("SELECT 42", timeout=30)

    cursor.execute.assert_called_once_with("SELECT 42", timeout=30)


# ---------------------------------------------------------------------------
# test_list_databases
# ---------------------------------------------------------------------------

def test_list_databases():
    """Req 4.5: list_databases returns a flat list of database name strings."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("default",), ("analytics",), ("raw",)]
    executor = QueryExecutor(_make_pool(cursor))

    dbs = executor.list_databases()

    assert dbs == ["default", "analytics", "raw"]
    cursor.execute.assert_called_once_with("SHOW DATABASES")


# ---------------------------------------------------------------------------
# test_list_tables
# ---------------------------------------------------------------------------

def test_list_tables():
    """Req 4.6: list_tables returns a flat list of table name strings."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("events",), ("users",)]
    executor = QueryExecutor(_make_pool(cursor))

    tables = executor.list_tables("analytics")

    assert tables == ["events", "users"]
    cursor.execute.assert_called_once_with("SHOW TABLES IN analytics")
