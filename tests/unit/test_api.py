"""
Unit tests for app/api.py endpoints.

Uses httpx.AsyncClient with ASGITransport (compatible with httpx >= 0.20).
Tests are run synchronously via anyio.run().
"""

import queue
from unittest.mock import MagicMock

import anyio
import httpx
import pytest
from fastapi import FastAPI

from app.api import router
from app.models import ColumnMeta, QueryResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_app(mock_executor, mock_krb_manager):
    app = FastAPI()
    app.include_router(router)
    app.state.executor = mock_executor
    app.state.krb_manager = mock_krb_manager
    return app


def call(app, method: str, path: str, **kwargs) -> httpx.Response:
    """Run a single async HTTP call against the ASGI app synchronously."""
    async def _run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            return await getattr(client, method)(path, **kwargs)
    return anyio.run(_run)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_executor():
    return MagicMock()


@pytest.fixture()
def mock_krb_manager():
    m = MagicMock()
    m.is_tgt_valid.return_value = True
    return m


# ---------------------------------------------------------------------------
# POST /query
# ---------------------------------------------------------------------------

def test_query_valid_sql_returns_200(mock_executor, mock_krb_manager):
    mock_executor.execute.return_value = QueryResponse(
        columns=[ColumnMeta(name="id", type="int")],
        rows=[{"id": 1}],
        row_count=1,
    )
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 1
    assert data["columns"][0]["name"] == "id"


def test_query_missing_sql_field_returns_422(mock_executor, mock_krb_manager):
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={})
    assert resp.status_code == 422


def test_query_empty_sql_returns_422(mock_executor, mock_krb_manager):
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": ""})
    assert resp.status_code == 422


def test_query_whitespace_only_sql_returns_422(mock_executor, mock_krb_manager):
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": "   \t\n  "})
    assert resp.status_code == 422
    mock_executor.execute.assert_not_called()


def test_query_timeout_returns_408(mock_executor, mock_krb_manager):
    mock_executor.execute.side_effect = TimeoutError("query timed out")
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": "SELECT SLEEP(999)"})
    assert resp.status_code == 408


def test_query_impala_error_returns_400(mock_executor, mock_krb_manager):
    mock_executor.execute.side_effect = Exception("Impala error: table not found")
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": "SELECT * FROM missing_table"})
    assert resp.status_code == 400


def test_query_pool_exhausted_returns_503(mock_executor, mock_krb_manager):
    mock_executor.execute.side_effect = queue.Empty()
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "post", "/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

def test_health_both_ok(mock_executor, mock_krb_manager):
    mock_krb_manager.is_tgt_valid.return_value = True
    mock_executor.ping.return_value = True
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["kerberos"] == "ok"
    assert data["impala"] == "ok"


def test_health_kerberos_error(mock_executor, mock_krb_manager):
    mock_krb_manager.is_tgt_valid.return_value = False
    mock_executor.ping.return_value = True
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["kerberos"] == "error"
    assert data["impala"] == "ok"


def test_health_impala_error(mock_executor, mock_krb_manager):
    mock_krb_manager.is_tgt_valid.return_value = True
    mock_executor.ping.return_value = False
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["kerberos"] == "ok"
    assert data["impala"] == "error"


def test_health_both_error(mock_executor, mock_krb_manager):
    mock_krb_manager.is_tgt_valid.return_value = False
    mock_executor.ping.return_value = False
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["kerberos"] == "error"
    assert data["impala"] == "error"


# ---------------------------------------------------------------------------
# GET /databases
# ---------------------------------------------------------------------------

def test_list_databases_returns_200(mock_executor, mock_krb_manager):
    mock_executor.list_databases.return_value = ["default", "analytics"]
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/databases")
    assert resp.status_code == 200
    assert resp.json()["databases"] == ["default", "analytics"]


def test_list_databases_pool_exhausted_returns_503(mock_executor, mock_krb_manager):
    mock_executor.list_databases.side_effect = queue.Empty()
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/databases")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET /databases/{database}/tables
# ---------------------------------------------------------------------------

def test_list_tables_returns_200(mock_executor, mock_krb_manager):
    mock_executor.list_tables.return_value = ["orders", "users"]
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/databases/analytics/tables")
    assert resp.status_code == 200
    assert resp.json()["tables"] == ["orders", "users"]
    mock_executor.list_tables.assert_called_once_with("analytics")


def test_list_tables_pool_exhausted_returns_503(mock_executor, mock_krb_manager):
    mock_executor.list_tables.side_effect = queue.Empty()
    app = make_app(mock_executor, mock_krb_manager)
    resp = call(app, "get", "/databases/analytics/tables")
    assert resp.status_code == 503
