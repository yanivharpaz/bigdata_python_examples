"""
FastAPI router with all four API endpoints.

Endpoints:
  POST /query                        – execute a SQL query
  GET  /health                       – service health check
  GET  /databases                    – list Impala databases
  GET  /databases/{database}/tables  – list tables in a database
"""

import logging
import queue

from fastapi import APIRouter, Depends, HTTPException, Request

from app.models import (
    DatabasesResponse,
    HealthResponse,
    QueryRequest,
    QueryResponse,
    TablesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Dependency helpers – pull shared objects from app.state
# ---------------------------------------------------------------------------

def _get_executor(request: Request):
    return request.app.state.executor


def _get_krb_manager(request: Request):
    return request.app.state.krb_manager


# ---------------------------------------------------------------------------
# POST /query
# ---------------------------------------------------------------------------

@router.post("/query", response_model=QueryResponse, status_code=200)
def run_query(
    body: QueryRequest,
    executor=Depends(_get_executor),
):
    """Execute a SQL query against Impala (Req 3.1–3.6)."""
    # Reject whitespace-only SQL (Req 3.6 / 4.3)
    if not body.sql.strip():
        raise HTTPException(status_code=422, detail="sql must not be blank or whitespace-only")

    try:
        return executor.execute(body.sql)
    except queue.Empty:
        raise HTTPException(status_code=503, detail="Connection pool exhausted")
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Query timed out")
    except Exception as exc:
        # Treat any Impala-side error as a 400
        logger.warning("Impala query error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse, status_code=200)
def health_check(
    executor=Depends(_get_executor),
    krb_manager=Depends(_get_krb_manager),
):
    """Return health of Kerberos and Impala components (Req 4.1–4.2)."""
    kerberos_status = "ok" if krb_manager.is_tgt_valid() else "error"

    impala_status = "ok"
    if not executor.ping(timeout=3.0):
        impala_status = "error"

    overall = "ok" if kerberos_status == "ok" and impala_status == "ok" else "degraded"

    return HealthResponse(
        status=overall,
        kerberos=kerberos_status,
        impala=impala_status,
    )


# ---------------------------------------------------------------------------
# GET /databases
# ---------------------------------------------------------------------------

@router.get("/databases", response_model=DatabasesResponse, status_code=200)
def list_databases(executor=Depends(_get_executor)):
    """List all Impala databases (Req 4.4–4.5)."""
    try:
        return DatabasesResponse(databases=executor.list_databases())
    except queue.Empty:
        raise HTTPException(status_code=503, detail="Connection pool exhausted")
    except Exception as exc:
        logger.warning("list_databases error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /databases/{database}/tables
# ---------------------------------------------------------------------------

@router.get("/databases/{database}/tables", response_model=TablesResponse, status_code=200)
def list_tables(database: str, executor=Depends(_get_executor)):
    """List tables in a given Impala database (Req 4.6)."""
    try:
        return TablesResponse(tables=executor.list_tables(database))
    except queue.Empty:
        raise HTTPException(status_code=503, detail="Connection pool exhausted")
    except Exception as exc:
        logger.warning("list_tables error for db '%s': %s", database, exc)
        raise HTTPException(status_code=400, detail=str(exc))
