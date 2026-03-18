"""
Application entry point.

Creates the FastAPI app, wires up shared state (Settings, KerberosManager,
ImpalaConnectionPool, QueryExecutor), and manages startup/shutdown lifecycle.
"""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import ValidationError

from app.api import router
from app.config import Settings
from app.connection_pool import ImpalaConnectionPool
from app.kerberos_manager import KerberosManager
from app.query_executor import QueryExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: authenticate, warm pool, start renewal loop.
    Shutdown: drain connection pool."""

    # ── Startup ──────────────────────────────────────────────────────────────
    try:
        settings = Settings()
    except ValidationError as exc:
        logger.critical("Invalid configuration — cannot start: %s", exc)
        sys.exit(1)

    try:
        krb_manager = KerberosManager(settings)
        krb_manager.authenticate()
    except FileNotFoundError as exc:
        logger.critical("Keytab file not found — cannot start: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.critical("Kerberos authentication failed — cannot start: %s", exc)
        sys.exit(1)

    try:
        pool = ImpalaConnectionPool(settings, krb_manager)
    except Exception as exc:
        logger.critical("Failed to initialise connection pool — cannot start: %s", exc)
        sys.exit(1)

    executor = QueryExecutor(pool)
    renewal_task = krb_manager.start_renewal_loop()

    # Attach shared objects to app state so endpoints can access them
    app.state.settings = settings
    app.state.krb_manager = krb_manager
    app.state.pool = pool
    app.state.executor = executor

    logger.info("Startup complete — service is ready.")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    renewal_task.cancel()

    # Drain the connection pool
    drained = 0
    while not pool._pool.empty():
        try:
            conn = pool._pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass
            drained += 1
        except Exception:
            break

    logger.info("Shutdown complete — drained %d connection(s).", drained)


app = FastAPI(
    title="Kerberos-Impala Backend",
    description="FastAPI service for executing Impala queries with Kerberos authentication.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)
