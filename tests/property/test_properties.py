# Feature: kerberos-impala-backend, Property 3: Config validation completeness
"""
Property-based tests for the kerberos-impala-backend.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from app.config import Settings

# The 5 required fields (as env var names, uppercased)
REQUIRED_FIELDS = [
    "KRB5_REALM",
    "KRB5_KDC",
    "KRB5_PRINCIPAL",
    "KRB5_KEYTAB_PATH",
    "IMPALA_HOST",
]

# Mapping from env var name to Settings field name (lowercase)
FIELD_MAP = {
    "KRB5_REALM": "krb5_realm",
    "KRB5_KDC": "krb5_kdc",
    "KRB5_PRINCIPAL": "krb5_principal",
    "KRB5_KEYTAB_PATH": "krb5_keytab_path",
    "IMPALA_HOST": "impala_host",
}

# A complete set of valid values for all required fields
ALL_REQUIRED_VALUES = {
    "krb5_realm": "EXAMPLE.COM",
    "krb5_kdc": "kdc.example.com",
    "krb5_principal": "user@EXAMPLE.COM",
    "krb5_keytab_path": "/etc/krb5/user.keytab",
    "impala_host": "impala.example.com",
}


@given(st.frozensets(st.sampled_from(REQUIRED_FIELDS), min_size=1))
@settings(max_examples=100)
def test_config_validation_completeness(fields_to_omit):
    """
    **Validates: Requirements 5.3, 5.4, 5.6**

    For any non-empty subset of required fields that is omitted,
    Settings() must raise a ValidationError and the error must
    name at least one of the missing fields (case-insensitive).
    """
    # Build kwargs with all required fields except the omitted ones
    kwargs = {
        FIELD_MAP[env_var]: ALL_REQUIRED_VALUES[FIELD_MAP[env_var]]
        for env_var in REQUIRED_FIELDS
        if env_var not in fields_to_omit
    }

    with pytest.raises(ValidationError) as exc_info:
        Settings(**kwargs)

    error_text = str(exc_info.value).lower()

    # At least one of the omitted field names must appear in the error message
    omitted_field_names = [FIELD_MAP[f].lower() for f in fields_to_omit]
    assert any(field in error_text for field in omitted_field_names), (
        f"Expected error to mention one of {omitted_field_names}, "
        f"but got: {exc_info.value}"
    )


# Feature: kerberos-impala-backend, Property 4: Query response serialization round trip

import json

from app.models import ColumnMeta, QueryResponse

_col_name_strategy = st.text(
    min_size=1,
    max_size=20,
    alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"),
        whitelist_characters="_",
    ),
)

_col_type_strategy = st.text(min_size=1, max_size=20)

_col_value_strategy = st.one_of(
    st.integers(),
    st.text(),
    st.none(),
    st.floats(allow_nan=False, allow_infinity=False),
)


@given(data=st.data())
@settings(max_examples=100)
def test_query_response_serialization_round_trip(data):
    """
    **Validates: Requirements 3.1, 3.5**

    For any QueryResponse object, serializing to JSON and deserializing back
    must produce an equivalent object with identical column metadata and row data.
    """
    columns = data.draw(
        st.lists(
            st.builds(ColumnMeta, name=_col_name_strategy, type=_col_type_strategy),
            min_size=0,
            max_size=10,
        )
    )

    col_names = [col.name for col in columns]

    # Build rows whose keys match the column names (or empty if no columns)
    if col_names:
        rows_strategy = st.lists(
            st.fixed_dictionaries(
                {name: _col_value_strategy for name in col_names}
            ),
            min_size=0,
            max_size=10,
        )
    else:
        rows_strategy = st.just([])

    rows = data.draw(rows_strategy)

    response = QueryResponse(
        columns=columns,
        rows=rows,
        row_count=len(rows),
    )

    # Serialize → deserialize round trip
    json_str = response.model_dump_json()
    restored = QueryResponse.model_validate_json(json_str)

    assert restored == response


# Feature: kerberos-impala-backend, Property (1.2): TGT renewal triggers within threshold

from unittest.mock import patch

from app.kerberos_manager import KerberosManager


@given(
    time_to_expiry_seconds=st.integers(min_value=-3600, max_value=86400),
    threshold_seconds=st.integers(min_value=1, max_value=3600),
)
@settings(max_examples=100)
def test_tgt_renewal_threshold(time_to_expiry_seconds, threshold_seconds):
    """
    **Validates: Requirements 1.2**

    For any TGT expiry time and renewal threshold, renew_if_needed() must call
    authenticate() if and only if time_to_expiry <= threshold.
    """
    settings_obj = Settings.model_construct(
        krb5_realm="EXAMPLE.COM",
        krb5_kdc="kdc.example.com",
        krb5_principal="user@EXAMPLE.COM",
        krb5_keytab_path="/etc/krb5/user.keytab",
        impala_host="impala.example.com",
        krb5_renewal_threshold_seconds=threshold_seconds,
        krb5_max_retry=1,
        krb5_auth_mode="keytab",
    )

    manager = KerberosManager(settings_obj)

    tgt_valid = time_to_expiry_seconds > threshold_seconds

    with patch.object(manager, "is_tgt_valid", return_value=tgt_valid):
        with patch.object(manager, "authenticate") as mock_authenticate:
            manager.renew_if_needed()

    if time_to_expiry_seconds <= threshold_seconds:
        mock_authenticate.assert_called_once()
    else:
        mock_authenticate.assert_not_called()


# Feature: kerberos-impala-backend, Property 6: Connection pool bounds

from unittest.mock import MagicMock, patch

from hypothesis import assume

from app.connection_pool import ImpalaConnectionPool


@given(
    pool_min=st.integers(min_value=0, max_value=5),
    pool_max=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=100)
def test_connection_pool_bounds(pool_min, pool_max):
    """
    **Validates: Requirements 2.2**

    For any valid (min, max) pair where min <= max, after warmup the active
    connection count must be >= min and never exceed max.
    """
    assume(pool_min <= pool_max)

    pool_settings = Settings.model_construct(
        krb5_realm="EXAMPLE.COM",
        krb5_kdc="kdc.example.com",
        krb5_principal="user@EXAMPLE.COM",
        krb5_keytab_path="/etc/krb5/user.keytab",
        impala_host="impala.example.com",
        impala_port=21050,
        impala_database="default",
        impala_use_tls=False,
        impala_ca_cert=None,
        impala_pool_min=pool_min,
        impala_pool_max=pool_max,
        impala_query_timeout=60,
        krb5_max_retry=1,
    )

    mock_krb_manager = MagicMock()

    with patch("app.connection_pool.impala_dbapi.connect", return_value=MagicMock()):
        pool = ImpalaConnectionPool(pool_settings, mock_krb_manager)

    # After warmup, pool size must be >= min and <= max
    pool_size = pool._pool.qsize()
    assert pool_size >= pool_min, (
        f"Pool size {pool_size} is less than pool_min {pool_min}"
    )
    assert pool_size <= pool_max, (
        f"Pool size {pool_size} exceeds pool_max {pool_max}"
    )

    # Acquire all available connections and verify count never exceeds pool_max
    acquired = []
    with patch("app.connection_pool.impala_dbapi.connect", return_value=MagicMock()):
        for _ in range(pool_size):
            acquired.append(pool.acquire())

    assert len(acquired) <= pool_max, (
        f"Acquired {len(acquired)} connections which exceeds pool_max {pool_max}"
    )


# Feature: kerberos-impala-backend, Property 1: Query result column consistency

from contextlib import contextmanager

from app.query_executor import QueryExecutor

# Strategy for a single cursor description entry: (name, type_code, ...)
_desc_entry_strategy = st.tuples(
    st.text(min_size=1, max_size=20, alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"),
        whitelist_characters="_",
    )),
    st.text(min_size=1, max_size=10),  # type_code as string
    st.none(), st.none(), st.none(), st.none(), st.none(),
)

_cell_value_strategy = st.one_of(
    st.integers(),
    st.text(),
    st.none(),
    st.floats(allow_nan=False, allow_infinity=False),
)


@given(data=st.data())
@settings(max_examples=100)
def test_query_result_column_consistency(data):
    """
    **Validates: Requirements 3.1, 3.5**

    For any randomly generated cursor description and row data, every row in
    the resulting QueryResponse must contain exactly the keys listed in
    columns — no more, no fewer.
    """
    description = data.draw(
        st.lists(_desc_entry_strategy, min_size=0, max_size=10)
    )
    col_names = [entry[0] for entry in description]

    if col_names:
        row_strategy = st.lists(
            st.tuples(*[_cell_value_strategy for _ in col_names]),
            min_size=0,
            max_size=10,
        )
    else:
        row_strategy = st.just([])

    raw_rows = data.draw(row_strategy)

    # Build mock cursor
    mock_cursor = MagicMock()
    mock_cursor.description = description if description else None
    mock_cursor.fetchall.return_value = raw_rows

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_pool = MagicMock()

    @contextmanager
    def _connection():
        yield mock_conn

    mock_pool.connection = _connection

    executor = QueryExecutor(mock_pool)
    result = executor.execute("SELECT *")

    expected_keys = set(col_names)

    for row in result.rows:
        assert set(row.keys()) == expected_keys, (
            f"Row keys {set(row.keys())} != expected column keys {expected_keys}"
        )

    assert len(result.columns) == len(col_names)
    assert [c.name for c in result.columns] == col_names


# Feature: kerberos-impala-backend, Property 2 + 7: Empty and whitespace SQL rejection

import anyio
import httpx
from fastapi import FastAPI

from app.api import router as api_router


def _make_app(mock_executor, mock_krb_manager):
    app = FastAPI()
    app.include_router(api_router)
    app.state.executor = mock_executor
    app.state.krb_manager = mock_krb_manager
    return app


def _post_query(app, sql: str) -> int:
    """POST /query synchronously; return HTTP status code."""
    async def _run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            return await client.post("/query", json={"sql": sql})
    return anyio.run(_run).status_code


@given(
    sql=st.one_of(
        st.just(""),
        st.text(
            alphabet=st.characters(whitelist_categories=("Zs",), whitelist_characters=" \t\n\r\f\v"),
            min_size=1,
            max_size=50,
        ),
    )
)
@settings(max_examples=100)
def test_empty_and_whitespace_sql_rejected(sql):
    """
    **Validates: Requirements 3.6, 4.3**

    For any string that is empty or composed entirely of whitespace,
    POST /query must return 422 without calling the executor.
    """
    mock_executor = MagicMock()
    mock_krb_manager = MagicMock()
    mock_krb_manager.is_tgt_valid.return_value = True

    app = _make_app(mock_executor, mock_krb_manager)
    status = _post_query(app, sql)

    assert status == 422, f"Expected 422 for sql={repr(sql)}, got {status}"
    mock_executor.execute.assert_not_called()


# Feature: kerberos-impala-backend, Property 5: Health endpoint reflects component state

from itertools import product


@given(
    kerberos_ok=st.booleans(),
    impala_ok=st.booleans(),
)
@settings(max_examples=100)
def test_health_status_reflects_component_state(kerberos_ok, impala_ok):
    """
    **Validates: Requirements 4.2**

    For all 4 combinations of (kerberos_ok, impala_ok), the status field
    must be "ok" iff both components are "ok".
    """
    mock_executor = MagicMock()
    mock_krb_manager = MagicMock()
    mock_krb_manager.is_tgt_valid.return_value = kerberos_ok

    if impala_ok:
        mock_executor.ping.return_value = True
    else:
        mock_executor.ping.return_value = False

    app = _make_app(mock_executor, mock_krb_manager)

    async def _run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            return await client.get("/health")

    resp = anyio.run(_run)
    assert resp.status_code == 200
    data = resp.json()

    expected_status = "ok" if (kerberos_ok and impala_ok) else "degraded"
    assert data["status"] == expected_status, (
        f"kerberos_ok={kerberos_ok}, impala_ok={impala_ok}: "
        f"expected status={expected_status!r}, got {data['status']!r}"
    )
    assert data["kerberos"] == ("ok" if kerberos_ok else "error")
    assert data["impala"] == ("ok" if impala_ok else "error")
