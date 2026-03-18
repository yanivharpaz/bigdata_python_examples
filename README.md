# Kerberos-Impala Backend

A FastAPI service that executes Impala queries with Kerberos (GSSAPI) authentication and a thread-safe connection pool.

---

## Prerequisites

- Docker and Docker Compose v2
- (For local dev without Docker) Python 3.11+, `kinit`, and a reachable Impala/HiveServer2

---

## Quickstart with Docker Compose

```bash
# 1. Copy and edit environment variables
cp .env.example .env

# 2. Start the full stack (KDC → kdc-init → Hive → backend)
docker compose up --build

# 3. The API is available at http://localhost:8000
```

The `kdc-init` service runs once to create Kerberos principals and export the keytab to the shared `keytabs/` volume. The backend waits for Hive to start before serving requests.

---

## Environment Variable Reference

### Kerberos — Required

| Variable | Description |
|---|---|
| `KRB5_REALM` | Kerberos realm (e.g. `EXAMPLE.COM`) |
| `KRB5_KDC` | KDC hostname or IP |
| `KRB5_PRINCIPAL` | Service principal (e.g. `backend@EXAMPLE.COM`) |
| `KRB5_KEYTAB_PATH` | Absolute path to the keytab file |
| `IMPALA_HOST` | Impala / HiveServer2 hostname |

### Kerberos — Optional

| Variable | Default | Description |
|---|---|---|
| `KRB5_AUTH_MODE` | `keytab` | `keytab` or `kinit` |
| `KRB5_RENEWAL_THRESHOLD_SECONDS` | `300` | Seconds before TGT expiry to trigger renewal |
| `KRB5_MAX_RETRY` | `3` | Max kinit retry attempts |

### Impala — Optional

| Variable | Default | Description |
|---|---|---|
| `IMPALA_PORT` | `21050` | HiveServer2 / Impala port |
| `IMPALA_DATABASE` | `default` | Default database |
| `IMPALA_USE_TLS` | `false` | Enable TLS for Impala connection |
| `IMPALA_CA_CERT` | _(none)_ | Path to CA certificate (TLS only) |
| `IMPALA_POOL_MIN` | `2` | Minimum warm connections in pool |
| `IMPALA_POOL_MAX` | `10` | Maximum connections in pool |
| `IMPALA_QUERY_TIMEOUT` | `60` | Per-query timeout in seconds |

---

## API Endpoints

### POST /query — Execute SQL

```bash
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 AS n"}' | python3 -m json.tool
```

Response:
```json
{
  "columns": [{"name": "n", "type": "int"}],
  "rows": [{"n": 1}],
  "row_count": 1
}
```

### GET /health — Health check

```bash
curl -s http://localhost:8000/health | python3 -m json.tool
```

Response:
```json
{
  "status": "ok",
  "kerberos": "ok",
  "impala": "ok"
}
```

### GET /databases — List databases

```bash
curl -s http://localhost:8000/databases | python3 -m json.tool
```

### GET /databases/{database}/tables — List tables

```bash
curl -s http://localhost:8000/databases/default/tables | python3 -m json.tool
```

---

## Running Tests

```bash
# Unit + property tests (no Docker required)
python -m pytest tests/unit/ tests/property/ -v

# Integration tests (requires docker compose up first)
python -m pytest tests/integration/ -v
```

---

## Project Structure

```
app/
  config.py           # Pydantic settings
  models.py           # Request/response models
  kerberos_manager.py # TGT lifecycle management
  connection_pool.py  # Thread-safe Impala connection pool
  query_executor.py   # SQL execution layer
  api.py              # FastAPI router
  main.py             # App entry point and lifespan wiring
docker/
  Dockerfile          # Backend image
  kdc-init.sh         # KDC principal + keytab bootstrap
  hive-site.xml       # HiveServer2 Kerberos config
krb5/
  krb5.conf           # Kerberos client config (mounted into containers)
tests/
  unit/               # Unit tests (mocked dependencies)
  property/           # Hypothesis property-based tests
  integration/        # End-to-end tests (requires running stack)
```

