from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class ColumnMeta(BaseModel):
    name: str
    type: str


class QueryResponse(BaseModel):
    columns: list[ColumnMeta]
    rows: list[dict]
    row_count: int


class HealthResponse(BaseModel):
    status: str  # "ok" | "degraded" | "error"
    kerberos: str  # "ok" | "error"
    impala: str  # "ok" | "error"


class DatabasesResponse(BaseModel):
    databases: list[str]


class TablesResponse(BaseModel):
    tables: list[str]
