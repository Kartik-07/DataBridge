from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── Database Types ──────────────────────────────────────────────────────────

class DbType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
    SQLSERVER = "sqlserver"


# ── Connection ──────────────────────────────────────────────────────────────

class ConnectionConfig(BaseModel):
    db_type: DbType
    host: str
    port: int
    database: str
    username: str
    password: str
    # Snowflake-specific
    warehouse: Optional[str] = None
    schema_name: Optional[str] = None  # "schema" is a reserved Pydantic attr
    use_browser_login: Optional[bool] = None  # Snowflake: use authenticator='externalbrowser' for MFA
    # SQLite: database = file path (e.g. /path/to/db.sqlite3)
    # SQL Server: optional driver override
    driver: Optional[str] = None  # SQL Server: e.g. "ODBC Driver 17 for SQL Server"


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    server_version: Optional[str] = None
    tables_count: Optional[int] = None
    available_schemas: list[str] = []


# ── Introspection ───────────────────────────────────────────────────────────

class ColumnInfo(BaseModel):
    name: str
    data_type: str
    is_nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False


class TableInfo(BaseModel):
    name: str
    columns: list[ColumnInfo] = []
    row_count: int = 0


class IntrospectResponse(BaseModel):
    tables: list[TableInfo] = []
    views: list[str] = []
    indexes: list[str] = []
    sequences: list[str] = []


class TablesColumnsRequest(BaseModel):
    """Request body for POST /tables/columns — columns for specific tables only."""
    config: ConnectionConfig
    table_refs: list[str] = []


# ── Migration ───────────────────────────────────────────────────────────────

class ColumnMapping(BaseModel):
    action: str = "direct"  # direct string or transform string
    target_name: str
    target_type: str
    source_type: Optional[str] = None  # Source column data type, for schema compatibility warnings
    transform_rule: Optional[str] = None
    source_format: Optional[str] = None
    target_format: Optional[str] = None

class MigrationOptions(BaseModel):
    migrate_schema: bool = True
    migrate_data: bool = True
    migrate_views: bool = True
    migrate_functions: bool = False
    migrate_triggers: bool = False
    migrate_sequences: bool = True
    drop_existing: bool = False
    dry_run: bool = False
    schemas: list[str] = []  # empty = all schemas
    selected_tables: dict[str, list[str]] = {}  # schema -> [table_names]; empty = all tables
    mappings: dict[str, dict[str, ColumnMapping]] = {}  # "schema.table" -> {"source_col": ColumnMapping}


class MigrationRequest(BaseModel):
    source: ConnectionConfig
    target: ConnectionConfig
    options: MigrationOptions = Field(default_factory=MigrationOptions)
    session_id: Optional[str] = None  # For pause/resume control


class LogEntry(BaseModel):
    message: str
    type: str = "info"  # info | success | warning | error
    progress: int = 0
