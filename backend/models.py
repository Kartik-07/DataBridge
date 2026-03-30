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


# ── File Source Types ────────────────────────────────────────────────────────

class FileSourceType(str, Enum):
    LOCAL = "local"
    SFTP = "sftp"
    S3 = "s3"


class FileFormat(str, Enum):
    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"
    XLSX = "xlsx"
    PARQUET = "parquet"


class FileSourceConfig(BaseModel):
    source_type: FileSourceType
    # Local FS
    file_paths: list[str] = []
    # SFTP
    host: Optional[str] = None
    port: int = 22
    username: Optional[str] = None
    password: Optional[str] = None
    key_path: Optional[str] = None     # path to private key file on server
    remote_paths: list[str] = []
    # S3
    bucket: Optional[str] = None
    region: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    keys: list[str] = []               # S3 object keys or prefixes
    endpoint_url: Optional[str] = None  # for MinIO / custom endpoints
    # Common
    file_format: Optional[FileFormat] = None  # None = auto-detect from extension


class FileInfo(BaseModel):
    """Metadata about a discovered file."""
    path: str
    name: str
    size: Optional[int] = None
    format: Optional[FileFormat] = None


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
    # True for PostgreSQL USER-DEFINED / domain types, MySQL ENUM/SET, SQL Server DOMAIN_NAME, etc.
    is_user_defined: bool = False


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


class FileMigrationRequest(BaseModel):
    file_source: FileSourceConfig
    target: ConnectionConfig
    options: MigrationOptions = Field(default_factory=MigrationOptions)
    session_id: Optional[str] = None
