"""
Schema translator — converts DDL between PostgreSQL, MySQL, and Snowflake.
Handles type mapping, quoting conventions, and constraint syntax.
"""
from __future__ import annotations

from typing import Optional

from models import DbType, ColumnInfo, TableInfo, ColumnMapping


# ── Type Mapping ────────────────────────────────────────────────────────────

# Map (source_type, target_type) -> { source_col_type: target_col_type }
# All keys are lowercased for lookup.

_PG_TO_MYSQL = {
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "serial": "INT AUTO_INCREMENT",
    "bigserial": "BIGINT AUTO_INCREMENT",
    "boolean": "TINYINT(1)",
    "text": "TEXT",
    "character varying": "VARCHAR(255)",
    "varchar": "VARCHAR(255)",
    "char": "CHAR(1)",
    "numeric": "DECIMAL(18,2)",
    "decimal": "DECIMAL(18,2)",
    "real": "FLOAT",
    "double precision": "DOUBLE",
    "date": "DATE",
    "timestamp without time zone": "DATETIME",
    "timestamp with time zone": "DATETIME",
    "timestamp": "DATETIME",
    "time without time zone": "TIME",
    "time with time zone": "TIME",
    "json": "JSON",
    "jsonb": "JSON",
    "uuid": "CHAR(36)",
    "bytea": "BLOB",
    "inet": "VARCHAR(45)",
    "cidr": "VARCHAR(45)",
    "macaddr": "VARCHAR(17)",
    "array": "JSON",
    "interval": "VARCHAR(50)",
    "money": "DECIMAL(19,4)",
    "xml": "TEXT",
    "point": "POINT",
    "polygon": "POLYGON",
}

_PG_TO_SNOWFLAKE = {
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "serial": "INTEGER AUTOINCREMENT",
    "bigserial": "BIGINT AUTOINCREMENT",
    "boolean": "BOOLEAN",
    "text": "TEXT",
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "char": "CHAR(1)",
    "numeric": "NUMBER(18,2)",
    "decimal": "NUMBER(18,2)",
    "real": "FLOAT",
    "double precision": "DOUBLE",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP_NTZ",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "timestamp": "TIMESTAMP_NTZ",
    "time without time zone": "TIME",
    "time with time zone": "TIME",
    "json": "VARIANT",
    "jsonb": "VARIANT",
    "uuid": "VARCHAR(36)",
    "bytea": "BINARY",
    "inet": "VARCHAR(45)",
    "array": "VARIANT",
    "interval": "VARCHAR(50)",
    "money": "NUMBER(19,4)",
    "xml": "VARCHAR",
}

_MYSQL_TO_PG = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "SMALLINT",
    "mediumint": "INTEGER",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "decimal": "NUMERIC",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "text": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "tinytext": "TEXT",
    "blob": "BYTEA",
    "mediumblob": "BYTEA",
    "longblob": "BYTEA",
    "tinyblob": "BYTEA",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "year": "SMALLINT",
    "json": "JSONB",
    "enum": "TEXT",
    "set": "TEXT",
    "binary": "BYTEA",
    "varbinary": "BYTEA",
    "bit": "BIT",
    "boolean": "BOOLEAN",
    "point": "POINT",
    "polygon": "POLYGON",
}

_MYSQL_TO_SNOWFLAKE = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "SMALLINT",
    "mediumint": "INTEGER",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "NUMBER",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "text": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "tinytext": "TEXT",
    "blob": "BINARY",
    "mediumblob": "BINARY",
    "longblob": "BINARY",
    "date": "DATE",
    "datetime": "TIMESTAMP_NTZ",
    "timestamp": "TIMESTAMP_NTZ",
    "time": "TIME",
    "json": "VARIANT",
    "enum": "VARCHAR",
    "set": "VARCHAR",
    "boolean": "BOOLEAN",
}

_SNOWFLAKE_TO_PG = {
    "number": "NUMERIC",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "float": "DOUBLE PRECISION",
    "double": "DOUBLE PRECISION",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "text": "TEXT",
    "string": "TEXT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp_ntz": "TIMESTAMP",
    "timestamp_tz": "TIMESTAMPTZ",
    "timestamp_ltz": "TIMESTAMPTZ",
    "time": "TIME",
    "variant": "JSONB",
    "object": "JSONB",
    "array": "JSONB",
    "binary": "BYTEA",
}

_SNOWFLAKE_TO_MYSQL = {
    "number": "DECIMAL",
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "varchar": "VARCHAR(255)",
    "char": "CHAR",
    "text": "TEXT",
    "string": "TEXT",
    "boolean": "TINYINT(1)",
    "date": "DATE",
    "timestamp_ntz": "DATETIME",
    "timestamp_tz": "DATETIME",
    "timestamp_ltz": "DATETIME",
    "time": "TIME",
    "variant": "JSON",
    "object": "JSON",
    "array": "JSON",
    "binary": "BLOB",
}

# SQLite: INTEGER, TEXT, REAL, BLOB, NUMERIC
_PG_TO_SQLITE = {
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "smallint": "INTEGER",
    "serial": "INTEGER",
    "bigserial": "INTEGER",
    "boolean": "INTEGER",
    "text": "TEXT",
    "character varying": "TEXT",
    "varchar": "TEXT",
    "char": "TEXT",
    "numeric": "REAL",
    "decimal": "REAL",
    "real": "REAL",
    "double precision": "REAL",
    "date": "TEXT",
    "timestamp without time zone": "TEXT",
    "timestamp with time zone": "TEXT",
    "timestamp": "TEXT",
    "time without time zone": "TEXT",
    "time with time zone": "TEXT",
    "json": "TEXT",
    "jsonb": "TEXT",
    "uuid": "TEXT",
    "bytea": "BLOB",
    "inet": "TEXT",
    "array": "TEXT",
    "interval": "TEXT",
    "money": "REAL",
    "xml": "TEXT",
}

_MYSQL_TO_SQLITE = {
    "int": "INTEGER",
    "bigint": "INTEGER",
    "smallint": "INTEGER",
    "tinyint": "INTEGER",
    "mediumint": "INTEGER",
    "float": "REAL",
    "double": "REAL",
    "decimal": "REAL",
    "varchar": "TEXT",
    "char": "TEXT",
    "text": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "tinytext": "TEXT",
    "blob": "BLOB",
    "date": "TEXT",
    "datetime": "TEXT",
    "timestamp": "TEXT",
    "time": "TEXT",
    "json": "TEXT",
    "enum": "TEXT",
    "set": "TEXT",
    "boolean": "INTEGER",
}

_SNOWFLAKE_TO_SQLITE = {
    "number": "REAL",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "smallint": "INTEGER",
    "float": "REAL",
    "double": "REAL",
    "varchar": "TEXT",
    "char": "TEXT",
    "text": "TEXT",
    "string": "TEXT",
    "boolean": "INTEGER",
    "date": "TEXT",
    "timestamp_ntz": "TEXT",
    "timestamp_tz": "TEXT",
    "timestamp_ltz": "TEXT",
    "time": "TEXT",
    "variant": "TEXT",
    "object": "TEXT",
    "array": "TEXT",
    "binary": "BLOB",
}

# SQL Server types
_PG_TO_SQLSERVER = {
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "serial": "INT IDENTITY(1,1)",
    "bigserial": "BIGINT IDENTITY(1,1)",
    "boolean": "BIT",
    "text": "NVARCHAR(MAX)",
    "character varying": "NVARCHAR(255)",
    "varchar": "NVARCHAR(255)",
    "char": "NCHAR(1)",
    "numeric": "DECIMAL(18,2)",
    "decimal": "DECIMAL(18,2)",
    "real": "REAL",
    "double precision": "FLOAT",
    "date": "DATE",
    "timestamp without time zone": "DATETIME2",
    "timestamp with time zone": "DATETIMEOFFSET",
    "timestamp": "DATETIME2",
    "time without time zone": "TIME",
    "time with time zone": "DATETIMEOFFSET",
    "json": "NVARCHAR(MAX)",
    "jsonb": "NVARCHAR(MAX)",
    "uuid": "CHAR(36)",
    "bytea": "VARBINARY(MAX)",
    "inet": "NVARCHAR(45)",
    "array": "NVARCHAR(MAX)",
    "interval": "NVARCHAR(50)",
    "money": "DECIMAL(19,4)",
    "xml": "NVARCHAR(MAX)",
}

_MYSQL_TO_SQLSERVER = {
    "int": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "SMALLINT",
    "mediumint": "INT",
    "float": "REAL",
    "double": "FLOAT",
    "decimal": "DECIMAL(18,2)",
    "varchar": "NVARCHAR(255)",
    "char": "NCHAR(1)",
    "text": "NVARCHAR(MAX)",
    "mediumtext": "NVARCHAR(MAX)",
    "longtext": "NVARCHAR(MAX)",
    "tinytext": "NVARCHAR(255)",
    "blob": "VARBINARY(MAX)",
    "date": "DATE",
    "datetime": "DATETIME2",
    "timestamp": "DATETIME2",
    "time": "TIME",
    "json": "NVARCHAR(MAX)",
    "enum": "NVARCHAR(255)",
    "set": "NVARCHAR(255)",
    "boolean": "BIT",
}

_SNOWFLAKE_TO_SQLSERVER = {
    "number": "DECIMAL(18,2)",
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "float": "REAL",
    "double": "FLOAT",
    "varchar": "NVARCHAR(255)",
    "char": "NCHAR(1)",
    "text": "NVARCHAR(MAX)",
    "string": "NVARCHAR(MAX)",
    "boolean": "BIT",
    "date": "DATE",
    "timestamp_ntz": "DATETIME2",
    "timestamp_tz": "DATETIMEOFFSET",
    "timestamp_ltz": "DATETIMEOFFSET",
    "time": "TIME",
    "variant": "NVARCHAR(MAX)",
    "object": "NVARCHAR(MAX)",
    "array": "NVARCHAR(MAX)",
    "binary": "VARBINARY(MAX)",
}

_SQLITE_TO_PG = {
    "integer": "INTEGER",
    "text": "TEXT",
    "real": "DOUBLE PRECISION",
    "blob": "BYTEA",
    "numeric": "NUMERIC",
}

_SQLITE_TO_MYSQL = {
    "integer": "INT",
    "text": "TEXT",
    "real": "DOUBLE",
    "blob": "BLOB",
    "numeric": "DECIMAL(18,2)",
}

_SQLITE_TO_SNOWFLAKE = {
    "integer": "INTEGER",
    "text": "VARCHAR",
    "real": "FLOAT",
    "blob": "BINARY",
    "numeric": "NUMBER(18,2)",
}

_SQLSERVER_TO_PG = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "bit": "BOOLEAN",
    "nvarchar": "VARCHAR",
    "varchar": "VARCHAR",
    "nchar": "CHAR",
    "char": "CHAR",
    "text": "TEXT",
    "ntext": "TEXT",
    "real": "REAL",
    "float": "DOUBLE PRECISION",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "datetime2": "TIMESTAMP",
    "datetimeoffset": "TIMESTAMPTZ",
    "time": "TIME",
    "varbinary": "BYTEA",
}

_SQLSERVER_TO_MYSQL = {
    "int": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "bit": "TINYINT(1)",
    "nvarchar": "VARCHAR(255)",
    "varchar": "VARCHAR(255)",
    "nchar": "CHAR(1)",
    "char": "CHAR(1)",
    "text": "TEXT",
    "ntext": "TEXT",
    "real": "FLOAT",
    "float": "DOUBLE",
    "decimal": "DECIMAL(18,2)",
    "numeric": "DECIMAL(18,2)",
    "date": "DATE",
    "datetime": "DATETIME",
    "datetime2": "DATETIME",
    "datetimeoffset": "DATETIME",
    "time": "TIME",
    "varbinary": "BLOB",
}

_SQLSERVER_TO_SNOWFLAKE = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "bit": "BOOLEAN",
    "nvarchar": "VARCHAR",
    "varchar": "VARCHAR",
    "nchar": "CHAR",
    "char": "CHAR",
    "text": "TEXT",
    "ntext": "TEXT",
    "real": "FLOAT",
    "float": "DOUBLE",
    "decimal": "NUMBER(18,2)",
    "numeric": "NUMBER(18,2)",
    "date": "DATE",
    "datetime": "TIMESTAMP_NTZ",
    "datetime2": "TIMESTAMP_NTZ",
    "datetimeoffset": "TIMESTAMP_TZ",
    "time": "TIME",
    "varbinary": "BINARY",
}


def _get_type_map(source: DbType, target: DbType) -> dict[str, str]:
    """Return the type mapping dictionary for a source→target pair."""
    key = (source, target)
    maps = {
        (DbType.POSTGRESQL, DbType.MYSQL): _PG_TO_MYSQL,
        (DbType.POSTGRESQL, DbType.SNOWFLAKE): _PG_TO_SNOWFLAKE,
        (DbType.POSTGRESQL, DbType.SQLITE): _PG_TO_SQLITE,
        (DbType.POSTGRESQL, DbType.SQLSERVER): _PG_TO_SQLSERVER,
        (DbType.MYSQL, DbType.POSTGRESQL): _MYSQL_TO_PG,
        (DbType.MYSQL, DbType.SNOWFLAKE): _MYSQL_TO_SNOWFLAKE,
        (DbType.MYSQL, DbType.SQLITE): _MYSQL_TO_SQLITE,
        (DbType.MYSQL, DbType.SQLSERVER): _MYSQL_TO_SQLSERVER,
        (DbType.SNOWFLAKE, DbType.POSTGRESQL): _SNOWFLAKE_TO_PG,
        (DbType.SNOWFLAKE, DbType.MYSQL): _SNOWFLAKE_TO_MYSQL,
        (DbType.SNOWFLAKE, DbType.SQLITE): _SNOWFLAKE_TO_SQLITE,
        (DbType.SNOWFLAKE, DbType.SQLSERVER): _SNOWFLAKE_TO_SQLSERVER,
        (DbType.SQLITE, DbType.POSTGRESQL): _SQLITE_TO_PG,
        (DbType.SQLITE, DbType.MYSQL): _SQLITE_TO_MYSQL,
        (DbType.SQLITE, DbType.SNOWFLAKE): _SQLITE_TO_SNOWFLAKE,
        (DbType.SQLSERVER, DbType.POSTGRESQL): _SQLSERVER_TO_PG,
        (DbType.SQLSERVER, DbType.MYSQL): _SQLSERVER_TO_MYSQL,
        (DbType.SQLSERVER, DbType.SNOWFLAKE): _SQLSERVER_TO_SNOWFLAKE,
    }
    return maps.get(key, {})


def translate_type(col_type: str, source: DbType, target: DbType) -> str:
    """Translate a single column type from source dialect to target dialect."""
    if source == target:
        # Same dialect — pass through as-is (handles text[], vector, jsonb, etc.)
        return col_type

    type_map = _get_type_map(source, target)
    normalized = col_type.lower().strip()

    # Handle PostgreSQL array types like "text[]", "int4[]"
    if normalized.endswith("[]"):
        if target == DbType.MYSQL:
            return "JSON"
        elif target == DbType.SNOWFLAKE:
            return "VARIANT"
        elif target in (DbType.SQLITE, DbType.SQLSERVER):
            return "TEXT" if target == DbType.SQLITE else "NVARCHAR(MAX)"
        return col_type

    # Exact match
    if normalized in type_map:
        return type_map[normalized]

    # Try without parenthetical parts: "varchar(100)" → "varchar"
    base = normalized.split("(")[0].strip()
    if base in type_map:
        mapped = type_map[base]
        # Preserve length if the mapped type doesn't already have one
        if "(" in normalized and "(" not in mapped:
            length_part = normalized[normalized.index("("):]
            return f"{mapped}{length_part}"
        return mapped

    # Fallback for USER-DEFINED / unknown types → TEXT
    return "TEXT"


# ── DDL Generation ──────────────────────────────────────────────────────────

def _quote_identifier(name: str, db_type: DbType) -> str:
    """Quote an identifier using the appropriate syntax.
    Handles schema-qualified names like 'schema.table' by quoting each part.
    """
    if db_type == DbType.MYSQL:
        parts = name.split(".")
        return ".".join(f"`{p}`" for p in parts)
    if db_type == DbType.SQLSERVER:
        parts = name.split(".")
        return ".".join(f"[{p}]" for p in parts)
    # PostgreSQL / Snowflake / SQLite
    parts = name.split(".")
    return ".".join(f'"{p}"' for p in parts)


def _quote_column(name: str, db_type: DbType) -> str:
    """Quote a column name without splitting on dots."""
    if db_type == DbType.MYSQL:
        return f"`{name}`"
    if db_type == DbType.SQLSERVER:
        return f"[{name}]"
    return f'"{name}"'


def generate_create_table(
    table: TableInfo,
    source_type: DbType,
    target_type: DbType,
    with_pk: bool = True,
    migrating_fresh: bool = False,
    if_not_exists: bool = False,
    column_mappings: Optional[dict[str, ColumnMapping]] = None,
) -> str:
    """Generate a CREATE TABLE statement for the target database.

    When if_not_exists is True, emits CREATE TABLE IF NOT EXISTS so the statement
    is a no-op when the table already exists (used when not dropping existing tables).
    When column_mappings is provided, uses target_name and target_type from mappings.
    """
    q = lambda name: _quote_identifier(name, target_type)
    qc = lambda name: _quote_column(name, target_type)
    cols = []
    pk_cols = []
    mappings = column_mappings or {}

    for col in table.columns:
        m = mappings.get(col.name)
        col_name = m.target_name if m else col.name
        # Preserve exact datatype representation natively if source and target DBs are the identical engine type.
        # Otherwise, we MUST translate to prevent dialect crashes (e.g. Postgres JSONB -> Snowflake VARIANT)
        if m and m.target_type:
            mapped_type = m.target_type
        elif migrating_fresh and source_type == target_type:
            mapped_type = col.data_type
        else:
            mapped_type = translate_type(col.data_type, source_type, target_type)
            
        # Handle AUTO_INCREMENT — don't add NOT NULL separately since AI implies it
        nullable = "" if col.is_nullable or "AUTO_INCREMENT" in mapped_type.upper() or "AUTOINCREMENT" in mapped_type.upper() else " NOT NULL"
        default = ""
        if col.default and "nextval" not in (col.default or "").lower() and "auto_increment" not in mapped_type.lower() and "autoincrement" not in mapped_type.lower():
            default = f" DEFAULT {col.default}"
        cols.append(f"  {qc(col_name)} {mapped_type}{nullable}{default}")
        if col.is_primary_key:
            pk_cols.append(qc(col_name))

    if with_pk and pk_cols:
        cols.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")

    cols_sql = ",\n".join(cols)
    table_name = q(table.name)
    prefix = "CREATE TABLE IF NOT EXISTS " if if_not_exists else "CREATE TABLE "

    return f"{prefix}{table_name} (\n{cols_sql}\n);"

def generate_add_primary_key(
    table_name: str,
    table: TableInfo,
    target_type: DbType,
    column_mappings: Optional[dict[str, ColumnMapping]] = None,
) -> str | None:
    """Generate an ALTER TABLE statement to add a primary key after data load."""
    q = lambda name: _quote_identifier(name, target_type)
    qc = lambda name: _quote_column(name, target_type)
    mappings = column_mappings or {}
    pk_cols = [
        qc(mappings[col.name].target_name if col.name in mappings else col.name)
        for col in table.columns if col.is_primary_key
    ]
    
    if not pk_cols:
        return None

    if target_type == DbType.SQLSERVER:
        # SQL Server requires a named constraint
        base = table_name.split(".")[-1] if "." in table_name else table_name
        pk_name = f"PK_{base}"[:128]
        return f"ALTER TABLE {q(table_name)} ADD CONSTRAINT [{pk_name}] PRIMARY KEY ({', '.join(pk_cols)});"
        
    return f"ALTER TABLE {q(table_name)} ADD PRIMARY KEY ({', '.join(pk_cols)});"


def generate_drop_table(table_name: str, target_type: DbType) -> str:
    """Generate a DROP TABLE IF EXISTS statement."""
    q = _quote_identifier(table_name, target_type)
    if target_type == DbType.MYSQL:
        return f"DROP TABLE IF EXISTS {q};"
    if target_type in (DbType.SQLITE, DbType.SQLSERVER):
        return f"DROP TABLE IF EXISTS {q};"
    return f'DROP TABLE IF EXISTS {q} CASCADE;'


def _get_placeholder(target_type: DbType, for_execute_values: bool = False) -> str:
    """Return the parameter placeholder for the target database."""
    if for_execute_values and target_type == DbType.POSTGRESQL:
        return "%s"  # execute_values uses %s
    if target_type in (DbType.SQLITE, DbType.SQLSERVER):
        return "?"
    return "%s"


def generate_insert(
    table_name: str,
    columns: list[str],
    target_type: DbType,
    for_execute_values: bool = False,
) -> str:
    """Generate a parameterized INSERT statement for the target database."""
    q = lambda name: _quote_identifier(name, target_type)
    qc = lambda name: _quote_column(name, target_type)
    col_list = ", ".join(qc(c) for c in columns)
    ph = _get_placeholder(target_type, for_execute_values)

    if for_execute_values and target_type == DbType.POSTGRESQL:
        return f"INSERT INTO {q(table_name)} ({col_list}) VALUES %s"

    placeholders = ", ".join([ph] * len(columns))
    return f"INSERT INTO {q(table_name)} ({col_list}) VALUES ({placeholders})"


def generate_snowflake_staging_upsert(
    table_name: str,
    columns: list[str],
    pk_columns: list[str],
    target_type: DbType,
) -> tuple[str, str, str, str]:
    """Generate staging table SQL for Snowflake bulk upsert.
    Returns (create_staging_sql, insert_sql, merge_sql, drop_sql).
    Uses temp table + bulk INSERT + single MERGE for performance.
    """
    if not pk_columns:
        raise ValueError("Snowflake staging upsert requires primary key columns")
    q = lambda name: _quote_identifier(name, target_type)
    qc = lambda name: _quote_column(name, target_type)
    col_list = ", ".join(qc(c) for c in columns)
    non_pk = [c for c in columns if c not in pk_columns]
    staging_name = f"_databridge_stage_{table_name.replace('.', '_')}"[:128]
    placeholders = ", ".join(["%s"] * len(columns))
    src_cols = ", ".join(f"src.{qc(c)}" for c in columns)
    on_clause = " AND ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in pk_columns)

    # Use VARIANT for staging to accept any type; Snowflake will coerce on MERGE
    col_defs = ", ".join(f"{qc(c)} VARIANT" for c in columns)
    create_sql = f"CREATE TEMPORARY TABLE {q(staging_name)} ({col_defs})"
    insert_sql = f"INSERT INTO {q(staging_name)} ({col_list}) VALUES ({placeholders})"
    if non_pk:
        update_set = ", ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in non_pk)
        merge_sql = (
            f"MERGE INTO {q(table_name)} AS tgt USING {q(staging_name)} AS src "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols})"
        )
    else:
        merge_sql = (
            f"MERGE INTO {q(table_name)} AS tgt USING {q(staging_name)} AS src "
            f"ON {on_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols})"
        )
    drop_sql = f"DROP TABLE IF EXISTS {q(staging_name)}"
    return create_sql, insert_sql, merge_sql, drop_sql


def generate_upsert(
    table_name: str,
    columns: list[str],
    pk_columns: list[str],
    target_type: DbType,
    for_execute_values: bool = False,
) -> str | None:
    """Generate an UPSERT statement for idempotent batch writes.

    Returns None if pk_columns is empty (no PK → can't upsert).
    """
    if not pk_columns:
        return None

    q = lambda name: _quote_identifier(name, target_type)
    qc = lambda name: _quote_column(name, target_type)
    col_list = ", ".join(qc(c) for c in columns)
    non_pk = [c for c in columns if c not in pk_columns]

    ph = _get_placeholder(target_type, for_execute_values)

    if target_type == DbType.POSTGRESQL:
        pk_list = ", ".join(qc(c) for c in pk_columns)
        if for_execute_values:
            insert_stub = f"INSERT INTO {q(table_name)} ({col_list}) VALUES %s"
        else:
            placeholders = ", ".join(["%s"] * len(columns))
            insert_stub = f"INSERT INTO {q(table_name)} ({col_list}) VALUES ({placeholders})"
        if non_pk:
            update_clause = ", ".join(f"{qc(c)} = EXCLUDED.{qc(c)}" for c in non_pk)
            return f"{insert_stub} ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}"
        return f"{insert_stub} ON CONFLICT ({pk_list}) DO NOTHING"

    elif target_type == DbType.MYSQL:
        placeholders = ", ".join(["%s"] * len(columns))
        if non_pk:
            update_clause = ", ".join(f"{qc(c)} = VALUES({qc(c)})" for c in non_pk)
            return (
                f"INSERT INTO {q(table_name)} ({col_list}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {update_clause}"
            )
        return f"INSERT IGNORE INTO {q(table_name)} ({col_list}) VALUES ({placeholders})"

    elif target_type == DbType.SQLITE:
        placeholders = ", ".join([ph] * len(columns))
        pk_list = ", ".join(qc(c) for c in pk_columns)
        if non_pk:
            update_clause = ", ".join(f"{qc(c)} = excluded.{qc(c)}" for c in non_pk)
            return (
                f"INSERT INTO {q(table_name)} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}"
            )
        return (
            f"INSERT OR IGNORE INTO {q(table_name)} ({col_list}) VALUES ({placeholders})"
        )

    elif target_type == DbType.SQLSERVER:
        placeholders = ", ".join([ph] * len(columns))
        src_cols = ", ".join(f"src.{qc(c)}" for c in columns)
        on_clause = " AND ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in pk_columns)
        col_aliases = ", ".join(f"column{i+1} AS {qc(c)}" for i, c in enumerate(columns))
        col_defs = ", ".join(f"column{i+1}" for i in range(len(columns)))
        if non_pk:
            update_set = ", ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in non_pk)
            return (
                f"MERGE INTO {q(table_name)} AS tgt USING "
                f"(SELECT {col_aliases} FROM (VALUES ({placeholders})) AS v({col_defs})) AS src "
                f"ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols});"
            )
        return (
            f"MERGE INTO {q(table_name)} AS tgt USING "
            f"(SELECT {col_aliases} FROM (VALUES ({placeholders})) AS v({col_defs})) AS src "
            f"ON {on_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols});"
        )

    else:  # Snowflake — MERGE (single-row; for bulk use staging table approach in migration)
        # Snowflake VALUES needs parentheses: FROM (VALUES (?,?,?)) not FROM VALUES (?,?,?)
        placeholders = ", ".join(["%s"] * len(columns))
        src_cols = ", ".join(f"src.{qc(c)}" for c in columns)
        on_clause = " AND ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in pk_columns)
        col_aliases = ", ".join(f"column{i+1} AS {qc(c)}" for i, c in enumerate(columns))
        if non_pk:
            update_set = ", ".join(f"tgt.{qc(c)} = src.{qc(c)}" for c in non_pk)
            return (
                f"MERGE INTO {q(table_name)} AS tgt USING "
                f"(SELECT {col_aliases} FROM (VALUES ({placeholders})) AS src "
                f"ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols})"
            )
        else:
            return (
                f"MERGE INTO {q(table_name)} AS tgt USING "
                f"(SELECT {col_aliases} FROM (VALUES ({placeholders})) AS src "
                f"ON {on_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols})"
            )

