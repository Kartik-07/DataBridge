"""
Database connectors — thin wrappers around DB-API 2.0 drivers.
Supports PostgreSQL (psycopg2), MySQL (pymysql), Snowflake, SQLite, and SQL Server.
"""
from __future__ import annotations

from typing import Any

from models import ConnectionConfig, DbType, ColumnInfo, TableInfo


# ── Connection Factory ──────────────────────────────────────────────────────

def get_connection(config: ConnectionConfig) -> Any:
    """Return a DB-API 2.0 connection for the given config."""
    if config.db_type == DbType.POSTGRESQL:
        import psycopg2
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.username,
            password=config.password,
            connect_timeout=10,
        )
    elif config.db_type == DbType.MYSQL:
        import pymysql
        return pymysql.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.username,
            password=config.password,
            connect_timeout=10,
            charset="utf8mb4",
        )
    elif config.db_type == DbType.SNOWFLAKE:
        import snowflake.connector
        connect_kwargs: dict = {
            "account": config.host,
            "user": config.username,
            "password": config.password or "",
            "database": config.database,
            "warehouse": config.warehouse or "COMPUTE_WH",
            "schema": config.schema_name or "PUBLIC",
            "login_timeout": 15,
        }
        if getattr(config, "use_browser_login", None):
            connect_kwargs["authenticator"] = "externalbrowser"
        return snowflake.connector.connect(**connect_kwargs)
    elif config.db_type == DbType.SQLITE:
        import sqlite3
        # database = file path (e.g. /path/to/db.sqlite3 or :memory:)
        path = config.database or ":memory:"
        return sqlite3.connect(path, timeout=15)
    elif config.db_type == DbType.SQLSERVER:
        import pyodbc
        driver = getattr(config, "driver", None) or "ODBC Driver 17 for SQL Server"
        port = config.port or 1433
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={config.host},{port};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password}"
        )
        return pyodbc.connect(conn_str, timeout=15)
    else:
        raise ValueError(f"Unsupported database type: {config.db_type}")


# ── Schema Listing ──────────────────────────────────────────────────────────

def get_schemas(conn: Any, db_type: DbType, database: str) -> list[str]:
    """Return list of user-accessible schema names in the database."""
    cur = conn.cursor()

    if db_type == DbType.POSTGRESQL:
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        """)
    elif db_type == DbType.MYSQL:
        # MySQL "schemas" = databases; within a database, no sub-schemas.
        cur.close()
        return [database]
    elif db_type == DbType.SQLITE:
        # SQLite has main, temp; we use main as the single "schema"
        cur.close()
        return ["main"]
    elif db_type == DbType.SQLSERVER:
        cur.execute("""
            SELECT name FROM sys.schemas
            WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'db_owner', 'db_accessadmin',
                'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader',
                'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
            ORDER BY name
        """)
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        return schemas
    else:  # Snowflake
        cur.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cur.fetchall()
                   if row[1] not in ("INFORMATION_SCHEMA",)]
        cur.close()
        return schemas

    schemas = [row[0] for row in cur.fetchall()]
    cur.close()
    return schemas


# ── Test Connection ─────────────────────────────────────────────────────────

def test_connection(config: ConnectionConfig) -> dict:
    """Test connectivity and return metadata + available schemas."""
    try:
        conn = get_connection(config)
        cur = conn.cursor()

        # Get server version
        if config.db_type == DbType.POSTGRESQL:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0].split(",")[0]
        elif config.db_type == DbType.MYSQL:
            cur.execute("SELECT version()")
            version = f"MySQL {cur.fetchone()[0]}"
        elif config.db_type == DbType.SQLITE:
            cur.execute("SELECT sqlite_version()")
            version = f"SQLite {cur.fetchone()[0]}"
        elif config.db_type == DbType.SQLSERVER:
            cur.execute("SELECT @@VERSION")
            v = cur.fetchone()[0]
            version = f"SQL Server {v.split()[2]}" if len(v.split()) > 2 else f"SQL Server {v[:50]}"
        else:
            cur.execute("SELECT CURRENT_VERSION()")
            version = f"Snowflake {cur.fetchone()[0]}"

        # Count tables (across all user schemas for preview)
        table_count = len(get_table_names(conn, config.db_type, config.database))

        # List available schemas
        schemas = get_schemas(conn, config.db_type, config.database)

        cur.close()
        conn.close()

        return {
            "success": True,
            "message": f"Connected to {version}",
            "server_version": version,
            "tables_count": table_count,
            "available_schemas": schemas,
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "server_version": None,
            "tables_count": None,
            "available_schemas": [],
        }


# ── Schema Introspection ───────────────────────────────────────────────────

def get_table_names(
    conn: Any, db_type: DbType, database: str, schemas: list[str] | None = None
) -> list[str]:
    """Return list of table names. When schemas is given, scopes to those schemas."""
    cur = conn.cursor()

    if db_type == DbType.POSTGRESQL:
        if schemas:
            placeholders = ",".join(["%s"] * len(schemas))
            cur.execute(f"""
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema IN ({placeholders}) AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """)
    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (database,))
        names = [f"{database}.{row[0]}" for row in cur.fetchall()]
        cur.close()
        return names
    elif db_type == DbType.SQLITE:
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        names = [f"main.{row[0]}" for row in cur.fetchall()]
        cur.close()
        return names
    elif db_type == DbType.SQLSERVER:
        if schemas:
            placeholders = ",".join(["?"] * len(schemas))
            cur.execute(f"""
                SELECT table_schema + '.' + table_name
                FROM information_schema.tables
                WHERE table_schema IN ({placeholders}) AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT table_schema + '.' + table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('sys', 'INFORMATION_SCHEMA')
                  AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """)
        names = [row[0] for row in cur.fetchall()]
        cur.close()
        return names
    else:  # Snowflake
        if schemas:
            names = []
            for s in schemas:
                cur.execute(f'SHOW TABLES IN SCHEMA "{s}"')
                names.extend([f"{s}.{row[1]}" for row in cur.fetchall()])
            cur.close()
            return names
        else:
            # Return schema.table format so frontend gets correct schema (avoids "default" fallback)
            cur.execute("SELECT CURRENT_SCHEMA()")
            current_schema = (cur.fetchone()[0] or "PUBLIC").strip('"')
            cur.execute("SHOW TABLES")
            names = [f"{current_schema}.{row[1]}" for row in cur.fetchall()]
            cur.close()
            return names

    names = [row[0] for row in cur.fetchall()]
    cur.close()
    return names


def _parse_table_ref(table_ref: str, db_type: DbType):
    """Split 'schema.table' into (schema, table). If no dot, return defaults."""
    if "." in table_ref:
        schema, table = table_ref.split(".", 1)
        return schema, table
    if db_type == DbType.POSTGRESQL:
        return "public", table_ref
    if db_type == DbType.SQLITE:
        return "main", table_ref
    if db_type == DbType.SQLSERVER:
        return "dbo", table_ref
    return None, table_ref


def get_views(
    conn: Any, db_type: DbType, database: str, schemas: list[str] | None = None
) -> list[str]:
    """Return list of view names."""
    cur = conn.cursor()
    if db_type == DbType.POSTGRESQL:
        if schemas:
            placeholders = ",".join(["%s"] * len(schemas))
            cur.execute(f"""
                SELECT table_schema || '.' || table_name
                FROM information_schema.views
                WHERE table_schema IN ({placeholders})
                ORDER BY table_schema, table_name
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT table_schema || '.' || table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT table_name FROM information_schema.views
            WHERE table_schema = %s ORDER BY table_name
        """, (database,))
    elif db_type == DbType.SQLITE:
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type = 'view' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        result = [f"main.{row[0]}" for row in cur.fetchall()]
        cur.close()
        return result
    elif db_type == DbType.SQLSERVER:
        if schemas:
            placeholders = ",".join(["?"] * len(schemas))
            cur.execute(f"""
                SELECT table_schema + '.' + table_name
                FROM information_schema.views
                WHERE table_schema IN ({placeholders})
                ORDER BY table_schema, table_name
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT table_schema + '.' + table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('sys', 'INFORMATION_SCHEMA')
                ORDER BY table_schema, table_name
            """)
        result = [row[0] for row in cur.fetchall()]
        cur.close()
        return result
    else:
        cur.execute("SHOW VIEWS")
        result = [row[1] for row in cur.fetchall()]
        cur.close()
        return result

    result = [row[0] for row in cur.fetchall()]
    cur.close()
    return result


def get_view_definition(
    conn: Any, db_type: DbType, view_ref: str, database: str,
    schema: str | None = None,
) -> str | None:
    """Return CREATE VIEW definition for a view. Returns None if not supported or on error."""
    cur = conn.cursor()
    try:
        if "." in view_ref:
            schema, view_name = view_ref.split(".", 1)
        else:
            view_name = view_ref
            schema = schema or (database if db_type == DbType.MYSQL else "public" if db_type == DbType.POSTGRESQL else "dbo")

        if db_type == DbType.POSTGRESQL:
            qualified = f'"{schema}"."{view_name}"'
            cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", (qualified,))
            row = cur.fetchone()
            if row:
                return f"CREATE OR REPLACE VIEW {qualified} AS {row[0]}"
        elif db_type == DbType.MYSQL:
            db = schema if "." in view_ref else database
            cur.execute(f"SHOW CREATE VIEW `{db}`.`{view_name}`")
            row = cur.fetchone()
            if row and len(row) >= 2:
                return row[1]  # Create View column
        elif db_type == DbType.SQLSERVER:
            schema = schema or "dbo"
            cur.execute(f"""
                SELECT m.definition FROM sys.sql_modules m
                JOIN sys.views v ON m.object_id = v.object_id
                JOIN sys.schemas s ON v.schema_id = s.schema_id
                WHERE s.name = ? AND v.name = ?
            """, (schema, view_name))
            row = cur.fetchone()
            if row:
                return f"CREATE VIEW [{schema}].[{view_name}] AS {row[0]}"
        elif db_type == DbType.SNOWFLAKE:
            qualified = f'"{schema}"."{view_name}"'
            cur.execute("SELECT GET_DDL('VIEW', %s)", (qualified,))
            row = cur.fetchone()
            if row:
                return row[0]
    except Exception:
        pass
    finally:
        cur.close()
    return None


def get_sequence_current_value(conn: Any, sequence_ref: str) -> int | None:
    """Return the last value of a PostgreSQL sequence. Returns None on error."""
    if "." in sequence_ref:
        schema, seq_name = sequence_ref.split(".", 1)
    else:
        schema, seq_name = "public", sequence_ref
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT last_value FROM "{schema}"."{seq_name}"')
        row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None
    finally:
        cur.close()


def get_sequences(
    conn: Any, db_type: DbType, schemas: list[str] | None = None
) -> list[str]:
    """Return list of sequences (PostgreSQL only, others return [])."""
    if db_type != DbType.POSTGRESQL:
        return []
    cur = conn.cursor()
    if schemas:
        placeholders = ",".join(["%s"] * len(schemas))
        cur.execute(f"""
            SELECT sequence_schema || '.' || sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema IN ({placeholders})
            ORDER BY sequence_schema, sequence_name
        """, tuple(schemas))
    else:
        cur.execute("""
            SELECT sequence_schema || '.' || sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY sequence_schema, sequence_name
        """)
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    return result


def get_indexes(
    conn: Any, db_type: DbType, database: str, schemas: list[str] | None = None
) -> list[str]:
    """Return list of index names."""
    cur = conn.cursor()
    if db_type == DbType.POSTGRESQL:
        if schemas:
            placeholders = ",".join(["%s"] * len(schemas))
            cur.execute(f"""
                SELECT indexname FROM pg_indexes
                WHERE schemaname IN ({placeholders})
                ORDER BY indexname
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname NOT IN ('pg_catalog', 'pg_toast')
                ORDER BY indexname
            """)
    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT DISTINCT index_name FROM information_schema.statistics
            WHERE table_schema = %s ORDER BY index_name
        """, (database,))
    elif db_type == DbType.SQLITE:
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'")
        result = [row[0] for row in cur.fetchall()]
        cur.close()
        return result
    elif db_type == DbType.SQLSERVER:
        if schemas:
            placeholders = ",".join(["?"] * len(schemas))
            cur.execute(f"""
                SELECT DISTINCT i.name FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name IN ({placeholders}) AND i.name IS NOT NULL
                ORDER BY i.name
            """, tuple(schemas))
        else:
            cur.execute("""
                SELECT DISTINCT i.name FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE i.name IS NOT NULL ORDER BY i.name
            """)
        result = [row[0] for row in cur.fetchall()]
        cur.close()
        return result
    else:
        cur.close()
        return []

    result = [row[0] for row in cur.fetchall()]
    cur.close()
    return result


def get_columns(
    conn: Any, db_type: DbType, table_name: str, database: str,
    schema: str | None = None,
) -> list[ColumnInfo]:
    """Return column information for a table."""
    cur = conn.cursor()
    pg_schema = schema or "public"

    if db_type == DbType.POSTGRESQL:
        # typtype comes from pg_attribute → pg_type (authoritative). Fallback: join via udt_schema/udt_name
        # (the udt join alone can miss ENUMs/domain types in some catalogs, leaving typtype NULL).
        cur.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk,
                c.udt_name,
                COALESCE(ty.typtype, t_udt.typtype) AS pg_typtype
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_namespace tbl_ns ON tbl_ns.nspname = c.table_schema
            LEFT JOIN pg_catalog.pg_class cl
                ON cl.relnamespace = tbl_ns.oid
                AND cl.relname = c.table_name
                AND cl.relkind IN ('r', 'p', 'f', 'm')
            LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = cl.oid
                AND a.attname = c.column_name
                AND a.attnum > 0
                AND NOT a.attisdropped
            LEFT JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
            LEFT JOIN pg_catalog.pg_namespace udt_ns ON udt_ns.nspname = c.udt_schema
            LEFT JOIN pg_catalog.pg_type t_udt
                ON t_udt.typnamespace = udt_ns.oid AND t_udt.typname = c.udt_name
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (pg_schema, table_name, pg_schema, table_name))

        columns = []
        for row in cur.fetchall():
            col_name, data_type, is_nullable, default, is_pk, udt_name, pg_typtype = row
            is_user_defined = False

            # Resolve ARRAY type: udt_name starts with '_' for array element type
            if data_type == "ARRAY" and udt_name:
                element_type = udt_name.lstrip("_")
                data_type = f"{element_type}[]"
            # Resolve USER-DEFINED type: use the actual type name (e.g. vector, geometry, ENUM labels)
            elif data_type == "USER-DEFINED" and udt_name:
                data_type = udt_name
                # Coerce ENUMs and DOMAINs to TEXT on the target; keep base/extension types (e.g. vector) native.
                if pg_typtype in ("e", "d"):
                    is_user_defined = True

            columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                is_nullable=is_nullable == "YES",
                default=str(default) if default else None,
                is_primary_key=bool(is_pk),
                is_user_defined=is_user_defined,
            ))
        cur.close()
        return columns

    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE WHEN c.column_key = 'PRI' THEN 1 ELSE 0 END as is_pk
            FROM information_schema.columns c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (database, table_name))
    elif db_type == DbType.SQLITE:
        cur.execute(f'PRAGMA table_info("{table_name}")')
        columns = []
        for row in cur.fetchall():
            # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
            columns.append(ColumnInfo(
                name=row[1],
                data_type=row[2] or "TEXT",
                is_nullable=row[3] == 0,
                default=str(row[4]) if row[4] else None,
                is_primary_key=row[5] != 0,
            ))
        cur.close()
        return columns
    elif db_type == DbType.SQLSERVER:
        schema = schema or "dbo"
        cur.execute("""
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END,
                c.DOMAIN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (schema, table_name, schema, table_name))
        columns = []
        for row in cur.fetchall():
            domain_name = row[5] if len(row) > 5 else None
            is_udt = bool(domain_name and str(domain_name).strip())
            columns.append(ColumnInfo(
                name=row[0],
                data_type=row[1],
                is_nullable=row[2] == "YES",
                default=str(row[3]) if row[3] else None,
                is_primary_key=bool(row[4]),
                is_user_defined=is_udt,
            ))
        cur.close()
        return columns
    else:  # Snowflake
        # Resolve schema: use provided or CURRENT_SCHEMA() for unqualified table refs
        if not schema:
            cur.execute("SELECT CURRENT_SCHEMA()")
            schema = (cur.fetchone()[0] or "PUBLIC").strip('"')
        qualified = f'"{schema}"."{table_name}"'
        cur.execute(f'DESCRIBE TABLE {qualified}')
        columns = []
        for row in cur.fetchall():
            # DESCRIBE output: name, type, kind, null?, default, primary key, ...
            columns.append(ColumnInfo(
                name=row[0],
                data_type=row[1],
                is_nullable=row[3] == "Y" if len(row) > 3 else True,
                default=row[4] if len(row) > 4 and row[4] else None,
                is_primary_key=row[5] == "Y" if len(row) > 5 else False,
            ))
        cur.close()
        return columns

    columns = []
    for row in cur.fetchall():
        raw_type = row[1]
        is_udt = raw_type.lower() in ("enum", "set") if isinstance(raw_type, str) else False
        columns.append(ColumnInfo(
            name=row[0],
            data_type=raw_type,
            is_nullable=row[2] == "YES",
            default=str(row[3]) if row[3] else None,
            is_primary_key=bool(row[4]),
            is_user_defined=is_udt,
        ))
    cur.close()
    return columns


def get_row_count(
    conn: Any, db_type: DbType, table_name: str, schema: str | None = None,
    approximate: bool = False, database: str | None = None,
) -> int:
    """Return row count for a table. When approximate=True, use fast stats for large tables."""
    cur = conn.cursor()
    try:
        if db_type == DbType.POSTGRESQL:
            if approximate:
                cur.execute("""
                    SELECT reltuples::bigint FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
                """, (schema or "public", table_name))
                row = cur.fetchone()
                count = int(row[0]) if row and row[0] else 0
            else:
                qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                cur.execute(f'SELECT COUNT(*) FROM {qualified}')
                count = cur.fetchone()[0]
        elif db_type == DbType.MYSQL:
            if approximate:
                db = schema or database or ""
                cur.execute("""
                    SELECT table_rows FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (db or "unknown", table_name))
                row = cur.fetchone()
                count = int(row[0]) if row and row[0] is not None else 0
            else:
                cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = cur.fetchone()[0]
        elif db_type == DbType.SQLITE:
            qualified = f'"{table_name}"'
            cur.execute(f'SELECT COUNT(*) FROM {qualified}')
            count = cur.fetchone()[0]
        elif db_type == DbType.SQLSERVER:
            schema = schema or "dbo"
            qualified = f"[{schema}].[{table_name}]"
            cur.execute(f'SELECT COUNT(*) FROM {qualified}')
            count = cur.fetchone()[0]
        else:
            qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            cur.execute(f'SELECT COUNT(*) FROM {qualified}')
            count = cur.fetchone()[0]
    except Exception:
        count = 0
    cur.close()
    return int(count)


def introspect_schema(
    config: ConnectionConfig,
    schemas: list[str] | None = None,
    tables_only: bool = False,
    for_mapping: bool = False,
) -> dict:
    """Full schema introspection returning tables, views, indexes, sequences.
    When tables_only=True, returns only table names (no columns, row counts, views, indexes, sequences).
    When for_mapping=True, returns tables with columns but skips row_count, views, indexes, sequences (faster for Schema Mapping UI)."""
    conn = get_connection(config)

    table_refs = get_table_names(conn, config.db_type, config.database, schemas)
    tables = []
    if tables_only:
        tables = [TableInfo(name=t_ref, columns=[], row_count=0) for t_ref in table_refs]
        conn.close()
        return {
            "tables": tables,
            "views": [],
            "indexes": [],
            "sequences": [],
        }
    for t_ref in table_refs:
        s, t = _parse_table_ref(t_ref, config.db_type)
        cols = get_columns(conn, config.db_type, t, config.database, schema=s)
        if for_mapping:
            rc = 0
        else:
            rc = get_row_count(conn, config.db_type, t, schema=s)
        tables.append(TableInfo(name=t_ref, columns=cols, row_count=rc))

    if for_mapping:
        views, indexes, sequences = [], [], []
    else:
        views = get_views(conn, config.db_type, config.database, schemas)
        indexes = get_indexes(conn, config.db_type, config.database, schemas)
        sequences = get_sequences(conn, config.db_type, schemas)

    conn.close()
    return {
        "tables": tables,
        "views": views,
        "indexes": indexes,
        "sequences": sequences,
    }


def get_table_list(
    config: ConnectionConfig, schemas: list[str] | None = None
) -> list[TableInfo]:
    """Lightweight: return only table refs (no columns). For dedicated /tables endpoint."""
    conn = get_connection(config)
    table_refs = get_table_names(conn, config.db_type, config.database, schemas)
    tables = [TableInfo(name=ref, columns=[], row_count=0) for ref in table_refs]
    conn.close()
    return tables


def get_tables_columns(
    config: ConnectionConfig, table_refs: list[str]
) -> list[TableInfo]:
    """Return column info for the given table refs only (no row counts). For Schema Mapping."""
    if not table_refs:
        return []
    conn = get_connection(config)
    tables: list[TableInfo] = []
    for t_ref in table_refs:
        s, t = _parse_table_ref(t_ref, config.db_type)
        cols = get_columns(conn, config.db_type, t, config.database, schema=s)
        tables.append(TableInfo(name=t_ref, columns=cols, row_count=0))
    conn.close()
    return tables


def fetch_rows(
    conn: Any, db_type: DbType, table_name: str,
    batch_size: int = 1000, schema: str | None = None,
):
    """Generator that yields batches of rows from a table using server-side cursors where possible."""
    if db_type == DbType.POSTGRESQL:
        cur = conn.cursor(name="fetch_cur")
        cur.itersize = batch_size
        qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        cur.execute(f'SELECT * FROM {qualified}')
    elif db_type == DbType.MYSQL:
        import pymysql.cursors
        cur = conn.cursor(cursor=pymysql.cursors.SSCursor)
        cur.execute(f"SELECT * FROM `{table_name}`")
    elif db_type == DbType.SQLITE:
        cur = conn.cursor()
        qualified = f'"{table_name}"'
        cur.execute(f'SELECT * FROM {qualified}')
    elif db_type == DbType.SQLSERVER:
        cur = conn.cursor()
        schema = schema or "dbo"
        qualified = f"[{schema}].[{table_name}]"
        cur.execute(f'SELECT * FROM {qualified}')
    else:  # Snowflake
        cur = conn.cursor()
        qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        cur.execute(f'SELECT * FROM {qualified}')

    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows

    cur.close()


def get_column_names(
    conn: Any, db_type: DbType, table_name: str, schema: str | None = None
) -> list[str]:
    """Return list of column names for a table (uses cursor.description)."""
    cur = conn.cursor()
    if db_type == DbType.MYSQL:
        cur.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
    elif db_type == DbType.SQLITE:
        cur.execute(f'SELECT * FROM "{table_name}" LIMIT 0')
    elif db_type == DbType.SQLSERVER:
        schema = schema or "dbo"
        cur.execute(f'SELECT TOP 0 * FROM [{schema}].[{table_name}]')
    else:
        qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        cur.execute(f'SELECT * FROM {qualified} LIMIT 0')
    names = [desc[0] for desc in cur.description] if cur.description else []
    cur.close()
    return names


def get_foreign_keys(
    conn: Any, db_type: DbType, database: str,
    schemas: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Return list of (child_table, parent_table) FK edges.

    Table names are schema-qualified where applicable (e.g. 'public.orders').
    """
    cur = conn.cursor()
    edges: list[tuple[str, str]] = []

    if db_type == DbType.POSTGRESQL:
        query = """
            SELECT
                tc.table_schema || '.' || tc.table_name     AS child,
                ccu.table_schema || '.' || ccu.table_name   AS parent
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.constraint_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
        """
        if schemas:
            placeholders = ",".join(["%s"] * len(schemas))
            query += f" AND tc.table_schema IN ({placeholders})"
            cur.execute(query, tuple(schemas))
        else:
            query += """
                AND tc.table_schema NOT IN (
                    'pg_catalog', 'information_schema', 'pg_toast'
                )
            """
            cur.execute(query)

    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT
                CONCAT(table_name) AS child,
                CONCAT(referenced_table_name) AS parent
            FROM information_schema.key_column_usage
            WHERE referenced_table_name IS NOT NULL
              AND table_schema = %s
        """, (database,))
    elif db_type == DbType.SQLITE:
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cur.fetchall()]
        for tbl in tables:
            cur.execute(f'PRAGMA foreign_key_list("{tbl}")')
            for row in cur.fetchall():
                # table, from, to, ... ; row[0]=table (parent), row[2]=from col, row[3]=to col
                parent = row[0]
                if parent:
                    edges.append((f"main.{tbl}", f"main.{parent}"))
        cur.close()
        return list(set(edges))
    elif db_type == DbType.SQLSERVER:
        cur.execute("""
            SELECT
                OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS child,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS parent
            FROM sys.foreign_keys fk
        """)
        seen = set()
        for row in cur.fetchall():
            child, parent = row[0], row[1]
            if child != parent and (child, parent) not in seen:
                edges.append((child, parent))
                seen.add((child, parent))
        cur.close()
        return edges
    else:  # Snowflake — limited FK support, return empty
        cur.close()
        return []

    seen = set()
    for row in cur.fetchall():
        child, parent = row[0], row[1]
        if child != parent and (child, parent) not in seen:
            edges.append((child, parent))
            seen.add((child, parent))

    cur.close()
    return edges


def get_primary_key_columns(
    conn: Any, db_type: DbType, table_name: str,
    database: str, schema: str | None = None,
) -> list[str]:
    """Return list of primary key column names for a table."""
    cur = conn.cursor()
    pk_cols: list[str] = []

    if db_type == DbType.POSTGRESQL:
        pg_schema = schema or "public"
        cur.execute("""
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
                ON tc.constraint_name = ku.constraint_name
                AND tc.constraint_schema = ku.constraint_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY ku.ordinal_position
        """, (pg_schema, table_name))
        pk_cols = [row[0] for row in cur.fetchall()]

    elif db_type == DbType.MYSQL:
        cur.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s
              AND table_name = %s
              AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """, (database, table_name))
        pk_cols = [row[0] for row in cur.fetchall()]

    elif db_type == DbType.SQLITE:
        cur.execute(f'PRAGMA table_info("{table_name}")')
        pk_cols = [row[1] for row in cur.fetchall() if row[5] != 0]
    elif db_type == DbType.SQLSERVER:
        schema = schema or "dbo"
        cur.execute("""
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (schema, table_name))
        pk_cols = [row[0] for row in cur.fetchall()]
    else:  # Snowflake
        try:
            qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            cur.execute(f'SHOW PRIMARY KEYS IN TABLE {qualified}')
            pk_cols = [row[4] for row in cur.fetchall()]
        except Exception:
            pass

    cur.close()
    return pk_cols

