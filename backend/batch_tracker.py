"""
Batch tracker — checkpoint metadata for idempotent, resumable migration.
Creates a `_databridge_batch_meta` table on the target database to track
batch-level progress, enabling crash-safe resume.
"""
from __future__ import annotations

from typing import Any
from datetime import datetime

from models import DbType


_META_TABLE = "_databridge_batch_meta"


def _q(name: str, db_type: DbType) -> str:
    if db_type == DbType.MYSQL:
        return f"`{name}`"
    if db_type == DbType.SQLSERVER:
        return f"[{name}]"
    return f'"{name}"'


def init_tracker(conn: Any, db_type: DbType) -> None:
    """Create the batch metadata table if it doesn't exist."""
    cur = conn.cursor()
    qt = _q(_META_TABLE, db_type)

    if db_type == DbType.MYSQL:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qt} (
                table_name VARCHAR(255) NOT NULL,
                batch_index INT NOT NULL,
                row_count INT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, batch_index)
            )
        """)
    elif db_type == DbType.POSTGRESQL:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qt} (
                table_name TEXT NOT NULL,
                batch_index INT NOT NULL,
                row_count INT DEFAULT 0,
                status TEXT DEFAULT 'pending',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, batch_index)
            )
        """)
    elif db_type == DbType.SQLITE:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qt} (
                table_name TEXT NOT NULL,
                batch_index INTEGER NOT NULL,
                row_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, batch_index)
            )
        """)
    elif db_type == DbType.SQLSERVER:
        # Use schema-qualified name for OBJECT_ID check
        full_name = f"dbo.{_META_TABLE}"
        cur.execute(f"""
            IF OBJECT_ID(N'{full_name}', N'U') IS NULL
            BEGIN
                CREATE TABLE {qt} (
                    table_name NVARCHAR(500) NOT NULL,
                    batch_index INT NOT NULL,
                    row_count INT DEFAULT 0,
                    status NVARCHAR(20) DEFAULT 'pending',
                    updated_at DATETIME2 DEFAULT GETDATE(),
                    PRIMARY KEY (table_name, batch_index)
                )
            END
        """)
    else:  # Snowflake
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qt} (
                table_name VARCHAR NOT NULL,
                batch_index INT NOT NULL,
                row_count INT DEFAULT 0,
                status VARCHAR DEFAULT 'pending',
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (table_name, batch_index)
            )
        """)

    conn.commit()
    cur.close()


def register_batches(
    conn: Any, db_type: DbType, table_name: str, num_batches: int,
) -> None:
    """Register pending batch rows for a table."""
    cur = conn.cursor()
    qt = _q(_META_TABLE, db_type)

    # Clear any existing rows for this table (in case of re-run)
    ph = _ph(db_type)
    cur.execute(f"DELETE FROM {qt} WHERE table_name = {ph}", (table_name,))

    for i in range(num_batches):
        cur.execute(
            f"INSERT INTO {qt} (table_name, batch_index, status) VALUES ({ph}, {ph}, {ph})",
            (table_name, i, "pending"),
        )

    conn.commit()
    cur.close()


def _ph(db_type: DbType) -> str:
    return "?" if db_type in (DbType.SQLITE, DbType.SQLSERVER) else "%s"


def mark_batch(
    conn: Any, db_type: DbType, table_name: str,
    batch_index: int, status: str, row_count: int = 0,
) -> None:
    """Update a batch's status and row count."""
    cur = conn.cursor()
    qt = _q(_META_TABLE, db_type)
    ph = _ph(db_type)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute(
        f"UPDATE {qt} SET status = {ph}, row_count = {ph}, updated_at = {ph} "
        f"WHERE table_name = {ph} AND batch_index = {ph}",
        (status, row_count, now, table_name, batch_index),
    )
    conn.commit()
    cur.close()


def get_completed_batches(
    conn: Any, db_type: DbType, table_name: str,
) -> set[int]:
    """Return set of batch indexes already marked as 'done'."""
    cur = conn.cursor()
    qt = _q(_META_TABLE, db_type)
    ph = _ph(db_type)

    cur.execute(
        f"SELECT batch_index FROM {qt} WHERE table_name = {ph} AND status = 'done'",
        (table_name,),
    )
    result = {row[0] for row in cur.fetchall()}
    cur.close()
    return result


def cleanup_tracker(conn: Any, db_type: DbType) -> None:
    """Drop the batch metadata table after successful migration."""
    cur = conn.cursor()
    qt = _q(_META_TABLE, db_type)
    try:
        cur.execute(f"DROP TABLE IF EXISTS {qt}")
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    cur.close()
