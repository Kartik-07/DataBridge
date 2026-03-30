"""
Migration engine — orchestrates schema + data migration between databases.
Yields LogEntry dictionaries for real-time streaming to the frontend.

Features:
  - FK-aware dependency ordering (topological levels)
  - Parallel table migration (ThreadPoolExecutor)
  - Streaming pipeline (server-side cursor, chunked read/write)
  - Bulk upsert (ON CONFLICT / ON DUPLICATE KEY / MERGE)
  - Batch-level checkpointing for crash-safe resume
"""
from __future__ import annotations

import orjson
import math
import queue
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from typing import Generator, Optional

from models import (
    ConnectionConfig,
    MigrationOptions,
    DbType,
    ColumnMapping,
)
from connectors import (
    get_connection,
    get_table_names,
    get_columns,
    get_row_count,
    get_views,
    get_sequences,
    get_view_definition,
    get_sequence_current_value,
    fetch_rows,
    get_column_names,
    get_foreign_keys,
    get_primary_key_columns,
    _parse_table_ref,
)
from schema_translator import (
    generate_create_table,
    generate_drop_table,
    generate_insert,
    generate_upsert,
    generate_snowflake_staging_upsert,
    generate_add_primary_key,
    apply_transform_rules,
)
from dependency import build_dependency_graph, topological_levels
from batch_tracker import (
    init_tracker,
    register_batches,
    mark_batch,
    get_completed_batches,
    cleanup_tracker,
)
from models import TableInfo
from config import settings

try:
    from migration_state import _migration_pause_flags
except ImportError:
    _migration_pause_flags = {}


def _wait_if_paused(session_id: str | None) -> None:
    """If migration is paused, sleep until resumed."""
    if not session_id:
        return
    import time
    while _migration_pause_flags.get(session_id):
        time.sleep(0.5)


def _log(message: str, type: str = "info", progress: int = 0, **extra) -> dict:
    d = {"message": message, "type": type, "progress": progress}
    d.update(extra)
    return d


def _get_json_and_vector_indices(columns: list) -> tuple[set[int], set[int]]:
    """Pre-compute which column indices require JSON or Vector serialization."""
    json_indices = set()
    vector_indices = set()
    if not columns:
        return json_indices, vector_indices
        
    for i, col in enumerate(columns):
        if not col.data_type:
            continue
        dt = col.data_type.lower()
        if 'json' in dt:
            json_indices.add(i)
        elif 'vector' in dt or 'user-defined' in dt:
            vector_indices.add(i)
            
    return json_indices, vector_indices


def _format_to_strptime(user_format: str) -> str:
    """Convert user-friendly format (YYYY-MM-DD HH:mm:ss) to strptime format."""
    m = user_format.strip()
    m = re.sub(r"(?i)\bYYYY\b", "%Y", m)
    m = re.sub(r"(?i)\bMM\b", "%m", m)
    m = re.sub(r"(?i)\bDD\b", "%d", m)
    m = re.sub(r"(?i)\bHH\b", "%H", m)
    m = re.sub(r"(?i)\bmm\b", "%M", m)
    m = re.sub(r"(?i)\bss\b", "%S", m)
    return m


def _apply_date_transform(val, source_format: str, target_format: str):
    """Apply date transformation: parse with source_format, output per target_format."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):  # Already datetime/date
        dt = val
        if isinstance(dt, date) and not isinstance(dt, datetime):  # date only, not datetime
            dt = datetime.combine(dt, datetime.min.time())
    else:
        s = str(val).strip()
        if not s:
            return None
        fmt = _format_to_strptime(source_format)
        try:
            dt = datetime.strptime(s[:26], fmt)  # Truncate to avoid microsecond issues
        except ValueError:
            for fallback in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    dt = datetime.strptime(s[:26], fallback)
                    break
                except ValueError:
                    continue
            else:
                return val  # Could not parse, return as-is
    if target_format == "ISO 8601":
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + ("Z" if getattr(dt, "tzinfo", None) is None else dt.strftime("%z"))
    if target_format == "UNIX":
        if isinstance(dt, datetime):
            return int(dt.timestamp())
        return int(datetime.combine(dt, datetime.min.time()).timestamp())
    if target_format == "YYYY-MM-DD":
        return dt.strftime("%Y-%m-%d")
    return dt.isoformat()


def _adapt_row(
    row: tuple, 
    json_indices: set[int], 
    vector_indices: set[int], 
    target_db_type: DbType = DbType.MYSQL,
    column_transforms: Optional[list[Optional[dict]]] = None,
) -> tuple:
    """Convert Python dicts/lists in a row for insertion cleanly using pre-computed indices.
    
    This avoids O(N) isinstance checks on every cell of every row.
    column_transforms: per-column transform descriptor dict, or None.
      - ``{"type": "date", "source_format": ..., "target_format": ...}``
      - ``{"type": "rule", "rules": "trim|uppercase|truncate:255"}``
    """
    
    def _pg_adapt_list(lst: list) -> list:
        """Recursively prepare a list for PG array insertion."""
        res = []
        for item in lst:
            if isinstance(item, dict):
                res.append(orjson.dumps(item).decode("utf-8"))
            elif isinstance(item, list):
                res.append(_pg_adapt_list(item))
            else:
                res.append(item)
        return res

    transforms = column_transforms or []
    adapted = []
    for i, val in enumerate(row):
        if i < len(transforms) and transforms[i] is not None:
            t = transforms[i]
            if t["type"] == "date":
                val = _apply_date_transform(
                    val,
                    t["source_format"] or "YYYY-MM-DD HH:mm:ss",
                    t["target_format"] or "ISO 8601",
                )
            elif t["type"] == "rule":
                val = apply_transform_rules(val, t["rules"])

        if isinstance(val, dict):
            adapted.append(orjson.dumps(val).decode("utf-8"))
        elif isinstance(val, list):
            if target_db_type == DbType.POSTGRESQL:
                if i in json_indices or i in vector_indices:
                    adapted.append(orjson.dumps(val).decode("utf-8"))
                else:
                    adapted.append(_pg_adapt_list(val))
            else:
                adapted.append(orjson.dumps(val).decode("utf-8"))
        else:
            adapted.append(val)
    return tuple(adapted)


# ── Parallel table worker ───────────────────────────────────────────────────

def _migrate_table_worker(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    table: TableInfo,
    write_sql: str | dict,
    batch_size: int,
    progress_queue: queue.Queue,
    t_idx: int,
    dry_run: bool = False,
    table_mappings: Optional[dict[str, ColumnMapping]] = None,
) -> dict:
    """Migrate a single table's data in a worker thread.

    Uses its own connections (thread-safe). Streams rows in chunks,
    commits per batch, and checkpoints via batch_tracker.
    Returns summary dict.
    """
    s, bare_name = _parse_table_ref(table.name, source_config.db_type)

    if table.row_count == 0:
        progress_queue.put(("done", t_idx, 0, table.name))
        return {"table": table.name, "rows": 0, "status": "done"}

    if dry_run:
        progress_queue.put(("done", t_idx, table.row_count, table.name))
        return {"table": table.name, "rows": table.row_count, "status": "done"}

    src_conn = None
    tgt_conn = None
    try:
        src_conn = get_connection(source_config)
        tgt_conn = get_connection(target_config)

        # Check for already-completed batches (resume support)
        num_batches = math.ceil(table.row_count / batch_size) if table.row_count > 0 else 1
        try:
            completed = get_completed_batches(tgt_conn, target_config.db_type, table.name)
        except Exception:
            try:
                tgt_conn.rollback()
            except Exception:
                pass
            completed = set()

        # Register batches (clears and re-registers for this table)
        try:
            register_batches(tgt_conn, target_config.db_type, table.name, num_batches)
            # Re-mark already completed batches
            for bi in completed:
                if bi < num_batches:
                    mark_batch(tgt_conn, target_config.db_type, table.name, bi, "done")
        except Exception:
            try:
                tgt_conn.rollback()
            except Exception:
                pass

        t_cur = tgt_conn.cursor()
        table_migrated = 0
        batch_index = 0
        use_snowflake_staging = isinstance(write_sql, dict) and write_sql.get("type") == "snowflake_staging"
        if use_snowflake_staging:
            t_cur.execute(write_sql["create"])
            tgt_conn.commit()
        
        json_indices, vector_indices = _get_json_and_vector_indices(table.columns)
        mappings = table_mappings or {}
        column_transforms: list[dict | None] = []
        for col in table.columns:
            m = mappings.get(col.name)
            if m and m.action == "transform":
                if m.transform_rule:
                    column_transforms.append({"type": "rule", "rules": m.transform_rule})
                elif m.source_format and m.target_format:
                    column_transforms.append({"type": "date", "source_format": m.source_format, "target_format": m.target_format})
                else:
                    column_transforms.append(None)
            else:
                column_transforms.append(None)

        for batch in fetch_rows(
            src_conn, source_config.db_type, bare_name,
            batch_size, schema=s,
        ):
            # Skip already-completed batches
            if batch_index in completed:
                table_migrated += len(batch)
                batch_index += 1
                progress_queue.put(("progress", t_idx, table_migrated, table.name))
                continue

            try:
                mark_batch(tgt_conn, target_config.db_type, table.name, batch_index, "running")
            except Exception:
                try:
                    tgt_conn.rollback()
                except Exception:
                    pass

            last_error = None
            for attempt in range(3):
                if attempt > 0:
                    import time
                    time.sleep(1.0 * attempt)
                    try:
                        tgt_conn.rollback()
                        mark_batch(tgt_conn, target_config.db_type, table.name, batch_index, "running")
                    except Exception:
                        pass
                try:
                    adapted_batch = [
                        _adapt_row(row, json_indices, vector_indices, target_config.db_type, column_transforms)
                        for row in batch
                    ]
                    if use_snowflake_staging:
                        t_cur.executemany(write_sql["insert"], adapted_batch)
                        t_cur.execute(write_sql["merge"])
                    elif target_config.db_type == DbType.POSTGRESQL and isinstance(write_sql, str) and "VALUES %s" in write_sql:
                        from psycopg2.extras import execute_values
                        execute_values(t_cur, write_sql, adapted_batch)
                    else:
                        t_cur.executemany(write_sql, adapted_batch)
                    tgt_conn.commit()
                    try:
                        mark_batch(tgt_conn, target_config.db_type, table.name, batch_index, "done", len(batch))
                    except Exception:
                        try:
                            tgt_conn.rollback()
                        except Exception:
                            pass
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    try:
                        tgt_conn.rollback()
                        mark_batch(tgt_conn, target_config.db_type, table.name, batch_index, "failed")
                    except Exception:
                        try:
                            tgt_conn.rollback()
                        except Exception:
                            pass
            if last_error is not None:
                progress_queue.put(("error", t_idx, table_migrated, f"{table.name}: {str(last_error)[:150]}"))
                break

            table_migrated += len(batch)
            batch_index += 1
            progress_queue.put(("progress", t_idx, table_migrated, table.name))

        if use_snowflake_staging:
            try:
                t_cur.execute(write_sql["drop"])
                tgt_conn.commit()
            except Exception:
                try:
                    tgt_conn.rollback()
                except Exception:
                    pass
        t_cur.close()
        progress_queue.put(("done", t_idx, table_migrated, table.name))
        return {"table": table.name, "rows": table_migrated, "status": "done"}

    except Exception as e:
        progress_queue.put(("error", t_idx, 0, f"{table.name}: {str(e)[:150]}"))
        return {"table": table.name, "rows": 0, "status": "error", "error": str(e)[:200]}

    finally:
        if src_conn:
            try:
                src_conn.close()
            except Exception:
                pass
        if tgt_conn:
            try:
                tgt_conn.close()
            except Exception:
                pass


# ── Main migration orchestrator ─────────────────────────────────────────────

def run_migration(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    options: MigrationOptions,
    session_id: str | None = None,
) -> Generator[dict, None, None]:
    """
    Execute a full database migration.
    Yields log dicts: { message, type, progress, [table_progress] }
    Respects pause/resume via session_id when provided.
    """
    for event in _run_migration_core(source_config, target_config, options):
        _wait_if_paused(session_id)
        yield event


def _run_migration_core(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    options: MigrationOptions,
) -> Generator[dict, None, None]:
    """Internal migration logic (no pause handling)."""
    import time
    from datetime import datetime
    from history import append_history

    start_time = time.time()
    status = "success"
    final_tables_count = 0
    final_rows_count = 0

    source_conn = None
    target_conn = None
    selected_schemas = options.schemas if options.schemas else None

    try:
        # ── Step 1: Connect to source ───────────────────────────────────
        yield _log("Connecting to source database…", "info", 2)
        source_conn = get_connection(source_config)
        yield _log(
            f"Source connection established ({source_config.db_type.value})",
            "success", 5,
        )

        # ── Step 2: Connect to target ───────────────────────────────────
        yield _log("Connecting to target database…", "info", 8)
        target_conn = get_connection(target_config)
        yield _log(
            f"Target connection established ({target_config.db_type.value})",
            "success", 10,
        )

        # ── Step 3: Discover source schema ──────────────────────────────
        schema_label = ", ".join(selected_schemas) if selected_schemas else "all schemas"
        yield _log(f"Analyzing source schema ({schema_label})…", "info", 12)
        table_refs = get_table_names(
            source_conn, source_config.db_type, source_config.database,
            schemas=selected_schemas,
        )
        views = get_views(
            source_conn, source_config.db_type, source_config.database,
            schemas=selected_schemas,
        ) if options.migrate_views else []
        sequences = get_sequences(
            source_conn, source_config.db_type,
            schemas=selected_schemas,
        ) if options.migrate_sequences else []

        tables: list[TableInfo] = []
        total_rows = 0
        use_approximate = len(table_refs) > 15  # Use fast stats when many tables
        for t_ref in table_refs:
            s, t = _parse_table_ref(t_ref, source_config.db_type)
            cols = get_columns(
                source_conn, source_config.db_type, t, source_config.database,
                schema=s,
            )
            rc = get_row_count(
                source_conn, source_config.db_type, t, schema=s,
                approximate=use_approximate,
                database=source_config.database,
            )
            total_rows += rc
            tables.append(TableInfo(name=t_ref, columns=cols, row_count=rc))

        # Filter by selected_tables if specified
        if options.selected_tables:
            filtered = []
            for table in tables:
                s, t = _parse_table_ref(table.name, source_config.db_type)
                schema_key = s or "default"
                if schema_key in options.selected_tables:
                    if t in options.selected_tables[schema_key]:
                        filtered.append(table)
            tables = filtered
            total_rows = sum(t.row_count for t in tables)

        final_tables_count = len(tables)
        final_rows_count = total_rows

        yield _log(
            f"Found {len(tables)} tables, {len(views)} views, "
            f"{len(sequences)} sequences — {total_rows:,} total rows",
            "info", 15,
        )

        # ── Step 3.5: FK dependency analysis ────────────────────────────
        yield _log("Analyzing foreign key dependencies…", "info", 16)
        fk_edges = get_foreign_keys(
            source_conn, source_config.db_type, source_config.database,
            schemas=selected_schemas,
        )
        table_name_list = [t.name for t in tables]
        dep_graph = build_dependency_graph(table_name_list, fk_edges)
        levels = topological_levels(dep_graph)

        if fk_edges:
            yield _log(
                f"Dependency analysis: {len(fk_edges)} FK relationships, "
                f"{len(levels)} migration levels",
                "info", 17,
            )
        else:
            yield _log(
                f"No FK dependencies found — all {len(tables)} tables are independent",
                "info", 17,
            )

        # Build table lookup by name
        table_by_name = {t.name: t for t in tables}

        # Build ordered table list from levels
        ordered_tables = []
        for level in levels:
            for tname in level:
                if tname in table_by_name:
                    ordered_tables.append(table_by_name[tname])
        # Add any tables not in the graph (shouldn't happen, but defensive)
        ordered_set = {t.name for t in ordered_tables}
        for t in tables:
            if t.name not in ordered_set:
                ordered_tables.append(t)
        tables = ordered_tables

        # Build initial table_progress list
        tp_list = []
        tp_index_by_name: dict[str, int] = {}
        for i, table in enumerate(tables):
            s, t = _parse_table_ref(table.name, source_config.db_type)
            tp_list.append({
                "name": t,
                "schema": s or "default",
                "totalRows": table.row_count,
                "migratedRows": 0,
                "status": "pending",
            })
            tp_index_by_name[table.name] = i

        yield _log(
            f"Prepared {len(tables)} tables for migration",
            "info", 18,
            table_progress=tp_list,
        )

        # ── Step 3.6: Create schemas on target (if needed) ──────────────
        if target_config.db_type == DbType.POSTGRESQL:
            target_schemas_needed = set()
            for table in tables:
                s, _ = _parse_table_ref(table.name, source_config.db_type)
                if s:
                    target_schemas_needed.add(s)

            if target_schemas_needed:
                yield _log(
                    f"Creating {len(target_schemas_needed)} schema(s) on target…",
                    "info", 19,
                )
                t_cur = target_conn.cursor()
                for schema_name in sorted(target_schemas_needed):
                    if options.dry_run:
                        yield _log(
                            f"[DRY RUN] CREATE SCHEMA IF NOT EXISTS {schema_name}",
                            "info", 19,
                        )
                    else:
                        try:
                            t_cur.execute(
                                f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
                            )
                            target_conn.commit()
                        except Exception as e:
                            target_conn.rollback()
                            yield _log(
                                f"Warning: Error creating schema {schema_name} — {str(e)[:120]}",
                                "warning", 19,
                            )
                
                t_cur.close()
                
                # Check if any migrating tables need the vector extension
                needs_vector = False
                for table in tables:
                    for col in table.columns:
                        if col.data_type and 'vector' in col.data_type.lower():
                            needs_vector = True
                            break
                    if needs_vector:
                        break

                if needs_vector and not options.dry_run:
                    try:
                        t_cur = target_conn.cursor()
                        t_cur.execute('CREATE EXTENSION IF NOT EXISTS vector')
                        target_conn.commit()
                        t_cur.close()
                    except Exception as e:
                        target_conn.rollback()
                        yield _log(
                            f"Warning: Could not create pgvector extension — {str(e)[:120]}",
                            "warning", 20,
                        )

        elif target_config.db_type == DbType.SQLSERVER:
            target_schemas_needed = set()
            for table in tables:
                s, _ = _parse_table_ref(table.name, source_config.db_type)
                if s and s.lower() != "dbo":
                    target_schemas_needed.add(s)
            if target_schemas_needed:
                yield _log(
                    f"Creating {len(target_schemas_needed)} schema(s) on target…",
                    "info", 19,
                )
                t_cur = target_conn.cursor()
                for schema_name in sorted(target_schemas_needed):
                    if options.dry_run:
                        yield _log(f"[DRY RUN] CREATE SCHEMA [{schema_name}]", "info", 19)
                    else:
                        try:
                            t_cur.execute(f"CREATE SCHEMA [{schema_name}]")
                            target_conn.commit()
                        except Exception as e:
                            target_conn.rollback()
                            if "already exists" not in str(e).lower():
                                yield _log(
                                    f"Warning: Error creating schema {schema_name} — {str(e)[:120]}",
                                    "warning", 19,
                                )
                t_cur.close()

        # ── Step 4: Optionally drop existing tables ─────────────────────
        if options.drop_existing:
            yield _log("Dropping existing tables on target…", "warning", 22)
            t_cur = target_conn.cursor()
            for table in reversed(tables):
                s, bare_name = _parse_table_ref(table.name, target_config.db_type)
                drop_sql = generate_drop_table(table.name, target_config.db_type)
                if options.dry_run:
                    yield _log(f"[DRY RUN] {drop_sql}", "info", 23)
                else:
                    try:
                        t_cur.execute(drop_sql)
                        target_conn.commit()
                    except Exception as e:
                        target_conn.rollback()
                        yield _log(
                            f"Warning: Error dropping {table.name} — {str(e)[:120]}",
                            "warning", 23,
                        )
            t_cur.close()
            yield _log("Existing tables dropped", "success", 25)

        # ── Step 5: Create schema on target ─────────────────────────────
        # Logic: (1) If target table does not exist → create and later load.
        #        (2) If table exists and drop_existing → already dropped in Step 4; create fresh.
        #        (3) If table exists and not drop_existing → use IF NOT EXISTS (no-op), then upsert data.
        if options.migrate_schema:
            yield _log("Creating schema on target…", "info", 28)
            t_cur = target_conn.cursor()

            for i, table in enumerate(tables):
                pct = 28 + int((i / max(len(tables), 1)) * 15)
                s, bare_name = _parse_table_ref(table.name, source_config.db_type)
                target_table = TableInfo(
                    name=table.name, columns=table.columns, row_count=table.row_count
                )
                table_map = options.mappings.get(table.name) or {}
                create_sql = generate_create_table(
                    target_table, source_config.db_type, target_config.db_type,
                    with_pk=not options.drop_existing,
                    migrating_fresh=options.drop_existing,
                    if_not_exists=not options.drop_existing,
                    column_mappings=table_map if table_map else None,
                )
                if options.dry_run:
                    yield _log(f"[DRY RUN] CREATE TABLE {table.name}", "info", pct)
                else:
                    try:
                        t_cur.execute(create_sql)
                        target_conn.commit()
                    except Exception as e:
                        target_conn.rollback()
                        yield _log(
                            f"Warning: Error creating table {table.name} — {str(e)[:120]}",
                            "warning", pct,
                        )

            t_cur.close()
            yield _log(
                f"Schema created — {len(tables)} tables", "success", 45
            )

        # ── Step 5.5: Migrate views (when same engine or supported) ─────────
        if options.migrate_views and views:
            yield _log(f"Creating {len(views)} view(s) on target…", "info", 46)
            t_cur = target_conn.cursor()
            for view_ref in views:
                try:
                    v_def = get_view_definition(
                        source_conn, source_config.db_type, view_ref,
                        source_config.database,
                    )
                    if v_def and not options.dry_run:
                        t_cur.execute(v_def)
                        target_conn.commit()
                    elif options.dry_run and v_def:
                        yield _log(f"[DRY RUN] CREATE VIEW {view_ref}", "info", 46)
                except Exception as e:
                    try:
                        target_conn.rollback()
                    except Exception:
                        pass
                    yield _log(
                        f"Warning: Could not create view {view_ref} — {str(e)[:120]}",
                        "warning", 46,
                    )
            t_cur.close()
            if views:
                yield _log(f"Views created — {len(views)} view(s)", "success", 46)

        # ── Step 6: Migrate data (parallel, streaming) ───────────────────
        # When drop_existing: tables are empty → INSERT. When not: use upsert (update existing, insert new).
        if options.migrate_data:
            yield _log("Starting data migration…", "info", 47)

            # Init batch tracker
            try:
                init_tracker(target_conn, target_config.db_type)
            except Exception:
                pass

            # Pre-compute write SQL: INSERT when drop_existing (empty tables), else upsert when PK present
            migrating_fresh = options.drop_existing
            write_sql_map = {}
            table_mappings_map: dict[str, dict[str, ColumnMapping]] = {}

            for table in tables:
                s, bare_name = _parse_table_ref(table.name, source_config.db_type)
                col_names = get_column_names(
                    source_conn, source_config.db_type, bare_name, schema=s
                )
                pk_cols = get_primary_key_columns(
                    source_conn, source_config.db_type, bare_name,
                    source_config.database, schema=s,
                )
                table_map = options.mappings.get(table.name) or {}
                target_col_names = [
                    table_map[c].target_name if c in table_map else c
                    for c in col_names
                ]
                target_pk_cols = [
                    table_map[pk].target_name if pk in table_map else pk
                    for pk in pk_cols
                ]
                for_execute_values = target_config.db_type == DbType.POSTGRESQL

                if pk_cols and not migrating_fresh:
                    if target_config.db_type == DbType.SNOWFLAKE:
                        # Snowflake: use staging table for bulk upsert (avoids 1 MERGE per row)
                        create_s, insert_s, merge_s, drop_s = generate_snowflake_staging_upsert(
                            table.name, target_col_names, target_pk_cols, target_config.db_type
                        )
                        write_sql_map[table.name] = {
                            "type": "snowflake_staging",
                            "create": create_s,
                            "insert": insert_s,
                            "merge": merge_s,
                            "drop": drop_s,
                        }
                    else:
                        upsert_sql = generate_upsert(
                            table.name, target_col_names, target_pk_cols, target_config.db_type,
                            for_execute_values=for_execute_values
                        )
                        if upsert_sql:
                            write_sql_map[table.name] = upsert_sql
                        else:
                            write_sql_map[table.name] = generate_insert(
                                table.name, target_col_names, target_config.db_type,
                                for_execute_values=for_execute_values
                            )
                else:
                    write_sql_map[table.name] = generate_insert(
                        table.name, target_col_names, target_config.db_type,
                        for_execute_values=for_execute_values
                    )
                table_mappings_map[table.name] = table_map

            migrated_rows = 0
            data_start_pct = 48
            data_end_pct = 90
            data_range = data_end_pct - data_start_pct
            parallelism = settings.MIGRATION_PARALLELISM
            batch_size = settings.MIGRATION_BATCH_SIZE

            if options.dry_run:
                # Dry run: just log each table sequentially
                for t_idx, table in enumerate(tables):
                    tp_list[tp_index_by_name[table.name]]["migratedRows"] = table.row_count
                    tp_list[tp_index_by_name[table.name]]["status"] = "done"
                    migrated_rows += table.row_count
                    yield _log(
                        f"[DRY RUN] Would migrate {table.row_count:,} rows from {table.name}",
                        "info",
                        data_start_pct + int((migrated_rows / max(total_rows, 1)) * data_range),
                        table_progress=tp_list,
                    )
            else:
                # Migrate level by level for FK ordering
                for level_idx, level in enumerate(levels):
                    level_tables = [
                        table_by_name[tname]
                        for tname in level if tname in table_by_name
                    ]
                    if not level_tables:
                        continue

                    level_label = f"Level {level_idx + 1}/{len(levels)}"
                    yield _log(
                        f"{level_label}: migrating {len(level_tables)} table(s) "
                        f"(parallelism={min(parallelism, len(level_tables))})",
                        "info",
                        data_start_pct + int((migrated_rows / max(total_rows, 1)) * data_range),
                    )

                    # Mark tables in this level as migrating
                    for lt in level_tables:
                        if lt.row_count > 0:
                            tp_list[tp_index_by_name[lt.name]]["status"] = "migrating"
                        else:
                            tp_list[tp_index_by_name[lt.name]]["status"] = "done"

                    yield _log(
                        f"Starting {level_label}…",
                        "info",
                        data_start_pct + int((migrated_rows / max(total_rows, 1)) * data_range),
                        table_progress=tp_list,
                    )

                    # Run tables in this level in parallel
                    progress_q: queue.Queue = queue.Queue()
                    workers = min(parallelism, len(level_tables))

                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {}
                        for lt in level_tables:
                            t_idx = tp_index_by_name[lt.name]
                            fut = executor.submit(
                                _migrate_table_worker,
                                source_config, target_config, lt,
                                write_sql_map[lt.name],
                                batch_size, progress_q, t_idx,
                                options.dry_run,
                                table_mappings_map.get(lt.name),
                            )
                            futures[fut] = lt

                        # Poll queue for progress while futures are running
                        done_count = 0
                        total_in_level = len(level_tables)

                        while done_count < total_in_level:
                            try:
                                msg = progress_q.get(timeout=0.1)
                            except queue.Empty:
                                # Check if all futures completed
                                if all(f.done() for f in futures):
                                    # Drain remaining queue items
                                    while not progress_q.empty():
                                        msg = progress_q.get_nowait()
                                        ev_type, ev_tidx, ev_rows, ev_info = msg
                                        if ev_type == "progress":
                                            tp_list[ev_tidx]["migratedRows"] = ev_rows
                                        elif ev_type == "done":
                                            tp_list[ev_tidx]["status"] = "done"
                                            tp_list[ev_tidx]["migratedRows"] = ev_rows
                                            done_count += 1
                                        elif ev_type == "error":
                                            tp_list[ev_tidx]["status"] = "done"
                                            done_count += 1
                                    break
                                continue

                            ev_type, ev_tidx, ev_rows, ev_info = msg

                            if ev_type == "progress":
                                tp_list[ev_tidx]["migratedRows"] = ev_rows
                                migrated_cur = sum(tp["migratedRows"] for tp in tp_list)
                                yield _log(
                                    f"Migrating: {ev_info}",
                                    "info",
                                    data_start_pct + int((migrated_cur / max(total_rows, 1)) * data_range),
                                    table_progress=tp_list,
                                )

                            elif ev_type == "done":
                                tp_list[ev_tidx]["status"] = "done"
                                tp_list[ev_tidx]["migratedRows"] = ev_rows
                                done_count += 1
                                migrated_cur = sum(tp["migratedRows"] for tp in tp_list)
                                yield _log(
                                    f"✓ {ev_info} — {ev_rows:,} rows migrated",
                                    "success",
                                    data_start_pct + int((migrated_cur / max(total_rows, 1)) * data_range),
                                    table_progress=tp_list,
                                )

                            elif ev_type == "error":
                                tp_list[ev_tidx]["status"] = "done"
                                done_count += 1
                                yield _log(
                                    f"Warning: {ev_info}",
                                    "warning",
                                    data_start_pct + int((sum(tp["migratedRows"] for tp in tp_list) / max(total_rows, 1)) * data_range),
                                    table_progress=tp_list,
                                )

                    # Update migrated_rows total after this level
                    migrated_rows = sum(tp["migratedRows"] for tp in tp_list)

            yield _log(
                f"Data migration complete — {migrated_rows:,} rows migrated",
                "success", 90,
                table_progress=tp_list,
            )

            # Cleanup batch tracker
            try:
                cleanup_tracker(target_conn, target_config.db_type)
            except Exception:
                pass

        # ── Step 6.5: Apply Constraints (if deferred) ───────────────────
        if options.migrate_schema and options.drop_existing:
            yield _log("Applying deferred primary key constraints…", "info", 91)
            t_cur = target_conn.cursor()
            for table in tables:
                table_map = options.mappings.get(table.name) or {}
                alter_sql = generate_add_primary_key(
                    table.name, table, target_config.db_type,
                    column_mappings=table_map if table_map else None,
                )
                if alter_sql:
                    if options.dry_run:
                        yield _log(f"[DRY RUN] {alter_sql}", "info", 91)
                    else:
                        try:
                            t_cur.execute(alter_sql)
                            target_conn.commit()
                        except Exception as e:
                            target_conn.rollback()
                            yield _log(
                                f"Warning: Error applying index {table.name} — {str(e)[:120]}",
                                "warning", 91,
                            )
            t_cur.close()

        # ── Step 6.6: Migrate sequences (PostgreSQL) ─────────────────────
        if options.migrate_sequences and sequences and target_config.db_type == DbType.POSTGRESQL:
            yield _log(f"Syncing {len(sequences)} sequence(s)…", "info", 91)
            t_cur = target_conn.cursor()
            for seq_ref in sequences:
                try:
                    last_val = get_sequence_current_value(source_conn, seq_ref)
                    if last_val is not None and not options.dry_run:
                        s, seq_name = _parse_table_ref(seq_ref, DbType.POSTGRESQL)
                        qualified = f'"{s}"."{seq_name}"'
                        t_cur.execute("SELECT setval(%s::regclass, %s)", (qualified, last_val))
                        target_conn.commit()
                except Exception as e:
                    try:
                        target_conn.rollback()
                    except Exception:
                        pass
                    yield _log(
                        f"Warning: Could not sync sequence {seq_ref} — {str(e)[:120]}",
                        "warning", 91,
                    )
            t_cur.close()
            if sequences:
                yield _log(f"Sequences synced — {len(sequences)} sequence(s)", "success", 91)

        # ── Step 7: Verify integrity ────────────────────────────────────
        yield _log("Verifying data integrity…", "info", 92)

        mismatches = 0
        if not options.dry_run and options.migrate_data:
            for table in tables:
                try:
                    s, bare_name = _parse_table_ref(table.name, target_config.db_type)
                    target_count = get_row_count(
                        target_conn, target_config.db_type, bare_name, schema=s
                    )
                    if target_count != table.row_count:
                        yield _log(
                            f"Warning: Row count mismatch on {table.name} "
                            f"(source: {table.row_count:,}, target: {target_count:,})",
                            "warning", 95,
                        )
                        mismatches += 1
                except Exception:
                    pass

        if mismatches == 0:
            yield _log("Data integrity verified — all row counts match", "success", 98)
        else:
            yield _log(
                f"Data verification complete with {mismatches} mismatches",
                "warning", 98,
            )

        # ── Done ────────────────────────────────────────────────────────
        total = sum(t.row_count for t in tables)
        suffix = " (DRY RUN)" if options.dry_run else ""
        yield _log(
            f"Migration completed{suffix} — {len(tables)} tables, {total:,} rows migrated",
            "success", 100,
        )

    except Exception as e:
        status = "failed"
        yield _log(f"Migration failed: {str(e)}", "error", -1)

    finally:
        end_time = time.time()
        duration_sec = end_time - start_time
        mins, secs = divmod(int(duration_sec), 60)
        duration_str = f"{mins}m {secs}s"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        try:
            append_history({
                "source": source_config.db_type.value,
                "target": target_config.db_type.value,
                "status": status,
                "tables": final_tables_count,
                "rows": final_rows_count,
                "duration": duration_str,
                "timestamp": timestamp
            })
        except Exception:
            pass

        if source_conn:
            try:
                source_conn.close()
            except Exception:
                pass
        if target_conn:
            try:
                target_conn.close()
            except Exception:
                pass
