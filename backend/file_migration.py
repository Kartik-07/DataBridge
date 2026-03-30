"""
File migration engine — migrates data from file sources (Local/SFTP/S3) into database targets.
Yields LogEntry dicts for real-time SSE streaming, matching the protocol of migration.py.
"""
from __future__ import annotations

import os
from typing import Generator, Optional

from models import (
    ColumnInfo,
    ColumnMapping,
    ConnectionConfig,
    DbType,
    FileFormat,
    FileSourceConfig,
    MigrationOptions,
    TableInfo,
)
from connectors import get_connection
from schema_translator import (
    generate_create_table,
    generate_drop_table,
    generate_insert,
    apply_transform_rules,
)
from file_connectors import download_file, list_files
from file_parsers import detect_format, infer_schema, read_rows

try:
    from migration_state import _migration_pause_flags
except ImportError:
    _migration_pause_flags = {}


BATCH_SIZE = 1000


def _log(message: str, type_: str = "info", progress: int = 0, **extra) -> dict:
    d = {"message": message, "type": type_, "progress": progress}
    d.update(extra)
    return d


def _wait_if_paused(session_id: str | None) -> None:
    if not session_id:
        return
    import time
    while _migration_pause_flags.get(session_id):
        time.sleep(0.5)


def _table_name_from_path(path: str) -> str:
    """Derive a clean table name from a file path (stem without extension)."""
    base = os.path.basename(path)
    name = os.path.splitext(base)[0]
    # Replace non-alphanumeric chars with underscores; strip leading digits
    clean = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if clean and clean[0].isdigit():
        clean = "_" + clean
    return clean or "imported_file"


def _adapt_value(val, transform: Optional[dict]) -> object:
    """Apply an optional transform rule to a single cell value."""
    if transform is None or val is None:
        return val
    if transform["type"] == "rule":
        return apply_transform_rules(val, transform["rules"])
    return val


def get_file_schemas(
    file_source: FileSourceConfig,
    file_paths: list[str],
) -> list[TableInfo]:
    """
    Infer schema for each file and return a list of TableInfo objects
    (one per file, table name = filename stem).
    """
    tables: list[TableInfo] = []
    for path in file_paths:
        fmt = file_source.file_format or detect_format(path)
        file_obj = download_file(file_source, path)
        try:
            headers, columns = infer_schema(file_obj, fmt)
        finally:
            file_obj.close()
        table_name = _table_name_from_path(path)
        tables.append(TableInfo(name=table_name, columns=columns, row_count=0))
    return tables


def run_file_migration(
    file_source: FileSourceConfig,
    target_config: ConnectionConfig,
    options: MigrationOptions,
    session_id: str | None = None,
) -> Generator[dict, None, None]:
    """
    Migrate data from file source into a database target.
    Yields log dicts with {message, type, progress, [table_progress]}.
    """
    yield _log("Starting file import migration…", "info", 0)

    # ── Resolve file list ────────────────────────────────────────────────────
    try:
        all_files = list_files(file_source)
    except Exception as exc:
        yield _log(f"Failed to list files: {exc}", "error", 0)
        return

    if not all_files:
        yield _log("No supported files found in source.", "warning", 0)
        return

    # Respect selected_tables: {"" -> [table_name, ...]} keyed by stem
    selected_stems: set[str] = set()
    for stems in options.selected_tables.values():
        selected_stems.update(stems)

    files_to_migrate = []
    for fi in all_files:
        stem = _table_name_from_path(fi.path)
        if selected_stems and stem not in selected_stems:
            continue
        files_to_migrate.append(fi)

    if not files_to_migrate:
        yield _log("No files selected for migration.", "warning", 0)
        return

    total_files = len(files_to_migrate)
    yield _log(f"Migrating {total_files} file(s) → {target_config.db_type.value}", "info", 0)

    # ── Connect to target ────────────────────────────────────────────────────
    try:
        tgt_conn = get_connection(target_config)
    except Exception as exc:
        yield _log(f"Cannot connect to target: {exc}", "error", 0)
        return

    table_progress = [
        {"name": _table_name_from_path(fi.path), "schema": "", "totalRows": 0, "migratedRows": 0, "status": "pending"}
        for fi in files_to_migrate
    ]
    yield _log("Target connected.", "info", 2, table_progress=table_progress)

    total_rows_migrated = 0
    files_done = 0

    for idx, fi in enumerate(files_to_migrate):
        _wait_if_paused(session_id)

        table_name = _table_name_from_path(fi.path)
        fmt = file_source.file_format or fi.format or detect_format(fi.path)
        table_progress[idx]["status"] = "migrating"
        progress_pct = int(5 + 90 * idx / total_files)
        yield _log(f"Processing: {fi.name} → table '{table_name}'", "info", progress_pct,
                   table_progress=table_progress)

        # ── Infer schema ─────────────────────────────────────────────────────
        try:
            file_obj = download_file(file_source, fi.path)
            headers, columns = infer_schema(file_obj, fmt)
            file_obj.seek(0)
        except Exception as exc:
            yield _log(f"  Schema inference failed for {fi.name}: {exc}", "error", progress_pct)
            table_progress[idx]["status"] = "error"
            continue

        if not headers:
            yield _log(f"  No columns found in {fi.name}, skipping.", "warning", progress_pct)
            table_progress[idx]["status"] = "done"
            continue

        # Apply user-defined column name overrides from mappings
        table_key = table_name  # used as key in options.mappings (no schema prefix for files)
        table_mappings: dict[str, ColumnMapping] = options.mappings.get(table_key, {})

        # Build target column list (with renames from mappings)
        target_columns: list[str] = []
        for col in columns:
            m = table_mappings.get(col.name)
            target_columns.append(m.target_name if m else col.name)

        table_info = TableInfo(name=table_name, columns=columns, row_count=0)

        # ── Drop / create table ───────────────────────────────────────────────
        tgt_cur = tgt_conn.cursor()
        if options.drop_existing:
            try:
                drop_sql = generate_drop_table(table_name, target_config.db_type)
                tgt_cur.execute(drop_sql)
                tgt_conn.commit()
            except Exception:
                try:
                    tgt_conn.rollback()
                except Exception:
                    pass

        if options.migrate_schema:
            try:
                create_sql = generate_create_table(
                    table_info,
                    source_type=DbType.POSTGRESQL,  # file types are already SQL-style (BIGINT, TEXT, …)
                    target_type=target_config.db_type,
                    with_pk=False,
                    migrating_fresh=options.drop_existing,
                    if_not_exists=not options.drop_existing,
                    column_mappings=table_mappings if table_mappings else None,
                )
                tgt_cur.execute(create_sql)
                tgt_conn.commit()
                yield _log(f"  Table '{table_name}' created.", "info", progress_pct)
            except Exception as exc:
                try:
                    tgt_conn.rollback()
                except Exception:
                    pass
                yield _log(f"  Failed to create table '{table_name}': {exc}", "error", progress_pct)
                table_progress[idx]["status"] = "error"
                tgt_cur.close()
                continue

        # ── Migrate data ──────────────────────────────────────────────────────
        if not options.migrate_data:
            table_progress[idx]["status"] = "done"
            tgt_cur.close()
            files_done += 1
            continue

        if options.dry_run:
            yield _log(f"  [Dry run] Skipping data for '{table_name}'.", "info", progress_pct)
            table_progress[idx]["status"] = "done"
            tgt_cur.close()
            files_done += 1
            continue

        # Build INSERT SQL using target column names
        insert_sql = generate_insert(
            table_name,
            target_columns,
            target_config.db_type,
            for_execute_values=(target_config.db_type == DbType.POSTGRESQL),
        )

        # Pre-compute per-column transforms
        col_transforms: list[Optional[dict]] = []
        for col in columns:
            m = table_mappings.get(col.name)
            if m and m.action == "transform":
                if m.transform_rule:
                    col_transforms.append({"type": "rule", "rules": m.transform_rule})
                elif m.source_format and m.target_format:
                    col_transforms.append({"type": "date", "source_format": m.source_format, "target_format": m.target_format})
                else:
                    col_transforms.append(None)
            else:
                col_transforms.append(None)

        rows_for_file = 0
        try:
            for batch in read_rows(file_obj, fmt, headers, BATCH_SIZE):
                _wait_if_paused(session_id)

                adapted = []
                for row in batch:
                    adapted.append(tuple(
                        _adapt_value(val, col_transforms[i] if i < len(col_transforms) else None)
                        for i, val in enumerate(row)
                    ))

                if target_config.db_type == DbType.POSTGRESQL and "VALUES %s" in insert_sql:
                    from psycopg2.extras import execute_values
                    execute_values(tgt_cur, insert_sql, adapted)
                else:
                    tgt_cur.executemany(insert_sql, adapted)
                tgt_conn.commit()

                rows_for_file += len(batch)
                table_progress[idx]["migratedRows"] = rows_for_file
                table_progress[idx]["totalRows"] = rows_for_file
                inner_pct = int(progress_pct + (90 / total_files) * (rows_for_file / max(rows_for_file, 1) * 0.5))
                yield _log(
                    f"  '{table_name}': {rows_for_file} rows imported…",
                    "info",
                    inner_pct,
                    table_progress=table_progress,
                )

            total_rows_migrated += rows_for_file
            table_progress[idx]["status"] = "done"
            yield _log(
                f"  '{table_name}' complete — {rows_for_file} rows.",
                "success",
                int(5 + 90 * (idx + 1) / total_files),
                table_progress=table_progress,
            )

        except Exception as exc:
            try:
                tgt_conn.rollback()
            except Exception:
                pass
            yield _log(f"  Data import failed for '{table_name}': {exc}", "error", progress_pct)
            table_progress[idx]["status"] = "error"
        finally:
            tgt_cur.close()
            try:
                file_obj.close()
            except Exception:
                pass

        files_done += 1

    # ── Finalise ──────────────────────────────────────────────────────────────
    try:
        tgt_conn.close()
    except Exception:
        pass

    success_count = sum(1 for t in table_progress if t["status"] == "done")
    error_count = sum(1 for t in table_progress if t["status"] == "error")

    if error_count == 0:
        yield _log(
            f"File import complete — {total_rows_migrated} rows across {success_count} table(s).",
            "success",
            100,
            table_progress=table_progress,
        )
    else:
        yield _log(
            f"File import finished with {error_count} error(s). "
            f"{total_rows_migrated} rows imported into {success_count} table(s).",
            "warning",
            100,
            table_progress=table_progress,
        )
