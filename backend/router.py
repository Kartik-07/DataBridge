"""
API router — FastAPI endpoints for the migration application.
"""
from __future__ import annotations

import json
import asyncio

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from models import (
    ConnectionConfig,
    ConnectionTestResponse,
    FileMigrationRequest,
    FileSourceConfig,
    IntrospectResponse,
    MigrationRequest,
    TablesColumnsRequest,
)
from connectors import (
    test_connection,
    introspect_schema,
    get_schemas,
    get_connection,
    get_table_list,
    get_tables_columns,
)
from migration import run_migration
from file_connectors import test_file_source, list_files, download_file
from file_parsers import detect_format, infer_schema
from file_migration import run_file_migration, get_file_schemas, _table_name_from_path
from schema_translator import translate_type, AVAILABLE_TRANSFORMS
from models import DbType, TableInfo

from migration_state import _migration_pause_flags

router = APIRouter(prefix="/api")


# ── Health ──────────────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok"}


# ── Test Connection ─────────────────────────────────────────────────────────

@router.post("/test-connection", response_model=ConnectionTestResponse)
async def api_test_connection(config: ConnectionConfig):
    """Test database connectivity and return basic metadata."""
    result = await asyncio.to_thread(test_connection, config)
    return ConnectionTestResponse(**result)


# ── Schemas ─────────────────────────────────────────────────────────────────

@router.post("/schemas")
async def api_schemas(config: ConnectionConfig):
    """Return list of available schema names for a database."""
    def _get():
        conn = get_connection(config)
        schemas = get_schemas(conn, config.db_type, config.database)
        conn.close()
        return schemas

    schemas = await asyncio.to_thread(_get)
    return {"schemas": schemas}


# ── Tables (lightweight list) ────────────────────────────────────────────────

@router.post("/tables")
async def api_tables(config: ConnectionConfig):
    """Lightweight: return only table refs (schema.table), no columns. Use for dropdowns and progressive loading."""
    tables = await asyncio.to_thread(get_table_list, config, None)
    return {"tables": tables}


# ── Tables columns (by table) ───────────────────────────────────────────────

@router.post("/tables/columns")
async def api_tables_columns(body: TablesColumnsRequest):
    """Return column info for the given table refs only (no row counts). For Schema Mapping progressive load."""
    tables = await asyncio.to_thread(get_tables_columns, body.config, body.table_refs)
    return {"tables": tables}


# ── Introspect ──────────────────────────────────────────────────────────────

@router.post("/introspect", response_model=IntrospectResponse)
async def api_introspect(config: ConnectionConfig, tables_only: bool = False, for_mapping: bool = False):
    """Return full schema information for a database.
    If tables_only=true, returns only table names (faster).
    If for_mapping=true, returns tables with columns but skips row counts and views/indexes/sequences (faster for Schema Mapping)."""
    result = await asyncio.to_thread(introspect_schema, config, None, tables_only, for_mapping)
    return IntrospectResponse(**result)


# ── Type mapping (for Schema Mapping UI) ────────────────────────────────────

@router.post("/translate-types")
async def api_translate_types(body: dict):
    """Translate a list of source column types to target engine types. Same engine returns as-is.
    Body: { source_db, target_db, source_types: string[] }."""
    source_db = body.get("source_db", "postgresql")
    target_db = body.get("target_db", "postgresql")
    source_types = body.get("source_types") or []
    try:
        source_enum = DbType(source_db.lower())
        target_enum = DbType(target_db.lower())
    except ValueError:
        return {"target_types": list(source_types)}
    target_types = [
        translate_type(st, source_enum, target_enum)
        for st in source_types
    ]
    return {"target_types": target_types}


# ── Available transforms (for Schema Mapping UI) ────────────────────────────

@router.get("/transforms")
async def api_transforms():
    """Return the catalog of available row-level data transforms."""
    return {"transforms": AVAILABLE_TRANSFORMS}


# ── Migrate (SSE Stream) ───────────────────────────────────────────────────

@router.post("/migrate")
async def api_migrate(request: MigrationRequest):
    """
    Start a migration and stream log events via Server-Sent Events (SSE).
    Each event is a JSON object: { message, type, progress }.
    """

    async def event_generator():
        # Run the blocking migration generator in a thread
        loop = asyncio.get_event_loop()

        # We collect events from the generator in a thread-safe manner
        gen = run_migration(
            request.source, request.target, request.options,
            session_id=request.session_id,
        )

        def _next_event():
            try:
                return next(gen)
            except StopIteration:
                return None

        while True:
            event = await loop.run_in_executor(None, _next_event)
            if event is None:
                break
            data = json.dumps(event)
            yield f"data: {data}\n\n"
            # Small delay so the frontend can render each event
            await asyncio.sleep(0.05)

        # Send a final "done" event
        yield f"data: {json.dumps({'done': True})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

# ── Migration Pause/Resume ───────────────────────────────────────────────────

@router.post("/migration/pause")
async def api_migration_pause(body: dict):
    """Set pause flag for a migration session. Migration will block until resumed."""
    sid = body.get("session_id")
    if sid:
        _migration_pause_flags[sid] = True
    return {"ok": True}


@router.post("/migration/resume")
async def api_migration_resume(body: dict):
    """Clear pause flag for a migration session."""
    sid = body.get("session_id")
    if sid:
        _migration_pause_flags[sid] = False
    return {"ok": True}


# ── History ─────────────────────────────────────────────────────────────────

@router.get("/history")
async def api_history():
    from history import get_history
    history = await asyncio.to_thread(get_history)
    return history


# ── File Source ──────────────────────────────────────────────────────────────

@router.post("/file-source/test")
async def api_file_source_test(config: FileSourceConfig):
    """Test connectivity to a file source (local / SFTP / S3).
    Returns { success, message, files_count }."""
    result = await asyncio.to_thread(test_file_source, config)
    return result


@router.post("/file-source/files")
async def api_file_source_files(config: FileSourceConfig):
    """List all discoverable files in the configured source.
    Returns { files: [{ path, name, size, format }] }."""
    files = await asyncio.to_thread(list_files, config)
    return {"files": [f.model_dump() for f in files]}


@router.post("/file-source/schema")
async def api_file_source_schema(body: dict):
    """Infer schema for a list of file paths within a source.
    Body: { config: FileSourceConfig, file_paths: [str] }
    Returns { tables: [TableInfo] } — one table per file."""
    config = FileSourceConfig(**body["config"])
    file_paths: list[str] = body.get("file_paths", [])
    if not file_paths:
        return {"tables": []}

    def _infer():
        return get_file_schemas(config, file_paths)

    tables = await asyncio.to_thread(_infer)
    return {"tables": [t.model_dump() for t in tables]}


# ── File Migrate (SSE Stream) ────────────────────────────────────────────────

@router.post("/file-migrate")
async def api_file_migrate(request: FileMigrationRequest):
    """
    Start a file-to-database migration and stream log events via SSE.
    Each event is a JSON object: { message, type, progress, [table_progress] }.
    """

    async def event_generator():
        loop = asyncio.get_event_loop()
        gen = run_file_migration(
            request.file_source,
            request.target,
            request.options,
            session_id=request.session_id,
        )

        def _next_event():
            try:
                return next(gen)
            except StopIteration:
                return None

        while True:
            event = await loop.run_in_executor(None, _next_event)
            if event is None:
                break
            data = json.dumps(event)
            yield f"data: {data}\n\n"
            await asyncio.sleep(0.05)

        yield f"data: {json.dumps({'done': True})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
