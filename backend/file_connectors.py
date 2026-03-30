"""
File source connectors — handles connectivity and file listing for Local FS, SFTP, and AWS S3.
"""
from __future__ import annotations

import io
import os
from typing import IO, Any

from models import FileFormat, FileInfo, FileSourceConfig, FileSourceType

# Supported extensions → FileFormat
_EXT_MAP: dict[str, FileFormat] = {
    ".json": FileFormat.JSON,
    ".jsonl": FileFormat.JSONL,
    ".ndjson": FileFormat.JSONL,
    ".csv": FileFormat.CSV,
    ".tsv": FileFormat.CSV,
    ".xlsx": FileFormat.XLSX,
    ".xls": FileFormat.XLSX,
    ".parquet": FileFormat.PARQUET,
}

_SUPPORTED_EXTENSIONS = set(_EXT_MAP.keys())


def detect_format_from_path(path: str) -> FileFormat | None:
    """Infer FileFormat from file extension. Returns None if unknown."""
    ext = os.path.splitext(path)[-1].lower()
    return _EXT_MAP.get(ext)


def _file_info_from_path(path: str, size: int | None = None, override_format: FileFormat | None = None) -> FileInfo:
    name = os.path.basename(path)
    fmt = override_format or detect_format_from_path(path)
    return FileInfo(path=path, name=name, size=size, format=fmt)


# ── Local ────────────────────────────────────────────────────────────────────

def _test_local(config: FileSourceConfig) -> dict:
    paths = config.file_paths
    if not paths:
        return {"success": False, "message": "No file paths provided."}
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        return {"success": False, "message": f"Path(s) not found: {', '.join(missing[:3])}"}
    files = _list_local(config)
    return {"success": True, "message": f"Found {len(files)} file(s).", "files_count": len(files)}


def _list_local(config: FileSourceConfig) -> list[FileInfo]:
    results: list[FileInfo] = []
    for path in config.file_paths:
        if os.path.isfile(path):
            ext = os.path.splitext(path)[-1].lower()
            if ext in _SUPPORTED_EXTENSIONS:
                size = os.path.getsize(path)
                results.append(_file_info_from_path(path, size, config.file_format))
        elif os.path.isdir(path):
            for entry in sorted(os.scandir(path), key=lambda e: e.name):
                if entry.is_file():
                    ext = os.path.splitext(entry.name)[-1].lower()
                    if ext in _SUPPORTED_EXTENSIONS:
                        results.append(_file_info_from_path(entry.path, entry.stat().st_size, config.file_format))
    return results


def _open_local(path: str) -> IO[bytes]:
    return open(path, "rb")


# ── SFTP ─────────────────────────────────────────────────────────────────────

def _get_sftp_client(config: FileSourceConfig):
    """Return a connected (transport, sftp) tuple. Caller must close both."""
    try:
        import paramiko
    except ImportError as exc:
        raise RuntimeError("paramiko is required for SFTP support. Install it with: pip install paramiko") from exc

    transport = paramiko.Transport((config.host, config.port))
    if config.key_path:
        pkey = paramiko.RSAKey.from_private_key_file(config.key_path)
        transport.connect(username=config.username, pkey=pkey)
    else:
        transport.connect(username=config.username, password=config.password or "")
    sftp = paramiko.SFTPClient.from_transport(transport)
    return transport, sftp


def _test_sftp(config: FileSourceConfig) -> dict:
    if not config.host or not config.username:
        return {"success": False, "message": "SFTP host and username are required."}
    try:
        transport, sftp = _get_sftp_client(config)
        try:
            files = _list_sftp_with_client(sftp, config)
            count = len(files)
        finally:
            sftp.close()
            transport.close()
        return {"success": True, "message": f"SFTP connected. Found {count} file(s).", "files_count": count}
    except Exception as exc:
        return {"success": False, "message": f"SFTP connection failed: {exc}"}


def _list_sftp_with_client(sftp: Any, config: FileSourceConfig) -> list[FileInfo]:
    results: list[FileInfo] = []
    remote_paths = config.remote_paths or ["."]
    for rpath in remote_paths:
        try:
            attrs = sftp.stat(rpath)
            import stat as stat_mod
            if stat_mod.S_ISDIR(attrs.st_mode):
                for entry in sftp.listdir_attr(rpath):
                    full = f"{rpath.rstrip('/')}/{entry.filename}"
                    ext = os.path.splitext(entry.filename)[-1].lower()
                    if ext in _SUPPORTED_EXTENSIONS:
                        results.append(_file_info_from_path(full, entry.st_size, config.file_format))
            else:
                ext = os.path.splitext(rpath)[-1].lower()
                if ext in _SUPPORTED_EXTENSIONS:
                    results.append(_file_info_from_path(rpath, attrs.st_size, config.file_format))
        except Exception:
            continue
    return results


def _list_sftp(config: FileSourceConfig) -> list[FileInfo]:
    transport, sftp = _get_sftp_client(config)
    try:
        return _list_sftp_with_client(sftp, config)
    finally:
        sftp.close()
        transport.close()


def _open_sftp(config: FileSourceConfig, path: str) -> IO[bytes]:
    """Download SFTP file into memory buffer and return it."""
    transport, sftp = _get_sftp_client(config)
    try:
        buf = io.BytesIO()
        sftp.getfo(path, buf)
        buf.seek(0)
        return buf
    finally:
        sftp.close()
        transport.close()


# ── AWS S3 ───────────────────────────────────────────────────────────────────

def _get_s3_client(config: FileSourceConfig):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 support. Install it with: pip install boto3") from exc

    kwargs: dict = {}
    if config.access_key_id:
        kwargs["aws_access_key_id"] = config.access_key_id
    if config.secret_access_key:
        kwargs["aws_secret_access_key"] = config.secret_access_key
    if config.region:
        kwargs["region_name"] = config.region
    if config.endpoint_url:
        kwargs["endpoint_url"] = config.endpoint_url
    return boto3.client("s3", **kwargs)


def _test_s3(config: FileSourceConfig) -> dict:
    if not config.bucket:
        return {"success": False, "message": "S3 bucket name is required."}
    try:
        s3 = _get_s3_client(config)
        s3.head_bucket(Bucket=config.bucket)
        files = _list_s3_with_client(s3, config)
        count = len(files)
        return {"success": True, "message": f"S3 bucket accessible. Found {count} file(s).", "files_count": count}
    except Exception as exc:
        return {"success": False, "message": f"S3 access failed: {exc}"}


def _list_s3_with_client(s3: Any, config: FileSourceConfig) -> list[FileInfo]:
    results: list[FileInfo] = []
    keys = config.keys or [""]  # empty prefix → list all

    for key in keys:
        ext = os.path.splitext(key)[-1].lower()
        if ext in _SUPPORTED_EXTENSIONS:
            # Treat as direct object key
            try:
                head = s3.head_object(Bucket=config.bucket, Key=key)
                size = head.get("ContentLength")
                results.append(_file_info_from_path(key, size, config.file_format))
            except Exception:
                pass
        else:
            # Treat as prefix / folder
            paginator = s3.get_paginator("list_objects_v2")
            prefix = key if key.endswith("/") or not key else key
            for page in paginator.paginate(Bucket=config.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    obj_key = obj["Key"]
                    obj_ext = os.path.splitext(obj_key)[-1].lower()
                    if obj_ext in _SUPPORTED_EXTENSIONS:
                        results.append(_file_info_from_path(obj_key, obj.get("Size"), config.file_format))
    return results


def _list_s3(config: FileSourceConfig) -> list[FileInfo]:
    s3 = _get_s3_client(config)
    return _list_s3_with_client(s3, config)


def _open_s3(config: FileSourceConfig, path: str) -> IO[bytes]:
    """Stream S3 object into memory buffer and return it."""
    s3 = _get_s3_client(config)
    response = s3.get_object(Bucket=config.bucket, Key=path)
    buf = io.BytesIO(response["Body"].read())
    buf.seek(0)
    return buf


# ── Public API ───────────────────────────────────────────────────────────────

def test_file_source(config: FileSourceConfig) -> dict:
    """Verify access to the file source. Returns dict with success, message, files_count."""
    if config.source_type == FileSourceType.LOCAL:
        return _test_local(config)
    elif config.source_type == FileSourceType.SFTP:
        return _test_sftp(config)
    elif config.source_type == FileSourceType.S3:
        return _test_s3(config)
    return {"success": False, "message": f"Unknown source type: {config.source_type}"}


def list_files(config: FileSourceConfig) -> list[FileInfo]:
    """List available files with metadata from the configured source."""
    if config.source_type == FileSourceType.LOCAL:
        return _list_local(config)
    elif config.source_type == FileSourceType.SFTP:
        return _list_sftp(config)
    elif config.source_type == FileSourceType.S3:
        return _list_s3(config)
    return []


def download_file(config: FileSourceConfig, file_path: str) -> IO[bytes]:
    """Return a seekable bytes IO for reading the specified file."""
    if config.source_type == FileSourceType.LOCAL:
        return _open_local(file_path)
    elif config.source_type == FileSourceType.SFTP:
        return _open_sftp(config, file_path)
    elif config.source_type == FileSourceType.S3:
        return _open_s3(config, file_path)
    raise ValueError(f"Unknown source type: {config.source_type}")
