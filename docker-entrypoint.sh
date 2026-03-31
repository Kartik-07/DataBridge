#!/bin/sh
set -e
# Railway (and Docker) set PORT; default only for local runs
PORT="${PORT:-8000}"
export PORT
# Uvicorn logs to stderr by default; Railway often tags stderr as "error". Send both to stdout.
exec uvicorn main:app --host 0.0.0.0 --port "$PORT" 2>&1
