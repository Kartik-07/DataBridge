#!/bin/sh
set -e
# Railway (and Docker) set PORT; default only for local runs
PORT="${PORT:-8000}"
export PORT
exec uvicorn main:app --host 0.0.0.0 --port "$PORT"
