# syntax=docker/dockerfile:1
FROM node:20-bookworm-slim AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY --from=frontend /app/frontend/dist ./static

ENV PYTHONPATH=/app/backend
ENV STATIC_DIR=/app/static
ENV PYTHONUNBUFFERED=1

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

WORKDIR /app/backend
EXPOSE 8000
CMD ["/usr/local/bin/docker-entrypoint.sh"]
