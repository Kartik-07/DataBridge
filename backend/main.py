"""
DBMigrate Backend — FastAPI entry point.
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from config import settings
from router import router

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
)

# ── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ──────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Railway and load balancers use this path; keep it before the static mount."""
    return {"status": "ok"}


app.include_router(router)

_static_path = Path(settings.STATIC_DIR) if settings.STATIC_DIR else None
if _static_path and _static_path.is_dir():
    app.mount(
        "/",
        StaticFiles(directory=str(_static_path), html=True),
        name="static",
    )
else:

    @app.get("/")
    async def root():
        return {
            "app": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "docs": "/docs",
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True,
        log_level=settings.LOG_LEVEL,
    )
