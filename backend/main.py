"""
DBMigrate Backend — FastAPI entry point.
"""
import logging
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("databridge.main")

# ── Signal handlers ──────────────────────────────────────────────────────────
# Installed before any app code so we can distinguish a clean shutdown
# (SIGTERM from Railway/Docker) from an unexpected exit.

def _handle_sigterm(signum, frame):
    logger.warning(
        "SIGTERM received — container is being stopped by the orchestrator. "
        "This is the cause of the 'silent exit after startup' if it arrives "
        "immediately after the process starts."
    )
    sys.exit(0)


def _handle_sigint(signum, frame):
    logger.warning("SIGINT received — shutting down.")
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigint)

logger.info("Signal handlers registered (SIGTERM, SIGINT).")

# ── Config ───────────────────────────────────────────────────────────────────
try:
    logger.info("Loading application settings…")
    from config import settings
    logger.info(
        "Settings loaded — host=%s port=%d log_level=%s cors_origins=%s",
        settings.HOST,
        settings.PORT,
        settings.LOG_LEVEL,
        settings.CORS_ORIGINS,
    )
except Exception:
    logger.exception("FATAL: failed to load settings — cannot start.")
    sys.exit(1)

# ── Router ───────────────────────────────────────────────────────────────────
try:
    logger.info("Importing API router…")
    from router import router
    logger.info("Router imported successfully.")
except Exception:
    logger.exception("FATAL: failed to import router — cannot start.")
    sys.exit(1)

# ── App ──────────────────────────────────────────────────────────────────────
try:
    logger.info("Creating FastAPI application…")
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
    )
    logger.info("FastAPI app created.")
except Exception:
    logger.exception("FATAL: failed to create FastAPI app — cannot start.")
    sys.exit(1)

# ── CORS ─────────────────────────────────────────────────────────────────────
try:
    logger.info("Adding CORS middleware with origins: %s", settings.CORS_ORIGINS)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS middleware added.")
except Exception:
    logger.exception("FATAL: failed to add CORS middleware — cannot start.")
    sys.exit(1)

# ── Routes ───────────────────────────────────────────────────────────────────
try:
    logger.info("Registering API router…")
    app.include_router(router)
    logger.info("Router registered. Registered routes: %s", [r.path for r in app.routes])
except Exception:
    logger.exception("FATAL: failed to register router — cannot start.")
    sys.exit(1)


# ── Lifecycle events ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    logger.info(
        "Application startup complete — %s v%s is ready on %s:%d",
        settings.APP_NAME,
        settings.APP_VERSION,
        settings.HOST,
        settings.PORT,
    )


@app.on_event("shutdown")
async def on_shutdown():
    logger.warning(
        "Application shutdown event fired — %s is stopping. "
        "If this appears immediately after startup, check for a SIGTERM "
        "from the orchestrator, a failing health-check, or a port conflict.",
        settings.APP_NAME,
    )


# ── Root endpoint ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
    }


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    logger.info(
        "Starting Uvicorn — host=%s port=%d log_level=%s",
        settings.HOST,
        settings.PORT,
        settings.LOG_LEVEL,
    )
    try:
        uvicorn.run(
            "main:app",
            host=settings.HOST,
            port=settings.PORT,
            reload=False,
            log_level=settings.LOG_LEVEL,
        )
    except Exception:
        logger.exception("Uvicorn exited with an unhandled exception.")
        sys.exit(1)
