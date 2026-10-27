"""
Aplicación principal FastAPI para SismoWatch.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import get_db_cursor
from app.routers import agent, auth, earthquakes, operations, reports, sse
from app.security import hash_password

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("sismowatch.api.main")


def _seed_initial_users() -> None:
    """Inserta los usuarios iniciales (admin y viewer) en app.users si no existen."""
    try:
        admin_id = "00000000-0000-0000-0000-000000000001"
        viewer_id = "00000000-0000-0000-0000-000000000002"

        with get_db_cursor(commit=True) as cur:
            # Seed Admin
            admin_pwd_hash = hash_password(settings.ADMIN_PASSWORD)
            cur.execute(
                """
                INSERT INTO app.users (id, email, password_hash, role, is_active)
                VALUES (%s, %s, %s, 'admin', true)
                ON CONFLICT (email) DO UPDATE
                SET password_hash = EXCLUDED.password_hash, role = 'admin', is_active = true
                """,
                (admin_id, settings.ADMIN_EMAIL, admin_pwd_hash),
            )

            # Seed Viewer
            viewer_pwd_hash = hash_password(settings.VIEWER_PASSWORD)
            cur.execute(
                """
                INSERT INTO app.users (id, email, password_hash, role, is_active)
                VALUES (%s, %s, %s, 'viewer', true)
                ON CONFLICT (email) DO UPDATE
                SET password_hash = EXCLUDED.password_hash, role = 'viewer', is_active = true
                """,
                (viewer_id, settings.VIEWER_EMAIL, viewer_pwd_hash),
            )

        logger.info(
            "Usuarios iniciales verificados: admin (%s) y viewer (%s)",
            settings.ADMIN_EMAIL,
            settings.VIEWER_EMAIL,
        )
    except Exception as exc:
        logger.error("Error al sembrar usuarios iniciales: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Ciclo de vida de la aplicación: inicialización y apagado."""
    logger.info("Iniciando SismoWatch API...")
    _seed_initial_users()
    yield
    logger.info("Apagando SismoWatch API...")


app = FastAPI(
    title="SismoWatch API",
    version="1.0.0",
    description="API RESTful y SSE de alto rendimiento para monitoreo sísmico con arquitectura Medallion",
    lifespan=lifespan,
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS if settings.CORS_ORIGINS else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint de chequeo de salud
@app.get("/health", tags=["Salud del Sistema"], summary="Verificar estado de salud de la API")
def health_check() -> dict:
    return {
        "status": "healthy",
        "service": "sismowatch-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# Registrar routers bajo el prefijo /api/v1
app.include_router(auth.router, prefix="/api/v1")
app.include_router(earthquakes.router, prefix="/api/v1")
app.include_router(reports.router, prefix="/api/v1")
app.include_router(agent.router, prefix="/api/v1")
app.include_router(operations.router, prefix="/api/v1")
app.include_router(sse.router, prefix="/api/v1")
