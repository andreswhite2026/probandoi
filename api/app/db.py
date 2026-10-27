"""
Gestión de conexiones a PostgreSQL con el rol de mínimo privilegio sismo_api.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import settings

logger = logging.getLogger("sismowatch.api.db")


def get_connection() -> psycopg2.extensions.connection:
    """Crea una conexión directa a la base de datos con el rol sismo_api."""
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


@contextmanager
def get_db_cursor(commit: bool = False) -> Generator[RealDictCursor, None, None]:
    """Context manager para ejecutar sentencias SQL con cursores tipados tipo diccionario."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
