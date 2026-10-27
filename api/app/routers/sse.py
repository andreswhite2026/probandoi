"""
Router para Server-Sent Events (SSE) con streaming en tiempo real y manejo de reconexiones.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from app.db import get_db_cursor

logger = logging.getLogger("sismowatch.api.sse")
router = APIRouter(prefix="/events", tags=["Streaming en Tiempo Real"])


async def event_generator(request: Request) -> AsyncGenerator[dict, None]:
    """
    Generador asíncrono para Server-Sent Events (SSE).
    Envía periódicamente sismos recientes y heartbeats para mantener viva la conexión
    y asegurar una desconexión limpia del cliente sin bloquear el servidor.
    """
    last_published_at = None
    logger.info("Nuevo cliente conectado a la transmisión SSE")

    try:
        while True:
            # Detectar desconexión del cliente
            if await request.is_disconnected():
                logger.info("Cliente SSE desconectado limpiamente")
                break

            now_utc = datetime.now(timezone.utc)
            events_to_emit = []

            # Consultar sismos recientes en Gold
            try:
                with get_db_cursor() as cur:
                    if last_published_at is None:
                        # Al conectar, enviar los últimos 3 eventos curados
                        cur.execute(
                            """
                            SELECT id, mag, place, region, time_utc, longitude, latitude, depth_km, published_at
                            FROM curated.earthquakes
                            ORDER BY time_utc DESC
                            LIMIT 3
                            """
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, mag, place, region, time_utc, longitude, latitude, depth_km, published_at
                            FROM curated.earthquakes
                            WHERE published_at > %s
                            ORDER BY time_utc DESC
                            LIMIT 10
                            """,
                            (last_published_at,),
                        )
                    rows = cur.fetchall()

                if rows:
                    last_published_at = max(r["published_at"] for r in rows)
                    for r in rows:
                        events_to_emit.append({
                            "id": r["id"],
                            "mag": float(r["mag"]) if r["mag"] is not None else None,
                            "place": r["place"],
                            "region": r["region"],
                            "time_utc": r["time_utc"].isoformat() if r["time_utc"] else None,
                            "longitude": float(r["longitude"]),
                            "latitude": float(r["latitude"]),
                            "depth_km": float(r["depth_km"]) if r["depth_km"] is not None else None,
                        })
            except Exception as exc:
                logger.warning("Error consultando sismos para SSE: %s", exc)

            # Emitir eventos si los hay
            for ev in events_to_emit:
                yield {
                    "event": "earthquake",
                    "id": ev["id"],
                    "data": json.dumps(ev, ensure_ascii=False),
                }

            # Enviar heartbeat periódico con timestamp
            yield {
                "event": "heartbeat",
                "data": json.dumps({"timestamp": now_utc.isoformat(), "status": "alive"}),
            }

            # Pausa no bloqueante
            await asyncio.sleep(5)

    except asyncio.CancelledError:
        logger.info("Transmisión SSE cancelada por desconexión de cliente")
        raise


@router.get("/stream", summary="Transmisión en tiempo real vía Server-Sent Events (SSE)")
async def events_stream(request: Request) -> EventSourceResponse:
    """
    Establece un canal SSE para emitir actualizaciones de sismos en tiempo real y latidos (heartbeats).
    Soporta reconexión automática en el cliente y detección de desconexión.
    """
    return EventSourceResponse(
        event_generator(request),
        ping=15,
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
