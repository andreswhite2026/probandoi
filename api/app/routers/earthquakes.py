"""
Router para Monitoreo Sísmico: Terremotos, KPIs y Exportaciones (CSV / GeoJSON).
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse

from app.db import get_db_cursor
from app.models import EarthquakeResponse, EarthquakesListResponse, KPIsResponse
from app.security import get_current_user

logger = logging.getLogger("sismowatch.api.earthquakes")
router = APIRouter(prefix="/earthquakes", tags=["Monitoreo Sísmico"])


def _row_to_earthquake_response(row: Dict[str, Any]) -> EarthquakeResponse:
    """Convierte una fila de PostgreSQL a modelo Pydantic tipado."""
    return EarthquakeResponse(
        id=str(row["id"]),
        mag=float(row["mag"]) if row["mag"] is not None else None,
        mag_type=row.get("mag_type"),
        place=row.get("place"),
        region=row.get("region"),
        time_utc=row["time_utc"],
        updated_utc=row.get("updated_utc"),
        tsunami=bool(row.get("tsunami", False)),
        sig=int(row["sig"]) if row.get("sig") is not None else None,
        event_status=row.get("event_status"),
        event_type=row.get("event_type"),
        url=row.get("url"),
        longitude=float(row["longitude"]),
        latitude=float(row["latitude"]),
        depth_km=float(row["depth_km"]) if row.get("depth_km") is not None else None,
        ingest_run_id=str(row["ingest_run_id"]),
        published_at=row["published_at"],
    )


@router.get("", response_model=EarthquakesListResponse, summary="Listar sismos con filtros y paginación")
def list_earthquakes(
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    min_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    max_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    region: Optional[str] = Query(default=None, min_length=1),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> EarthquakesListResponse:
    """
    Retorna la lista de sismos curados (Gold) con filtros opcionales de magnitud,
    región y rango temporal, más paginación estricta.
    """
    where_clauses: List[str] = []
    params: List[Any] = []

    if min_mag is not None:
        where_clauses.append("mag >= %s")
        params.append(min_mag)

    if max_mag is not None:
        where_clauses.append("mag <= %s")
        params.append(max_mag)

    if region:
        where_clauses.append("region ILIKE %s")
        params.append(f"%{region.strip()}%")

    if start_time:
        where_clauses.append("time_utc >= %s")
        params.append(start_time)

    if end_time:
        where_clauses.append("time_utc <= %s")
        params.append(end_time)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with get_db_cursor() as cur:
        # Total de registros coincidentes
        cur.execute(f"SELECT COUNT(*) AS total FROM curated.earthquakes {where_sql}", tuple(params))
        total = cur.fetchone()["total"]

        # Filas paginadas
        query_sql = f"""
            SELECT id, mag, mag_type, place, region, time_utc, updated_utc,
                   tsunami, sig, event_status, event_type, url,
                   longitude, latitude, depth_km, ingest_run_id, published_at
            FROM curated.earthquakes
            {where_sql}
            ORDER BY time_utc DESC
            LIMIT %s OFFSET %s
        """
        cur.execute(query_sql, tuple(params + [limit, offset]))
        rows = cur.fetchall()

    items = [_row_to_earthquake_response(r) for r in rows]

    return EarthquakesListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )


@router.get("/kpis", response_model=KPIsResponse, summary="Indicadores clave de monitoreo sísmico")
def get_kpis(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> KPIsResponse:
    """
    Calcula los KPIs sísmicos principales: actividad reciente (24h y 7d),
    magnitud máxima y media, alertas de tsunami y anomalías activas.
    """
    with get_db_cursor() as cur:
        # Determinar fecha de referencia (NOW() o el evento más reciente si estamos en histórico)
        cur.execute("SELECT MAX(time_utc) AS max_time FROM curated.earthquakes")
        max_time_row = cur.fetchone()
        latest_event_time = max_time_row["max_time"] if max_time_row and max_time_row["max_time"] else None

        if not latest_event_time:
            return KPIsResponse(
                total_24h=0,
                total_7d=0,
                max_mag_24h=None,
                avg_mag_24h=None,
                tsunami_alerts_24h=0,
                active_anomalies_count=0,
                top_region_24h=None,
            )

        # Calcular KPIs respecto a la hora actual (NOW() en UTC)
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE time_utc >= NOW() - INTERVAL '24 hours') AS total_24h,
                COUNT(*) FILTER (WHERE time_utc >= NOW() - INTERVAL '7 days') AS total_7d,
                MAX(mag) FILTER (WHERE time_utc >= NOW() - INTERVAL '24 hours') AS max_mag_24h,
                ROUND(AVG(mag) FILTER (WHERE time_utc >= NOW() - INTERVAL '24 hours')::numeric, 2) AS avg_mag_24h,
                COUNT(*) FILTER (WHERE time_utc >= NOW() - INTERVAL '24 hours' AND tsunami = true) AS tsunami_alerts_24h
            FROM curated.earthquakes
            """
        )
        stats = cur.fetchone()

        # Top región en últimas 24 horas respecto a NOW()
        cur.execute(
            """
            SELECT region, COUNT(*) AS count
            FROM curated.earthquakes
            WHERE time_utc >= NOW() - INTERVAL '24 hours' AND region IS NOT NULL
            GROUP BY region
            ORDER BY count DESC
            LIMIT 1
            """
        )
        top_region_row = cur.fetchone()
        top_region = top_region_row["region"] if top_region_row else None

        # Anomalías activas detectadas en últimas 24 horas respecto a NOW()
        cur.execute(
            """
            SELECT COUNT(*) AS active_anomalies
            FROM curated.anomalies
            WHERE is_anomaly = true
              AND window_start >= NOW() - INTERVAL '24 hours'
            """
        )
        anomalies_row = cur.fetchone()
        active_anomalies = anomalies_row["active_anomalies"] if anomalies_row else 0


    return KPIsResponse(
        total_24h=int(stats["total_24h"] or 0),
        total_7d=int(stats["total_7d"] or 0),
        max_mag_24h=float(stats["max_mag_24h"]) if stats["max_mag_24h"] is not None else None,
        avg_mag_24h=round(float(stats["avg_mag_24h"]), 2) if stats["avg_mag_24h"] is not None else None,
        tsunami_alerts_24h=int(stats["tsunami_alerts_24h"] or 0),
        active_anomalies_count=int(active_anomalies or 0),
        top_region_24h=top_region,
    )


@router.get("/export", summary="Exportar eventos sísmicos en CSV o GeoJSON")
def export_earthquakes(
    format: str = Query(default="csv", pattern="^(csv|geojson)$"),
    min_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    max_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    region: Optional[str] = Query(default=None, min_length=1),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = Query(default=5000, ge=1, le=20000),
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> Response:
    """
    Exporta datos de sismos curados (Gold) en formato CSV o GeoJSON estándar.
    """
    where_clauses: List[str] = []
    params: List[Any] = []

    if min_mag is not None:
        where_clauses.append("mag >= %s")
        params.append(min_mag)

    if max_mag is not None:
        where_clauses.append("mag <= %s")
        params.append(max_mag)

    if region:
        where_clauses.append("region ILIKE %s")
        params.append(f"%{region.strip()}%")

    if start_time:
        where_clauses.append("time_utc >= %s")
        params.append(start_time)

    if end_time:
        where_clauses.append("time_utc <= %s")
        params.append(end_time)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    query_sql = f"""
        SELECT id, mag, mag_type, place, region, time_utc, updated_utc,
               tsunami, sig, event_status, event_type, url,
               longitude, latitude, depth_km, ingest_run_id, published_at
        FROM curated.earthquakes
        {where_sql}
        ORDER BY time_utc DESC
        LIMIT %s
    """
    with get_db_cursor() as cur:
        cur.execute(query_sql, tuple(params + [limit]))
        rows = cur.fetchall()

    if format == "csv":
        output = io.StringIO()
        fieldnames = [
            "id", "mag", "mag_type", "place", "region", "time_utc",
            "longitude", "latitude", "depth_km", "tsunami", "sig",
            "event_status", "event_type", "url"
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            writer.writerow({
                "id": r["id"],
                "mag": float(r["mag"]) if r["mag"] is not None else "",
                "mag_type": r["mag_type"] or "",
                "place": r["place"] or "",
                "region": r["region"] or "",
                "time_utc": r["time_utc"].isoformat() if r["time_utc"] else "",
                "longitude": float(r["longitude"]),
                "latitude": float(r["latitude"]),
                "depth_km": float(r["depth_km"]) if r["depth_km"] is not None else "",
                "tsunami": "true" if r["tsunami"] else "false",
                "sig": r["sig"] if r["sig"] is not None else "",
                "event_status": r["event_status"] or "",
                "event_type": r["event_type"] or "",
                "url": r["url"] or "",
            })

        csv_content = output.getvalue()
        output.close()

        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="sismowatch_earthquakes.csv"'},
        )

    # GeoJSON
    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "id": r["id"],
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(r["longitude"]),
                    float(r["latitude"]),
                    float(r["depth_km"]) if r["depth_km"] is not None else 0.0,
                ],
            },
            "properties": {
                "mag": float(r["mag"]) if r["mag"] is not None else None,
                "mag_type": r["mag_type"],
                "place": r["place"],
                "region": r["region"],
                "time_utc": r["time_utc"].isoformat() if r["time_utc"] else None,
                "tsunami": bool(r["tsunami"]),
                "sig": r["sig"],
                "event_status": r["event_status"],
                "event_type": r["event_type"],
                "url": r["url"],
            },
        })

    geojson_data = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "count": len(features),
        },
        "features": features,
    }

    return Response(
        content=json.dumps(geojson_data, ensure_ascii=False),
        media_type="application/geo+json",
        headers={"Content-Disposition": 'attachment; filename="sismowatch_earthquakes.geojson"'},
    )


@router.get("/{id}", response_model=EarthquakeResponse, summary="Obtener detalle de un sismo por ID")
def get_earthquake_by_id(
    id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> EarthquakeResponse:
    """Retorna la información completa de un evento sísmico específico."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, mag, mag_type, place, region, time_utc, updated_utc,
                   tsunami, sig, event_status, event_type, url,
                   longitude, latitude, depth_km, ingest_run_id, published_at
            FROM curated.earthquakes
            WHERE id = %s
            """,
            (id,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Evento sísmico con id '{id}' no encontrado",
        )

    return _row_to_earthquake_response(row)
