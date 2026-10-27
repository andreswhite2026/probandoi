"""
Router para el Agente Analítico de IA con herramientas de solo lectura sobre Gold.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Tuple

import requests
from fastapi import APIRouter, Depends

from app.config import settings
from app.db import get_db_cursor
from app.models import AgentQueryRequest, AgentQueryResponse
from app.security import get_current_user

logger = logging.getLogger("sismowatch.api.agent")
router = APIRouter(prefix="/agent", tags=["Agente de IA"])


# ---------------------------------------------------------------------------
# Herramientas Deterministas de Solo Lectura sobre Gold
# ---------------------------------------------------------------------------
def _tool_get_kpis() -> Tuple[str, Dict[str, Any], str]:
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_events,
                COALESCE(MAX(mag), 0.0) AS max_mag,
                COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0) AS avg_mag,
                COUNT(*) FILTER (WHERE tsunami = true) AS tsunami_alerts,
                MIN(time_utc) AS oldest_event,
                MAX(time_utc) AS newest_event
            FROM curated.earthquakes
            """
        )
        stats = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS active_anomalies
            FROM curated.anomalies
            WHERE is_anomaly = true
            """
        )
        anomalies = cur.fetchone()

    data = {
        "total_eventos_curados": int(stats["total_events"] or 0),
        "magnitud_maxima": float(stats["max_mag"] or 0.0),
        "magnitud_promedio": float(stats["avg_mag"] or 0.0),
        "alertas_tsunami": int(stats["tsunami_alerts"] or 0),
        "anomalias_activas": int(anomalies["active_anomalies"] or 0),
        "fecha_mas_antigua": stats["oldest_event"].isoformat() if stats["oldest_event"] else None,
        "fecha_mas_reciente": stats["newest_event"].isoformat() if stats["newest_event"] else None,
    }
    summary = (
        f"Se registran {data['total_eventos_curados']} eventos curados en Gold. "
        f"Magnitud máxima: {data['magnitud_maxima']}, media: {data['magnitud_promedio']}. "
        f"Alertas de tsunami: {data['alertas_tsunami']}. Anomalías activas detectadas: {data['anomalias_activas']}."
    )
    return "kpis", data, summary


def _tool_get_strongest_earthquakes() -> Tuple[str, List[Dict[str, Any]], str]:
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, mag, place, region, time_utc, depth_km, tsunami
            FROM curated.earthquakes
            ORDER BY mag DESC NULLS LAST, time_utc DESC
            LIMIT 5
            """
        )
        rows = cur.fetchall()

    data = [
        {
            "id": r["id"],
            "magnitud": float(r["mag"]) if r["mag"] is not None else 0.0,
            "lugar": r["place"] or "Sin lugar",
            "region": r["region"] or "Desconocida",
            "fecha_utc": r["time_utc"].isoformat() if r["time_utc"] else None,
            "profundidad_km": float(r["depth_km"]) if r["depth_km"] is not None else 0.0,
            "alerta_tsunami": bool(r["tsunami"]),
        }
        for r in rows
    ]

    items_text = ", ".join(
        [f"M{item['magnitud']} en {item['lugar']} ({item['fecha_utc'][:10] if item['fecha_utc'] else ''})" for item in data]
    )
    summary = f"Los sismos de mayor magnitud registrados son: {items_text}." if data else "No hay sismos registrados."
    return "strongest_earthquakes", data, summary


def _tool_get_regional_activity() -> Tuple[str, List[Dict[str, Any]], str]:
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT region, COUNT(*) AS event_count, MAX(mag) AS max_mag, ROUND(AVG(mag)::numeric, 2) AS avg_mag
            FROM curated.earthquakes
            WHERE region IS NOT NULL AND region <> ''
            GROUP BY region
            ORDER BY event_count DESC
            LIMIT 5
            """
        )
        rows = cur.fetchall()

    data = [
        {
            "region": r["region"],
            "eventos": int(r["event_count"]),
            "magnitud_maxima": float(r["max_mag"] or 0.0),
            "magnitud_promedio": float(r["avg_mag"] or 0.0),
        }
        for r in rows
    ]

    items_text = ", ".join([f"{item['region']} ({item['eventos']} eventos, máx M{item['magnitud_maxima']})" for item in data])
    summary = f"Top 5 regiones con mayor actividad sísmica: {items_text}." if data else "No hay actividad regional disponible."
    return "regional_activity", data, summary


def _tool_get_latest_report() -> Tuple[str, Dict[str, Any], str]:
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT report_id, reference_date, status, provider, model, summary_md, indicators, generated_at
            FROM curated.ai_reports
            ORDER BY reference_date DESC, generated_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if not row:
        return "latest_report", {}, "Aún no se ha generado ningún informe diario de IA en la capa Gold."

    data = {
        "report_id": str(row["report_id"]),
        "reference_date": str(row["reference_date"]),
        "status": row["status"],
        "provider": row["provider"],
        "model": row["model"],
        "indicadores": row["indicators"] if isinstance(row["indicators"], dict) else {},
        "extracto": row["summary_md"][:350] if row["summary_md"] else "",
    }
    summary = f"Último informe generado para la fecha {data['reference_date']} (estado: {data['status']}). {data['extracto']}..."
    return "latest_report", data, summary


def _select_tool_by_intent(question: str) -> Tuple[str, Any, str]:
    q = question.lower()
    if any(k in q for k in ["informe", "reporte", "resumen diario", "narrativo", "boletin"]):
        return _tool_get_latest_report()
    elif any(k in q for k in ["region", "región", "regiones", "zona", "zonas", "pais", "país", "donde"]):
        return _tool_get_regional_activity()
    elif any(k in q for k in ["fuerte", "mayor magnitud", "más fuerte", "mas fuerte", "máximo", "maximo", "máxima", "maxima", "grande", "destacado", "epicentro"]):
        return _tool_get_strongest_earthquakes()
    else:
        return _tool_get_kpis()


# ---------------------------------------------------------------------------
# Endpoint del Agente
# ---------------------------------------------------------------------------
@router.post("/query", response_model=AgentQueryResponse, summary="Consultar asistente de IA sobre datos sísmicos")
def query_agent(
    request: AgentQueryRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> AgentQueryResponse:
    """
    Atiende consultas en lenguaje natural invocando herramientas estrictamente
    de lectura sobre Gold y redactando respuestas auditadas.
    """
    tool_name, tool_data, deterministic_summary = _select_tool_by_intent(request.question)

    # Intentar redactar mediante Ollama con contexto de Gold
    ai_status = "fallback"
    ai_answer = deterministic_summary
    latency_ms = 0
    run_id = str(uuid.uuid4())
    start_t = time.perf_counter()

    prompt = (
        f"Eres el asistente analítico de SismoWatch. Responde en español de forma breve (máximo 3 oraciones), "
        f"profesional y precisa a la pregunta del usuario utilizando exclusivamente los siguientes datos reales "
        f"extraídos de la capa Gold:\n\n"
        f"DATOS: {json.dumps(tool_data, ensure_ascii=False)}\n\n"
        f"PREGUNTA: {request.question}\n\n"
        f"RESPUESTA:"
    )

    try:
        resp = requests.post(
            f"{settings.OLLAMA_BASE_URL}/api/generate",
            json={
                "model": settings.OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "top_p": 0.9},
            },
            timeout=8.0,
        )
        latency_ms = int((time.perf_counter() - start_t) * 1000)

        if resp.status_code == 200:
            result_text = resp.json().get("response", "").strip()
            if result_text:
                ai_answer = result_text
                ai_status = "ok"
    except Exception as exc:
        latency_ms = int((time.perf_counter() - start_t) * 1000)
        logger.info("Ollama no disponible para agent_query (%s). Usando respuesta determinista.", exc)

    logger.info(
        "Agent Query audit: run_id=%s, user=%s, tool=%s, status=%s, latency_ms=%s",
        run_id,
        current_user.get("email"),
        tool_name,
        ai_status,
        latency_ms,
    )

    return AgentQueryResponse(
        answer=ai_answer,
        tool_used=tool_name,
        data=tool_data,
    )
