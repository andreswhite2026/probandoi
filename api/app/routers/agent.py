"""
Router para el Agente Analítico de IA con herramientas de solo lectura sobre Gold.
Soporta consultas temporales, geográficas, sismos de mayor magnitud, KPIs e informes.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import APIRouter, Depends

from app.config import settings
from app.db import get_db_cursor
from app.models import AgentQueryRequest, AgentQueryResponse
from app.security import get_current_user

logger = logging.getLogger("sismowatch.api.agent")
router = APIRouter(prefix="/agent", tags=["Agente de IA"])


# ---------------------------------------------------------------------------
# Catálogos de Detección Geográfica y Dominio
# ---------------------------------------------------------------------------
KNOWN_LOCATIONS: Dict[str, Tuple[str, str]] = {
    "colombia": ("colombia", "Colombia"),
    "colombiano": ("colombia", "Colombia"),
    "colombiana": ("colombia", "Colombia"),
    "alaska": ("alaska", "Alaska"),
    "california": ("california", "California"),
    "chile": ("chile", "Chile"),
    "hawaii": ("hawaii", "Hawaii"),
    "hawái": ("hawaii", "Hawaii"),
    "mexico": ("mexico", "México"),
    "méxico": ("mexico", "México"),
    "japon": ("japon", "Japón"),
    "japón": ("japon", "Japón"),
    "peru": ("peru", "Perú"),
    "perú": ("peru", "Perú"),
    "puerto rico": ("puerto rico", "Puerto Rico"),
    "indonesia": ("indonesia", "Indonesia"),
    "filipinas": ("philippines", "Filipinas"),
    "philippines": ("philippines", "Filipinas"),
    "vanuatu": ("vanuatu", "Vanuatu"),
    "tajikistan": ("tajikistan", "Tayikistán"),
    "tayikistán": ("tajikistan", "Tayikistán"),
    "tayikistan": ("tajikistan", "Tayikistán"),
    "china": ("china", "China"),
    "rusia": ("russia", "Rusia"),
    "russia": ("russia", "Rusia"),
    "argentina": ("argentina", "Argentina"),
    "ecuador": ("ecuador", "Ecuador"),
    "italia": ("italy", "Italia"),
    "turquia": ("turkey", "Turquía"),
    "turquía": ("turkey", "Turquía"),
    "nevada": ("nevada", "Nevada"),
    "texas": ("texas", "Texas"),
}

UNSUPPORTED_LOCATIONS: List[str] = [
    "atlantida", "atlántida", "narnia", "mordor", "luna", "marte", "jupiter", "júpiter",
    "krypton", "macondo", "paris", "parís", "madrid", "londres", "berlin", "berlín",
    "europa", "saturno", "sol"
]


def _is_greeting(q: str) -> bool:
    """Verifica si la consulta es un saludo o pregunta de presentación básica."""
    patterns = [
        r"\bhola\b", r"\bbuenos d[ií]as\b", r"\bbuenas tardes\b", r"\bbuenas noches\b",
        r"\bbuen d[ií]a\b", r"\bqui[eé]n eres\b", r"\bc[oó]mo est[aá]s\b", r"\bqu[eé] haces\b",
        r"\bqu[eé] puedes hacer\b", r"\bayuda\b", r"\bsaludos\b"
    ]
    return any(re.search(pat, q) for pat in patterns)


def _is_seismic_domain(q: str) -> bool:
    """Verifica si la pregunta pertenece al dominio de actividad sísmica o SismoWatch."""
    seismic_keywords = [
        "sismo", "sismos", "terremoto", "terremotos", "temblor", "temblores",
        "magnitud", "tsunami", "epicentro", "profundidad", "falla", "placa",
        "actividad", "kpi", "kpis", "informe", "reporte", "boletin", "boletín",
        "calidad", "cuarentena", "gold", "silver", "bronze", "usgs", "evento", "eventos",
        "tiembla", "temblado", "sacudida", "escala", "richter", "anomalia", "anomalía",
        "anomalias", "anomalías", "colombia", "alaska", "california", "chile", "hoy",
        "ayer", "24 horas", "semana"
    ]
    return any(k in q for k in seismic_keywords)


def _parse_temporal_intent(q: str) -> Optional[str]:
    """Extrae la ventana temporal solicitada."""
    # Ventana móvil de 24 horas (prioritaria si menciona '24 horas' o '24h')
    if any(k in q for k in ["24 horas", "24h", "24 hrs", "veinticuatro horas"]):
        return "last_24h"
    # Día calendario actual
    if any(k in q for k in ["hoy", "el dia de hoy", "el día de hoy", "dia de hoy", "día de hoy"]):
        return "today"
    # Ayer
    if any(k in q for k in ["ayer", "el dia de ayer", "el día de ayer", "dia de ayer", "día de ayer"]):
        return "yesterday"
    # Últimos 7 días / esta semana
    if any(k in q for k in ["esta semana", "ultimos 7 dias", "últimos 7 días", "7 dias", "7 días", "ultima semana", "última semana"]):
        return "last_7d"
    return None


def _parse_location_intent(q: str) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Identifica si la consulta restringe a un país o región.
    Retorna (clave_canonico, nombre_visual, es_no_soportado).
    """
    # 1. Comprobar ubicaciones conocidas
    for kw, (canon, disp) in KNOWN_LOCATIONS.items():
        if re.search(r"\b" + re.escape(kw) + r"\b", q):
            return canon, disp, False

    # 2. Comprobar ubicaciones explícitamente no soportadas o ficticias
    for un in UNSUPPORTED_LOCATIONS:
        if re.search(r"\b" + re.escape(un) + r"\b", q):
            return None, un.title(), True

    # 3. Detectar sintaxis 'en <Lugar>'
    m = re.search(r"\ben\s+([a-záéíóúñ]{3,25})\b", q)
    if m:
        candidate = m.group(1).lower()
        stop_words = {
            "el", "la", "los", "las", "un", "una", "este", "esta", "estos", "estas",
            "todo", "toda", "gold", "silver", "bronze", "total", "vivo", "tiempo",
            "promedio", "general", "horas", "dias", "días", "semanas", "meses", "fechas",
            "detalle", "resumen", "minio", "postgres", "linea", "línea"
        }
        if candidate not in stop_words and candidate not in KNOWN_LOCATIONS:
            try:
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS c FROM curated.earthquakes WHERE region ILIKE %s OR place ILIKE %s",
                        (f"%{candidate}%", f"%{candidate}%")
                    )
                    r = cur.fetchone()
                    if r and r["c"] > 0:
                        return candidate, candidate.title(), False
            except Exception:
                pass
            return None, candidate.title(), True

    return None, None, False


def _parse_metric_intent(q: str) -> str:
    """Identifica qué indicador cuantitativo se pide prioritariamente."""
    if any(k in q for k in ["magnitud media", "magnitud promedio", "promedio de magnitud", "media de magnitud", "promedio"]):
        return "avg_mag"
    if any(k in q for k in ["magnitud maxima", "magnitud máxima", "maxima magnitud", "máxima magnitud"]):
        return "max_mag"
    return "count"


# ---------------------------------------------------------------------------
# Herramientas Deterministas sobre la Capa Gold
# ---------------------------------------------------------------------------
def _tool_greeting() -> Tuple[str, Dict[str, Any], str]:
    data = {
        "type": "greeting",
        "asistente": "SismoWatch AI Assistant",
        "capacidades": [
            "Cantidad de sismos de hoy y de las últimas 24 horas",
            "Consultas temporales y geográficas (ej: sismos en Colombia hoy o ayer)",
            "Eventos sísmicos de mayor magnitud registrados",
            "Magnitud media y máxima por período o región",
            "Indicadores globales (KPIs) y resúmenes diarios generados en Gold"
        ]
    }
    summary = (
        "¡Hola! Soy el asistente analítico de SismoWatch. Puedo ayudarte a consultar la actividad sísmica "
        "curada en Gold: cuántos sismos hubo hoy o en las últimas 24 horas, eventos en Colombia o regiones "
        "específicas, magnitudes máximas y promedio, e indicadores globales."
    )
    return "greeting", data, summary


def _tool_out_of_domain() -> Tuple[str, Dict[str, Any], str]:
    data = {
        "status": "out_of_domain",
        "motivo": "Consulta ajena al catálogo sísmico y operaciones de SismoWatch.",
        "temas_soportados": [
            "Sismos de hoy, de ayer o de las últimas 24 horas (global o por región/país)",
            "Actividad sísmica en Colombia y delimitación territorial",
            "Terremoto o sismos con mayor magnitud registrados",
            "KPIs e indicadores globales del catálogo curado",
            "Informes analíticos diarios generados en Gold"
        ]
    }
    summary = (
        "Esta pregunta está fuera del ámbito sísmico de SismoWatch. "
        "Como asistente analítico de la plataforma, respondo consultas sobre eventos sísmicos registrados, "
        "magnitudes, sismos de hoy o de las últimas 24 horas, eventos en Colombia o regiones específicas e informes de calidad."
    )
    return "out_of_domain", data, summary


def _tool_filtered_earthquakes(
    period: Optional[str],
    location: Optional[str],
    raw_location: Optional[str],
    is_unsupported_location: bool,
    metric: str
) -> Tuple[str, Dict[str, Any], str]:
    """
    Ejecuta consultas analíticas parametrizadas combinando restricciones temporales y espaciales sobre Gold.
    """
    if is_unsupported_location:
        loc_name = raw_location or "la ubicación solicitada"
        data = {
            "status": "unsupported_location",
            "ubicacion_consultada": loc_name,
            "motivo": f"La ubicación '{loc_name}' no se encuentra en el catálogo sísmico o no es una región reconocida por el USGS."
        }
        summary = (
            f"No es posible consultar sismos en '{loc_name}' porque no corresponde a una región "
            f"geográfica reconocida en el catálogo del USGS ni en la base de datos de SismoWatch."
        )
        return "filtered_earthquakes", data, summary

    now_utc = datetime.now(timezone.utc)
    ref_time_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    conditions: List[str] = []
    params: List[Any] = []

    # Configuración de ventana temporal
    if period == "today":
        conditions.append("time_utc >= (CURRENT_DATE AT TIME ZONE 'UTC') AND time_utc < ((CURRENT_DATE + INTERVAL '1 day') AT TIME ZONE 'UTC')")
        today_str = now_utc.strftime("%Y-%m-%d")
        period_label = f"hoy ({today_str} UTC)"
        period_window = {
            "tipo": "dia_calendario_utc",
            "inicio_utc": f"{today_str}T00:00:00Z",
            "fin_utc": f"{today_str}T23:59:59Z",
            "definicion": "Día calendario actual en tiempo universal coordinado (UTC)"
        }
    elif period == "last_24h":
        conditions.append("time_utc >= NOW() - INTERVAL '24 hours'")
        start_24h = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S UTC")
        period_label = "durante las últimas 24 horas (ventana móvil)"
        period_window = {
            "tipo": "ventana_movil_24h",
            "inicio_utc": start_24h,
            "fin_utc": ref_time_str,
            "definicion": "Ventana móvil exacta de las últimas 24 horas calculada desde la hora de consulta"
        }
    elif period == "yesterday":
        conditions.append("time_utc >= ((CURRENT_DATE - INTERVAL '1 day') AT TIME ZONE 'UTC') AND time_utc < (CURRENT_DATE AT TIME ZONE 'UTC')")
        yesterday_str = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
        period_label = f"ayer ({yesterday_str} UTC)"
        period_window = {
            "tipo": "dia_calendario_utc",
            "inicio_utc": f"{yesterday_str}T00:00:00Z",
            "fin_utc": f"{yesterday_str}T23:59:59Z",
            "definicion": "Día calendario anterior en tiempo universal coordinado (UTC)"
        }
    elif period == "last_7d":
        conditions.append("time_utc >= NOW() - INTERVAL '7 days'")
        start_7d = (now_utc - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S UTC")
        period_label = "en los últimos 7 días"
        period_window = {
            "tipo": "ventana_movil_7d",
            "inicio_utc": start_7d,
            "fin_utc": ref_time_str,
            "definicion": "Ventana móvil de 7 días"
        }
    else:
        period_label = "en el histórico completo"
        period_window = {
            "tipo": "historico_completo",
            "definicion": "Total histórico curado disponible"
        }

    # Configuración de filtro geográfico
    if location == "colombia":
        conditions.append("(region ILIKE %s OR place ILIKE %s OR (latitude BETWEEN -4.3 AND 13.5 AND longitude BETWEEN -79.5 AND -66.8))")
        params.extend(["%Colombia%", "%Colombia%"])
        location_label = "en Colombia"
        method_desc = "Filtro multicriterio que combina la región normalizada, mención en texto USGS ('Colombia') y delimitación geográfica territorial [-4.3° a 13.5° lat, -79.5° a -66.8° lon]."
    elif location:
        conditions.append("(region ILIKE %s OR place ILIKE %s)")
        params.extend([f"%{location}%", f"%{location}%"])
        loc_display = raw_location or location.title()
        location_label = f"en {loc_display}"
        method_desc = f"Filtro por coincidencia de texto en región sismológica y lugar USGS para '{loc_display}'."
    else:
        location_label = "a nivel global"
        method_desc = "Catálogo global sin restricción geográfica."

    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    sql = f"""
        SELECT
            COUNT(*) AS total_count,
            COALESCE(MAX(mag), 0.0) AS max_mag,
            COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0) AS avg_mag,
            MIN(time_utc) AS oldest_in_window,
            MAX(time_utc) AS newest_in_window
        FROM curated.earthquakes
        WHERE {where_clause}
    """

    try:
        with get_db_cursor() as cur:
            cur.execute(sql, tuple(params))
            stats = cur.fetchone()
    except Exception as exc:
        logger.error("Error al ejecutar _tool_filtered_earthquakes: %s", exc)
        data = {
            "status": "db_error",
            "error_detail": str(exc),
            "periodo": period_label,
            "ubicacion": location_label
        }
        summary = (
            f"No se pudo verificar la consulta para {location_label} {period_label} "
            f"debido a un error en la base de datos."
        )
        return "filtered_earthquakes", data, summary

    total_count = int(stats["total_count"] or 0)
    max_mag = float(stats["max_mag"] or 0.0)
    avg_mag = float(stats["avg_mag"] or 0.0)

    data = {
        "status": "ok",
        "conteo_eventos": total_count,
        "magnitud_maxima": max_mag if total_count > 0 else None,
        "magnitud_promedio": avg_mag if total_count > 0 else None,
        "periodo_solicitado": period_label,
        "ventana_temporal_utc": period_window,
        "ubicacion_consultada": location_label,
        "metodo_geografico": method_desc,
        "referencia_consulta_utc": ref_time_str
    }

    if total_count == 0:
        summary = (
            f"Se confirma que no se registraron eventos sísmicos {location_label} {period_label} "
            f"en la capa Gold (consulta verificada con referencia a las {ref_time_str})."
        )
    elif metric == "avg_mag":
        summary = (
            f"La magnitud promedio de los sismos registrados {location_label} {period_label} "
            f"es de M{avg_mag} (calculada sobre {total_count} evento{'s' if total_count != 1 else ''} curado{'s' if total_count != 1 else ''} en Gold; "
            f"magnitud máxima: M{max_mag})."
        )
    elif metric == "max_mag":
        summary = (
            f"El sismo con mayor magnitud registrado {location_label} {period_label} "
            f"fue de M{max_mag} (de un total de {total_count} evento{'s' if total_count != 1 else ''} curado{'s' if total_count != 1 else ''} en Gold; "
            f"magnitud media: M{avg_mag})."
        )
    else:
        summary = (
            f"Se registraron {total_count} sismo{'s' if total_count != 1 else ''} {location_label} {period_label} "
            f"en la capa Gold (magnitud máxima: M{max_mag}, promedio: M{avg_mag}; "
            f"referencia: {ref_time_str})."
        )

    return "filtered_earthquakes", data, summary


def _tool_get_kpis() -> Tuple[str, Dict[str, Any], str]:
    """Retorna los indicadores cuantitativos globales de toda la capa Gold."""
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
        f"Se registran {data['total_eventos_curados']} eventos curados en Gold en total histórico. "
        f"Magnitud máxima global: {data['magnitud_maxima']}, media global: {data['magnitud_promedio']}. "
        f"Alertas de tsunami globales: {data['alertas_tsunami']}. Anomalías activas detectadas: {data['anomalias_activas']}."
    )
    return "kpis", data, summary


def _tool_get_strongest_earthquakes() -> Tuple[str, List[Dict[str, Any]], str]:
    """Retorna los 5 sismos más fuertes de la historia curada en Gold."""
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

    if data:
        top = data[0]
        top_date = top['fecha_utc'][:10] if top['fecha_utc'] else 'desconocida'
        summary = (
            f"El sismo con mayor magnitud registrado en Gold es de M{top['magnitud']}, "
            f"ocurrido en {top['lugar']} ({top['region']}) el {top_date} "
            f"(ID: {top['id']}, profundidad: {top['profundidad_km']} km)."
        )
    else:
        summary = "No hay sismos registrados en la capa Gold."
    return "strongest_earthquakes", data, summary


def _tool_get_regional_activity(is_country_query: bool = False) -> Tuple[str, List[Dict[str, Any]], str]:
    """Retorna el ranking de actividad sísmica por región sismológica estándar."""
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
    if is_country_query:
        summary = (
            f"SismoWatch agrupa los eventos por región sismológica estandarizada (derivada de las descripciones geográficas del USGS) "
            f"y no por división político-administrativa de país. Las regiones con más terremotos registrados son: {items_text}. "
            f"(Nota: regiones como Alaska o California corresponden a áreas sismológicas de EE. UU., no países independientes)."
        )
    else:
        summary = f"Top 5 regiones con mayor actividad sísmica: {items_text}." if data else "No hay actividad regional disponible."
    return "regional_activity", data, summary


def _tool_get_latest_report() -> Tuple[str, Dict[str, Any], str]:
    """Retorna el último informe diario de IA generado."""
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


# ---------------------------------------------------------------------------
# Clasificador de Intenciones
# ---------------------------------------------------------------------------
def _select_tool_by_intent(question: str) -> Tuple[str, Any, str]:
    q = question.lower().strip()

    # 1. Saludo y conversación básica
    if _is_greeting(q):
        return _tool_greeting()

    # 2. Comprobación de dominio sísmico: si no pertenece, descartar inmediatamente
    if not _is_seismic_domain(q):
        return _tool_out_of_domain()

    # 3. Informes y reportes diarios
    if any(k in q for k in ["informe", "reporte", "resumen diario", "narrativo", "boletin", "boletín"]) and not any(k in q for k in ["cuantos", "cuántos", "conteo"]):
        return _tool_get_latest_report()

    # Extracción de entidades temporales, espaciales y métricas
    temporal = _parse_temporal_intent(q)
    location, raw_location, is_unsupported = _parse_location_intent(q)
    metric = _parse_metric_intent(q)

    # 4. Sismo de mayor magnitud / sismos más fuertes globales (sin período ni ubicación específica)
    is_strongest_query = any(k in q for k in [
        "mayor magnitud", "mas fuerte", "más fuerte", "fuerte", "fuertes",
        "maxima magnitud", "máxima magnitud", "sismo mayor", "terremoto mayor",
        "mas grande", "más grande", "record", "récord"
    ]) and metric != "avg_mag"

    if is_strongest_query and not temporal and not location and not is_unsupported:
        return _tool_get_strongest_earthquakes()

    # 5. Indicadores globales (KPIs explícitos o total histórico, sin período ni ubicación)
    is_global_kpi = any(k in q for k in [
        "kpi", "kpis", "indicadores", "en total", "total de eventos",
        "total de sismos", "panorama general", "resumen general", "registrado en total"
    ])
    if is_global_kpi and not temporal and not location and not is_unsupported:
        return _tool_get_kpis()

    # 6. Actividad regional comparativa / ranking ("país con más sismos", "dónde tiembla más")
    is_country_ranking = any(k in q for k in ["pais con mas", "país con más", "paises con mas", "países con más", "pais con mayor", "país con mayor"])
    is_region_ranking = any(k in q for k in [
        "donde tiembla mas", "dónde tiembla más", "donde hay mas sismos", "dónde hay más sismos",
        "regiones con mas", "regiones con más", "top regiones", "zonas con mas", "zonas con más"
    ])
    if is_country_ranking or is_region_ranking:
        return _tool_get_regional_activity(is_country_query=is_country_ranking)

    # 7. Consultas temporales y/o geográficas (hoy, 24 horas, ayer, Colombia, Alaska, etc.)
    # Se activa si hay período, ubicación o si se solicita cantidad/media de sismos
    if temporal is not None or location is not None or is_unsupported or metric in ("avg_mag", "max_mag") or any(k in q for k in ["cuantos", "cuántos", "cantidad", "numero", "número", "hubo"]):
        return _tool_filtered_earthquakes(
            period=temporal,
            location=location,
            raw_location=raw_location,
            is_unsupported_location=is_unsupported,
            metric=metric
        )

    # 8. Fallback general dentro del dominio sísmico
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

    # Respuestas que no requieren enriquecimiento LLM
    if tool_name in ("out_of_domain", "greeting"):
        logger.info(
            "Agent Query directa (%s): user=%s, question='%s'",
            tool_name,
            current_user.get("email"),
            request.question,
        )
        return AgentQueryResponse(
            answer=deterministic_summary,
            tool_used=tool_name,
            data=tool_data if isinstance(tool_data, (dict, list)) else {},
        )

    if isinstance(tool_data, dict) and tool_data.get("status") in ("db_error", "unsupported_location"):
        return AgentQueryResponse(
            answer=deterministic_summary,
            tool_used=tool_name,
            data=tool_data,
        )

    # Intentar enriquecer la redacción mediante Ollama con contexto de Gold
    ai_status = "fallback"
    ai_answer = deterministic_summary
    latency_ms = 0
    run_id = str(uuid.uuid4())
    start_t = time.perf_counter()

    prompt = (
        f"Eres el asistente analítico de SismoWatch. Responde en español de forma breve (máximo 3 oraciones), "
        f"profesional, natural y precisa a la pregunta del usuario utilizando exclusivamente los siguientes datos reales "
        f"extraídos de la capa Gold. No inventes cifras ni menciones datos globales si se consultó un filtro específico.\n\n"
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
        data=tool_data if isinstance(tool_data, (dict, list)) else {},
    )
