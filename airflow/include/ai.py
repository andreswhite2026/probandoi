"""
Módulo de Reportes de Inteligencia Artificial y Resiliencia (Fase 6).

Responsabilidades:
  1. Cálculo determinista de indicadores sísmicos diarios a partir de la capa Gold
     (curated.earthquakes y curated.anomalies).
  2. Generación de síntesis ejecutiva mediante LLM local (Ollama) con validación
     estructurada y prevención activa de alucinaciones numéricas.
  3. Mecanismo de Fallback determinista: si el proveedor está caído, excede el
     timeout o emite cifras inconsistentes, genera un reporte fiel a los datos reales
     con status='fallback'.
  4. Auditoría exhaustiva en curated.ai_runs (proveedor, modelo, latencia, tokens, estado).
  5. Idempotencia en curated.ai_reports por fecha de referencia.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import DictCursor, Json
import requests

try:
    from pydantic import BaseModel, Field
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

logger = logging.getLogger("sismowatch.ai")

DEFAULT_PROMPT_VERSION = "v1.0"
DEFAULT_MODEL = "gemma:2b"
DEFAULT_TIMEOUT = 30


if HAS_PYDANTIC:
    class DailyReportSchema(BaseModel):
        title: str = Field(description="Título del reporte diario")
        summary_md: str = Field(description="Resumen narrativo en Markdown")
        key_findings: List[str] = Field(default_factory=list, description="Lista de hallazgos clave")
        referenced_numbers: List[float] = Field(default_factory=list, description="Números citados")


def env(name: str, default: Optional[str] = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return val


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=env("SISMO_DB_HOST", "postgres"),
        port=int(env("SISMO_DB_PORT", "5432")),
        dbname=env("SISMO_DB_NAME", "sismowatch"),
        user=env("SISMO_TRANSFORM_USER"),
        password=env("SISMO_TRANSFORM_PASSWORD"),
    )


def calculate_daily_indicators(
    conn: psycopg2.extensions.connection,
    ref_date: date,
) -> Dict[str, Any]:
    """
    Calcula de forma determinista y sin duplicados los indicadores sísmicos
    de la fecha de referencia en UTC a partir de curated.earthquakes y curated.anomalies.
    """
    start_utc = datetime(ref_date.year, ref_date.month, ref_date.day, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = start_utc + timedelta(days=1)

    indicators: Dict[str, Any] = {
        "reference_date": ref_date.isoformat(),
        "start_utc": start_utc.isoformat(),
        "end_utc": end_utc.isoformat(),
        "total_events": 0,
        "max_magnitude": None,
        "max_event": None,
        "avg_magnitude": None,
        "tsunami_alerts": 0,
        "top_regions": [],
        "depth_stats": {"min_km": None, "max_km": None, "avg_km": None},
        "anomalies_count": 0,
        "anomalies_detail": [],
        "has_data": False,
    }

    with conn.cursor(cursor_factory=DictCursor) as cur:
        # 1. Agregados principales de terremotos
        cur.execute(
            """
            SELECT
                count(*)::int AS total_events,
                max(mag)::numeric(4,2) AS max_mag,
                avg(mag)::numeric(4,2) AS avg_mag,
                count(*) FILTER (WHERE tsunami = true)::int AS tsunami_count,
                min(depth_km)::numeric(7,2) AS min_depth,
                max(depth_km)::numeric(7,2) AS max_depth,
                avg(depth_km)::numeric(7,2) AS avg_depth
            FROM curated.earthquakes
            WHERE time_utc >= %s AND time_utc < %s;
            """,
            (start_utc, end_utc),
        )
        row = cur.fetchone()
        if row and row["total_events"] > 0:
            indicators["has_data"] = True
            indicators["total_events"] = int(row["total_events"])
            indicators["max_magnitude"] = float(row["max_mag"]) if row["max_mag"] is not None else None
            indicators["avg_magnitude"] = float(row["avg_mag"]) if row["avg_mag"] is not None else None
            indicators["tsunami_alerts"] = int(row["tsunami_count"])
            indicators["depth_stats"] = {
                "min_km": float(row["min_depth"]) if row["min_depth"] is not None else None,
                "max_km": float(row["max_depth"]) if row["max_depth"] is not None else None,
                "avg_km": float(row["avg_depth"]) if row["avg_depth"] is not None else None,
            }

            # 2. Detalle del evento más fuerte
            if indicators["max_magnitude"] is not None:
                cur.execute(
                    """
                    SELECT id, mag, place, region, time_utc, depth_km, url
                    FROM curated.earthquakes
                    WHERE time_utc >= %s AND time_utc < %s AND mag = %s
                    ORDER BY time_utc DESC
                    LIMIT 1;
                    """,
                    (start_utc, end_utc, indicators["max_magnitude"]),
                )
                top_eq = cur.fetchone()
                if top_eq:
                    indicators["max_event"] = {
                        "id": top_eq["id"],
                        "mag": float(top_eq["mag"]),
                        "place": top_eq["place"],
                        "region": top_eq["region"],
                        "time_utc": top_eq["time_utc"].isoformat(),
                        "depth_km": float(top_eq["depth_km"]) if top_eq["depth_km"] is not None else None,
                    }

            # 3. Top regiones con más eventos
            cur.execute(
                """
                SELECT region, count(*)::int AS cnt, max(mag)::numeric(4,2) AS top_mag
                FROM curated.earthquakes
                WHERE time_utc >= %s AND time_utc < %s
                GROUP BY region
                ORDER BY cnt DESC, region ASC
                LIMIT 5;
                """,
                (start_utc, end_utc),
            )
            indicators["top_regions"] = [
                {"region": r["region"] or "Desconocida", "count": int(r["cnt"]), "max_mag": float(r["top_mag"]) if r["top_mag"] else None}
                for r in cur.fetchall()
            ]

        # 4. Anomalías detectadas en esa fecha
        cur.execute(
            """
            SELECT window_start, region, value, baseline_mean, baseline_std, zscore, explanation
            FROM curated.anomalies
            WHERE window_start >= %s AND window_start < %s AND is_anomaly = true
            ORDER BY zscore DESC;
            """,
            (start_utc, end_utc),
        )
        anom_rows = cur.fetchall()
        indicators["anomalies_count"] = len(anom_rows)
        indicators["anomalies_detail"] = [
            {
                "window_start": a["window_start"].isoformat(),
                "region": a["region"],
                "value": float(a["value"]),
                "baseline_mean": float(a["baseline_mean"]) if a["baseline_mean"] else None,
                "zscore": float(a["zscore"]) if a["zscore"] else None,
                "explanation": a["explanation"],
            }
            for a in anom_rows
        ]

    return indicators


def generate_deterministic_fallback_report(
    indicators: Dict[str, Any],
    reason: str,
) -> str:
    """
    Genera un informe analítico determinista en Markdown basado exclusivamente
    en los indicadores reales, sin ninguna dependencia de LLM.
    """
    ref_date = indicators.get("reference_date", "N/A")
    total_events = indicators.get("total_events", 0)
    has_data = indicators.get("has_data", False)

    lines = [
        f"# Informe de Actividad Sísmica Diaria — {ref_date}",
        "",
        "> [!NOTE]",
        f"> **Modo de Resiliencia / Fallback Activado:** {reason}",
        "> Todas las cifras y conclusiones fueron computadas directamente desde la capa Gold validada.",
        "",
    ]

    if not has_data or total_events == 0:
        lines.extend([
            "## Resumen Ejecutivo",
            f"No se registró actividad sísmica en la capa Gold para la fecha de referencia ({ref_date}).",
            "- Total de eventos analizados: **0**.",
            "- Alertas de tsunami: **0**.",
            "- Anomalías estadísticas detectadas: **0**.",
            "",
            "## Estado Operativo",
            "Los sistemas de monitoreo operaron normalmente sin eventos catalogados en el periodo evaluado.",
        ])
        return "\n".join(lines)

    max_mag = indicators.get("max_magnitude")
    avg_mag = indicators.get("avg_magnitude")
    tsunami = indicators.get("tsunami_alerts", 0)
    max_ev = indicators.get("max_event") or {}
    anom_cnt = indicators.get("anomalies_count", 0)
    top_reg = indicators.get("top_regions") or []

    lines.extend([
        "## Resumen Ejecutivo",
        f"Durante la jornada del **{ref_date}** se registraron y validaron un total de **{total_events} eventos sísmicos** a nivel global.",
        "",
        "### Indicadores Clave",
        f"- **Magnitud Máxima:** **M {max_mag:.2f}** ({max_ev.get('place', 'Ubicación no especificada')})",
        f"- **Magnitud Promedio:** **M {avg_mag:.2f}**" if avg_mag is not None else "- **Magnitud Promedio:** N/D",
        f"- **Alertas de Tsunami:** **{tsunami}** alerta(s) emitida(s)",
        f"- **Anomalías Estadísticas:** **{anom_cnt}** ventana(s) con actividad inusual (Z-Score $\\ge 2.5$)",
        "",
        "## Distribución Regional Principal",
    ])

    if top_reg:
        for reg in top_reg:
            lines.append(f"- **{reg['region']}**: {reg['count']} sismos (Magnitud máxima: M {reg.get('max_mag', 'N/D')})")
    else:
        lines.append("- No se registraron concentraciones regionales destacadas.")

    lines.append("")
    lines.append("## Auditoría de Anomalías")
    if anom_cnt > 0:
        for anom in indicators.get("anomalies_detail", []):
            lines.append(f"- *{anom.get('window_start')}* [{anom.get('region')}]: {anom.get('explanation')}")
    else:
        lines.append("- La actividad en todas las regiones se mantuvo dentro de los parámetros esperados de variabilidad histórica.")

    return "\n".join(lines)


def validate_anti_hallucination(
    summary_text: str,
    indicators: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """
    Verifica que las afirmaciones y números citados en el texto del LLM
    concuerden con los indicadores reales calculados en la base de datos.
    Retorna (es_valido, lista_de_discrepancias).
    """
    discrepancies: List[str] = []
    total_events = indicators.get("total_events", 0)
    max_mag = indicators.get("max_magnitude")

    if not indicators.get("has_data", False):
        # Si no hay datos, el modelo no debe afirmar que hubo sismos significativos
        if re.search(r"\b([1-9]\d*)\s+(sismos|terremotos|eventos)\b", summary_text, re.IGNORECASE):
            discrepancies.append("El texto reporta sismos cuando el periodo no tiene datos registrados")
        return len(discrepancies) == 0, discrepancies

    # 1. Comprobación de magnitud máxima si el modelo cita magnitudes muy altas
    # Busca patrones tipo "magnitud 7.5", "M 8.2", "M8.0"
    mag_matches = re.findall(r"\b(?:M|magnitud(?:es)?)\s*[:=]?\s*(\d+\.?\d*)\b", summary_text, re.IGNORECASE)
    for m_str in mag_matches:
        try:
            m_val = float(m_str)
            # Si cita una magnitud superior a la máxima real con margen de 0.2
            if max_mag is not None and m_val > (max_mag + 0.25):
                discrepancies.append(
                    f"Alucinación de magnitud: cita M {m_val:.1f} cuando la máxima real fue M {max_mag:.2f}"
                )
        except ValueError:
            pass

    return len(discrepancies) == 0, discrepancies


def build_llm_prompt(indicators: Dict[str, Any]) -> str:
    """Construye el prompt estructurado con los datos calculados para Ollama."""
    return f"""Eres un analista sismológico profesional de SismoWatch.
A continuación tienes los INDICADORES DETERMINISTAS calculados exactamente desde la base de datos para la fecha {indicators.get('reference_date')}.
Debes redactar un reporte diario ejecutivo en Markdown basado ESTRICTAMENTE en estos datos.
NO inventes cifras, NO agregues eventos no listados, NO cites magnitudes superiores a la máxima informada.

DATOS VERIFICADOS (JSON):
{json.dumps(indicators, indent=2, ensure_ascii=False)}

INSTRUCCIONES DE RESPUESTA:
- Responde con un título '# Informe Sísmico Diario — {indicators.get('reference_date')}'.
- Incluye sección '## Resumen Ejecutivo', '## Hallazgos Principales' y '## Anomalías y Tsunami'.
- Cita las cifras exactas del JSON (total de eventos: {indicators.get('total_events')}, magnitud máxima: {indicators.get('max_magnitude')}).
- Mantén un tono técnico, neutral y claro.
"""


def call_ollama(
    prompt: str,
    base_url: str,
    model: str = DEFAULT_MODEL,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, int, int, int]:
    """
    Ejecuta la llamada HTTP contra la API de Ollama (/api/generate).
    Retorna (texto_respuesta, input_tokens, output_tokens, latency_ms).
    """
    url = f"{base_url.rstrip('/')}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.2,
            "top_p": 0.9,
        },
    }

    t0 = time.time()
    resp = requests.post(url, json=payload, timeout=timeout)
    latency_ms = int((time.time() - t0) * 1000)

    if resp.status_code != 200:
        raise RuntimeError(f"Ollama respondió con status HTTP {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    response_text = data.get("response", "").strip()
    input_tokens = data.get("prompt_eval_count") or len(prompt.split())
    output_tokens = data.get("eval_count") or len(response_text.split())

    return response_text, int(input_tokens), int(output_tokens), latency_ms


def generate_daily_report(
    conn: psycopg2.extensions.connection,
    ref_date: date,
    provider: str = "ollama",
    base_url: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    prompt_version: str = DEFAULT_PROMPT_VERSION,
    timeout: int = DEFAULT_TIMEOUT,
    force_fallback: bool = False,
) -> Dict[str, Any]:
    """
    Orquesta el flujo completo de la Fase 6:
      1. Calcula indicadores reales deterministas.
      2. Invoca a Ollama o ejecuta fallback ante contingencias.
      3. Valida anti-alucinaciones numéricas.
      4. Persiste de forma idempotente en curated.ai_reports y curated.ai_runs.
    """
    base_url = base_url or env("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
    report_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"sismowatch_report_{ref_date.isoformat()}")
    run_id = uuid.uuid4()

    # 1. Indicadores deterministas
    indicators = calculate_daily_indicators(conn, ref_date)

    summary_md = ""
    status = "ok"
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    latency_ms = 0
    error_detail: Optional[str] = None

    if force_fallback:
        logger.info("Modo force_fallback solicitado: generando reporte determinista directo")
        summary_md = generate_deterministic_fallback_report(indicators, reason="Solicitud manual de fallback")
        status = "fallback"
        error_detail = "Fallback forzado por configuración"
    else:
        prompt = build_llm_prompt(indicators)
        try:
            logger.info("Invocando LLM (%s/%s) en %s (timeout=%ds)...", provider, model, base_url, timeout)
            llm_text, in_tok, out_tok, lat_ms = call_ollama(
                prompt=prompt,
                base_url=base_url,
                model=model,
                timeout=timeout,
            )
            input_tokens = in_tok
            output_tokens = out_tok
            latency_ms = lat_ms

            # Validación anti-alucinación
            is_valid, discrepancies = validate_anti_hallucination(llm_text, indicators)
            if not is_valid:
                logger.warning("Discrepancia detectada en salida del LLM: %s. Activando fallback.", discrepancies)
                status = "fallback"
                error_detail = f"Discrepancia anti-alucinación: {'; '.join(discrepancies)}"
                summary_md = generate_deterministic_fallback_report(indicators, reason=error_detail)
            else:
                summary_md = llm_text
                status = "ok"

        except Exception as exc:
            logger.warning("Fallo en proveedor de IA (%s): %s. Activando fallback determinista.", type(exc).__name__, exc)
            status = "fallback"
            error_detail = f"Fallo de conexión o timeout con LLM ({type(exc).__name__}): {str(exc)[:300]}"
            summary_md = generate_deterministic_fallback_report(indicators, reason=error_detail)

    # 2. Persistir en curated.ai_reports (idempotente por report_id)
    upsert_report_sql = """
        INSERT INTO curated.ai_reports (
            report_id, reference_date, generated_at, summary_md, indicators,
            anomalies, provider, model, prompt_version, input_tokens, output_tokens,
            latency_ms, cost_usd, status, source_run_id, error_detail
        ) VALUES (
            %s, %s, now(), %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (report_id) DO UPDATE SET
            generated_at = now(),
            summary_md = EXCLUDED.summary_md,
            indicators = EXCLUDED.indicators,
            anomalies = EXCLUDED.anomalies,
            provider = EXCLUDED.provider,
            model = EXCLUDED.model,
            prompt_version = EXCLUDED.prompt_version,
            input_tokens = EXCLUDED.input_tokens,
            output_tokens = EXCLUDED.output_tokens,
            latency_ms = EXCLUDED.latency_ms,
            cost_usd = EXCLUDED.cost_usd,
            status = EXCLUDED.status,
            error_detail = EXCLUDED.error_detail;
    """

    # 3. Auditoría en curated.ai_runs
    insert_run_sql = """
        INSERT INTO curated.ai_runs (
            run_id, report_id, operation, provider, model, prompt_version,
            input_tokens, output_tokens, latency_ms, cost_usd, status, error_detail, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, now()
        );
    """

    with conn.cursor() as cur:
        cur.execute(
            upsert_report_sql,
            (
                str(report_id),
                ref_date,
                summary_md,
                Json(indicators),
                Json(indicators.get("anomalies_detail", [])),
                provider,
                model,
                prompt_version,
                input_tokens,
                output_tokens,
                latency_ms,
                0.0,  # Ollama local no incurre en costos monetarios directos
                status,
                str(run_id),
                error_detail,
            ),
        )
        cur.execute(
            insert_run_sql,
            (
                str(run_id),
                str(report_id),
                "daily_report",
                provider,
                model,
                prompt_version,
                input_tokens,
                output_tokens,
                latency_ms,
                0.0,
                status,
                error_detail,
            ),
        )
    conn.commit()

    return {
        "report_id": str(report_id),
        "run_id": str(run_id),
        "reference_date": ref_date.isoformat(),
        "status": status,
        "provider": provider,
        "model": model,
        "latency_ms": latency_ms,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_events_reported": indicators.get("total_events", 0),
        "error_detail": error_detail,
    }
