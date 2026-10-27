#!/usr/bin/env python3
"""
Job de Publicación Gold y Detección de Anomalías (Fase 4).

Responsabilidades:
  1. Identificar lotes pendientes aprobados en staging.earthquakes y curated.quality_runs.
  2. Aplicar la compuerta de calidad (Quality Gate): no publicar lotes no aprobados.
  3. Upsert idempotente de eventos aprobados en curated.earthquakes.
  4. Agregación y cálculo de Z-Score sobre ventanas horarias por región.
  5. Registro idempotente de anomalías en curated.anomalies.
  6. Actualización del estado del pipeline en curated.pipeline_runs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import DictCursor, execute_values

sys.path.insert(0, "/opt/airflow")
from include.gold import (
    DEFAULT_MIN_SCORE,
    DEFAULT_ZSCORE_THRESHOLD,
    check_quality_gate,
    compute_anomalies_for_windows,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sismowatch.gold")


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


def find_batches_to_publish(conn: psycopg2.extensions.connection, specific_run_id: Optional[str] = None) -> List[str]:
    """
    Identifica los ingest_run_id en staging que tienen métricas de calidad y aún no han
    sido completamente publicados en curated.earthquakes (o el run_id especificado).
    """
    with conn.cursor() as cur:
        if specific_run_id:
            return [specific_run_id]

        # Lotes en staging que ya fueron procesados por calidad
        cur.execute(
            """
            SELECT DISTINCT s.ingest_run_id::text
            FROM staging.earthquakes s
            JOIN curated.quality_runs qr ON qr.run_id = s.ingest_run_id
            WHERE qr.status = 'passed'
            ORDER BY s.ingest_run_id::text
            """
        )
        rows = cur.fetchall()
        return [r[0] for r in rows]


def get_quality_run(conn: psycopg2.extensions.connection, run_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT run_id, dag_run_id, layer, evaluated, approved, rejected, score, status
            FROM curated.quality_runs
            WHERE run_id = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def publish_batch_to_gold(conn: psycopg2.extensions.connection, run_id: str) -> int:
    """
    Inserta/actualiza eventos aprobados desde staging.earthquakes hacia curated.earthquakes.
    Retorna la cantidad de eventos afectados.
    """
    upsert_sql = """
        INSERT INTO curated.earthquakes (
            id, mag, mag_type, place, region, time_utc, updated_utc,
            tsunami, sig, event_status, event_type, url,
            longitude, latitude, depth_km, ingest_run_id, published_at
        )
        SELECT
            id, mag, mag_type, place, region, time_utc, updated_utc,
            tsunami, sig, event_status, event_type, url,
            longitude, latitude, depth_km, ingest_run_id, now()
        FROM staging.earthquakes
        WHERE ingest_run_id = %s
        ON CONFLICT (id) DO UPDATE SET
            mag = EXCLUDED.mag,
            mag_type = EXCLUDED.mag_type,
            place = EXCLUDED.place,
            region = EXCLUDED.region,
            time_utc = EXCLUDED.time_utc,
            updated_utc = EXCLUDED.updated_utc,
            tsunami = EXCLUDED.tsunami,
            sig = EXCLUDED.sig,
            event_status = EXCLUDED.event_status,
            event_type = EXCLUDED.event_type,
            url = EXCLUDED.url,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            depth_km = EXCLUDED.depth_km,
            ingest_run_id = EXCLUDED.ingest_run_id,
            published_at = now()
        WHERE EXCLUDED.updated_utc >= curated.earthquakes.updated_utc
           OR curated.earthquakes.updated_utc IS NULL;
    """
    with conn.cursor() as cur:
        cur.execute(upsert_sql, (run_id,))
        count = cur.rowcount
    return count


def calculate_and_save_anomalies(
    conn: psycopg2.extensions.connection,
    threshold: float = DEFAULT_ZSCORE_THRESHOLD,
) -> int:
    """
    Calcula baselines por región (media y desviación estándar) sobre ventanas de 1 hora,
    evalúa cada ventana para detectar desviaciones estadísticas (Z-score) y realiza
    upsert en curated.anomalies. Retorna el número de anomalías persistidas.
    """
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # 1. Conteo por ventana horaria y región en curated.earthquakes
        cur.execute(
            """
            SELECT
                date_trunc('hour', time_utc) AS window_start,
                region,
                count(*)::int AS event_count
            FROM curated.earthquakes
            GROUP BY date_trunc('hour', time_utc), region
            ORDER BY window_start DESC
            """
        )
        window_rows = [dict(r) for r in cur.fetchall()]

        if not window_rows:
            logger.info("No hay ventanas horarias para calcular anomalías en curated.earthquakes")
            return 0

        # 2. Baselines regionales (media y desviación estándar)
        cur.execute(
            """
            WITH hourly AS (
                SELECT
                    date_trunc('hour', time_utc) AS window_start,
                    region,
                    count(*)::numeric AS cnt
                FROM curated.earthquakes
                GROUP BY date_trunc('hour', time_utc), region
            )
            SELECT
                region,
                avg(cnt)::numeric AS mean_count,
                coalesce(stddev_samp(cnt), 0)::numeric AS std_count
            FROM hourly
            GROUP BY region
            """
        )
        baseline_rows = cur.fetchall()
        baselines: Dict[str, Tuple[float, float]] = {
            r["region"]: (float(r["mean_count"] or 0.0), float(r["std_count"] or 0.0))
            for r in baseline_rows
        }

    # 3. Evaluar con lógica pura
    anomalies_data = compute_anomalies_for_windows(window_rows, baselines, threshold=threshold)

    # 4. Upsert en curated.anomalies
    insert_sql = """
        INSERT INTO curated.anomalies (
            window_start, window_hours, region, method, metric,
            value, baseline_mean, baseline_std, zscore, is_anomaly,
            explanation, detected_at
        ) VALUES %s
        ON CONFLICT (window_start, region, method) DO UPDATE SET
            value = EXCLUDED.value,
            baseline_mean = EXCLUDED.baseline_mean,
            baseline_std = EXCLUDED.baseline_std,
            zscore = EXCLUDED.zscore,
            is_anomaly = EXCLUDED.is_anomaly,
            explanation = EXCLUDED.explanation,
            detected_at = now();
    """
    tuples = [
        (
            a["window_start"],
            a["window_hours"],
            a["region"],
            a["method"],
            a["metric"],
            a["value"],
            a["baseline_mean"],
            a["baseline_std"],
            a["zscore"],
            a["is_anomaly"],
            a["explanation"],
            datetime.now(timezone.utc),
        )
        for a in anomalies_data
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, tuples)

    anomalies_flagged = sum(1 for a in anomalies_data if a["is_anomaly"])
    logger.info(
        "Anomalías evaluadas: %d ventanas procesadas, %d anomalías marcadas",
        len(anomalies_data),
        anomalies_flagged,
    )
    return len(anomalies_data)


def update_pipeline_run(
    conn: psycopg2.extensions.connection,
    dag_id: str,
    run_id: str,
    state: str,
    started_at: datetime,
    note: str,
) -> None:
    upsert_sql = """
        INSERT INTO curated.pipeline_runs (dag_id, run_id, state, started_at, finished_at, updated_at, note)
        VALUES (%s, %s, %s, %s, %s, now(), %s)
        ON CONFLICT (dag_id, run_id) DO UPDATE SET
            state = EXCLUDED.state,
            finished_at = EXCLUDED.finished_at,
            updated_at = now(),
            note = EXCLUDED.note;
    """
    with conn.cursor() as cur:
        cur.execute(upsert_sql, (dag_id, run_id, state, started_at, datetime.now(timezone.utc), note))


def main() -> None:
    parser = argparse.ArgumentParser(description="Publicación Gold y Detección de Anomalías")
    parser.add_argument("--run-id", type=str, default=None, help="ID específico del lote a publicar")
    parser.add_argument("--dag-run-id", type=str, default="manual_publish_gold", help="DAG Run ID de Airflow")
    parser.add_argument("--min-score", type=float, default=DEFAULT_MIN_SCORE, help="Puntaje mínimo de calidad requerido")
    parser.add_argument("--anomaly-threshold", type=float, default=DEFAULT_ZSCORE_THRESHOLD, help="Umbral Z-Score para anomalías")
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    conn = get_db_connection()
    conn.autocommit = False

    try:
        batches = find_batches_to_publish(conn, specific_run_id=args.run_id)
        if not batches:
            logger.info("No se encontraron lotes pendientes para publicar en Gold")
            update_pipeline_run(
                conn,
                dag_id="publish_gold",
                run_id=args.dag_run_id,
                state="success",
                started_at=started_at,
                note="Sin lotes pendientes",
            )
            conn.commit()
            print(json.dumps({"published_events": 0, "status": "skipped", "message": "No pending batches"}))
            return

        total_published = 0
        for batch_id in batches:
            qrun = get_quality_run(conn, batch_id)
            ok, reason = check_quality_gate(qrun, min_score=args.min_score)
            if not ok:
                logger.warning("Lote %s rechazado por Quality Gate: %s", batch_id, reason)
                continue

            published_count = publish_batch_to_gold(conn, batch_id)
            total_published += published_count
            logger.info("Lote %s publicado en curated.earthquakes (%d eventos)", batch_id, published_count)

        # Cálculo y registro de anomalías sobre curated.earthquakes
        windows_evaluated = calculate_and_save_anomalies(conn, threshold=args.anomaly_threshold)

        update_pipeline_run(
            conn,
            dag_id="publish_gold",
            run_id=args.dag_run_id,
            state="success",
            started_at=started_at,
            note=f"Publicados {total_published} eventos en {len(batches)} lotes; {windows_evaluated} ventanas evaluadas",
        )
        conn.commit()

        result = {
            "published_events": total_published,
            "batches_processed": len(batches),
            "windows_evaluated": windows_evaluated,
            "status": "success",
        }
        logger.info("Proceso Gold finalizado exitosamente: %s", result)
        print(json.dumps(result, indent=2))

    except Exception as exc:
        conn.rollback()
        logger.exception("Fallo durante la publicación a Gold: %s", exc)
        try:
            update_pipeline_run(
                conn,
                dag_id="publish_gold",
                run_id=args.dag_run_id,
                state="failed",
                started_at=started_at,
                note=f"Error: {str(exc)[:500]}",
            )
            conn.commit()
        except Exception:
            pass
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
