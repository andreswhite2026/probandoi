#!/usr/bin/env python3
"""
Job de Procesamiento Spark: Bronze -> Silver (Fase 3).

Responsabilidades:
  1. Identificar e ingerir lotes pendientes de raw.ingestions por run_id.
  2. Leer payload crudo desde MinIO Bronze (s3a://sismowatch-bronze/...).
  3. Parsear GeoJSON (coordenadas [lon, lat, depth], tiempos UTC, derivar region).
  4. Aplicar reglas declarativas de calidad (R01-R05).
  5. Cuarentena de registros inválidos con motivos en curated.quarantine.
  6. Registro de métricas de calidad en curated.quality_runs y curated.quality_rules.
  7. Publicación en MinIO Silver (Parquet particionado por dt).
  8. Upsert idempotente en PostgreSQL (staging.earthquakes) respetando versionado updated_utc.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config
import psycopg2
from psycopg2.extras import Json, execute_values
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Permitir importación de transform helpers
sys.path.insert(0, "/opt/airflow")
from include.transform import (
    derive_region,
    evaluate_quality_rules,
    parse_feature,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sismowatch.silver")


def env(name: str, default: str | None = None) -> str:
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


def get_minio_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env("AWS_SECRET_ACCESS_KEY"),
        region_name=env("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sismowatch-bronze-to-silver")
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", env("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", env("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.aws.region", env("AWS_REGION", "us-east-1"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def find_pending_batch(conn: psycopg2.extensions.connection, target_run_id: str | None = None) -> dict[str, Any] | None:
    """Busca el lote pendiente más antiguo o el run_id especificado."""
    with conn.cursor() as cur:
        if target_run_id:
            cur.execute(
                """
                SELECT run_id, object_key, event_count, sha256, status
                FROM raw.ingestions
                WHERE run_id = %s;
                """,
                (target_run_id,),
            )
        else:
            cur.execute(
                """
                SELECT run_id, object_key, event_count, sha256, status
                FROM raw.ingestions
                WHERE status IN ('ok', 'empty')
                  AND run_id NOT IN (
                      SELECT run_id FROM curated.quality_runs
                      WHERE layer = 'bronze->silver' AND status = 'passed'
                  )
                ORDER BY fetched_at ASC
                LIMIT 1;
                """
            )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "run_id": str(row[0]),
            "object_key": row[1],
            "event_count": row[2] or 0,
            "sha256": row[3],
            "status": row[4],
        }


def process_batch(
    batch: dict[str, Any],
    conn: psycopg2.extensions.connection,
    spark: SparkSession,
    dag_run_id: str | None = None,
) -> dict[str, Any]:
    run_id = batch["run_id"]
    object_key = batch["object_key"]
    bronze_bucket = env("SISMO_BRONZE_BUCKET", "sismowatch-bronze")
    silver_bucket = env("SISMO_SILVER_BUCKET", "sismowatch-silver")

    started_at = datetime.now(timezone.utc)
    logger.info("Iniciando procesamiento Silver para lote %s (%s)", run_id, object_key)

    # 1. Descarga del payload crudo desde MinIO Bronze
    s3 = get_minio_client()
    try:
        obj = s3.get_object(Bucket=bronze_bucket, Key=object_key)
        raw_bytes = obj["Body"].read()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        logger.error("Error al leer objeto de MinIO Bronze: %s", exc)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM curated.quality_runs WHERE run_id = %s;", (run_id,))
            cur.execute(
                """
                INSERT INTO curated.quality_runs (
                    run_id, dag_run_id, layer, evaluated, approved, rejected, score, status, started_at, finished_at
                ) VALUES (%s, %s, 'bronze->silver', 0, 0, 0, 0.0, 'failed', %s, %s);
                """,
                (run_id, dag_run_id, started_at, datetime.now(timezone.utc)),
            )
        conn.commit()
        raise RuntimeError(f"Fallo crítico al leer Bronze: {exc}")

    features = payload.get("features", [])
    evaluated_count = len(features)
    valid_records: list[dict[str, Any]] = []
    quarantine_records: list[dict[str, Any]] = []

    # Métricas por regla
    rule_metrics = {
        "R01_ID_REQUIRED": {"description": "Identificador requerido no vacío", "failed": 0},
        "R02_TIME_VALID": {"description": "Fecha válida y no futura", "failed": 0},
        "R03_COORDS_RANGE": {"description": "Coordenadas en rango geodésico [lon, lat, depth]", "failed": 0},
        "R04_MAG_RANGE": {"description": "Magnitud entre -2.0 y 10.5", "failed": 0},
        "R05_EVENT_TYPE": {"description": "Tipo de evento en catálogo USGS", "failed": 0},
    }

    # 2. Evaluación de reglas de calidad
    now_utc = datetime.now(timezone.utc)
    for feat in features:
        rec = parse_feature(feat, ingest_run_id=run_id)
        is_valid, failures = evaluate_quality_rules(rec, now=now_utc)
        if is_valid:
            valid_records.append(rec)
        else:
            for rule_code, _ in failures:
                if rule_code in rule_metrics:
                    rule_metrics[rule_code]["failed"] += 1
            reason_code = ",".join(c for c, _ in failures)
            reason_detail = "; ".join(f"{c}: {d}" for c, d in failures)
            quarantine_records.append({
                "run_id": run_id,
                "event_id": rec["id"] or None,
                "raw_object_key": object_key,
                "payload": feat,
                "reason_code": reason_code,
                "reason_detail": reason_detail,
            })

    approved_count = len(valid_records)
    rejected_count = len(quarantine_records)
    score = round((approved_count / evaluated_count) * 100.0, 2) if evaluated_count > 0 else 100.0
    status = "passed" if (rejected_count == 0 or approved_count > 0) else "failed"

    # 3. Operaciones atómicas en PostgreSQL (idempotencia)
    with conn.cursor() as cur:
        # a) Limpieza previa para reintentos seguros
        cur.execute("DELETE FROM curated.quarantine WHERE run_id = %s;", (run_id,))
        cur.execute("DELETE FROM curated.quality_rules WHERE run_id = %s;", (run_id,))
        cur.execute("DELETE FROM curated.quality_runs WHERE run_id = %s;", (run_id,))

        # b) Inserción de quality_runs
        cur.execute(
            """
            INSERT INTO curated.quality_runs (
                run_id, dag_run_id, layer, evaluated, approved, rejected, duplicates, score, status, started_at, finished_at
            ) VALUES (%s, %s, 'bronze->silver', %s, %s, %s, 0, %s, %s, %s, %s);
            """,
            (run_id, dag_run_id, evaluated_count, approved_count, rejected_count, score, status, started_at, datetime.now(timezone.utc)),
        )

        # c) Inserción de quality_rules
        for rcode, rmeta in rule_metrics.items():
            failed_count = rmeta["failed"]
            cur.execute(
                """
                INSERT INTO curated.quality_rules (
                    run_id, rule_code, description, severity, evaluated, failed, passed
                ) VALUES (%s, %s, %s, 'blocking', %s, %s, %s);
                """,
                (run_id, rcode, rmeta["description"], evaluated_count, failed_count, failed_count == 0),
            )

        # d) Inserción de cuarentena
        if quarantine_records:
            q_values = [
                (q["run_id"], q["event_id"], q["raw_object_key"], Json(q["payload"]), q["reason_code"], q["reason_detail"])
                for q in quarantine_records
            ]
            execute_values(
                cur,
                """
                INSERT INTO curated.quarantine (
                    run_id, event_id, raw_object_key, payload, reason_code, reason_detail
                ) VALUES %s;
                """,
                q_values,
            )

        # e) Upsert en staging.earthquakes
        if valid_records:
            staging_values = [
                (
                    r["id"],
                    r["mag"],
                    r["mag_type"],
                    r["place"],
                    r["region"],
                    r["time_utc"],
                    r["updated_utc"],
                    r["tsunami"],
                    r["sig"],
                    r["event_status"],
                    r["event_type"],
                    r["url"],
                    r["longitude"],
                    r["latitude"],
                    r["depth_km"],
                    r["ingest_run_id"],
                )
                for r in valid_records
            ]
            execute_values(
                cur,
                """
                INSERT INTO staging.earthquakes (
                    id, mag, mag_type, place, region, time_utc, updated_utc,
                    tsunami, sig, event_status, event_type, url,
                    longitude, latitude, depth_km, ingest_run_id
                ) VALUES %s
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
                    last_seen_at = now()
                WHERE staging.earthquakes.updated_utc IS NULL
                   OR EXCLUDED.updated_utc IS NULL
                   OR EXCLUDED.updated_utc >= staging.earthquakes.updated_utc;
                """,
                staging_values,
            )

    conn.commit()

    # 4. Escritura en MinIO Silver con Spark (Parquet particionado por dt)
    if valid_records:
        spark_rows = []
        for r in valid_records:
            dt_str = r["time_utc"].strftime("%Y-%m-%d") if r["time_utc"] else "unknown"
            spark_rows.append({
                "id": r["id"],
                "mag": float(r["mag"]) if r["mag"] is not None else None,
                "mag_type": r["mag_type"],
                "place": r["place"],
                "region": r["region"],
                "time_utc": r["time_utc"],
                "updated_utc": r["updated_utc"],
                "tsunami": r["tsunami"],
                "sig": r["sig"],
                "event_status": r["event_status"],
                "event_type": r["event_type"],
                "url": r["url"],
                "longitude": float(r["longitude"]) if r["longitude"] is not None else None,
                "latitude": float(r["latitude"]) if r["latitude"] is not None else None,
                "depth_km": float(r["depth_km"]) if r["depth_km"] is not None else None,
                "ingest_run_id": r["ingest_run_id"],
                "dt": dt_str,
            })

        df = spark.createDataFrame(spark_rows)
        silver_path = f"s3a://{silver_bucket}/earthquakes"
        logger.info("Guardando %d filas aprobadas en Silver Parquet (%s)", len(spark_rows), silver_path)
        df.write.mode("append").partitionBy("dt").parquet(silver_path)

    summary = {
        "run_id": run_id,
        "evaluated": evaluated_count,
        "approved": approved_count,
        "rejected": rejected_count,
        "score": score,
        "status": status,
    }
    logger.info("Lote procesado exitosamente: %s", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver con Calidad")
    parser.add_argument("--run-id", default=None, help="ID específico del lote a procesar")
    parser.add_argument("--dag-run-id", default=None, help="DAG run ID de Airflow")
    args = parser.parse_args()

    conn = get_db_connection()
    spark = None
    try:
        batch = find_pending_batch(conn, target_run_id=args.run_id)
        if not batch:
            logger.info("No hay lotes pendientes en raw.ingestions.")
            print(json.dumps({"status": "no_pending_batches"}))
            return 0

        spark = build_spark()
        summary = process_batch(batch, conn, spark, dag_run_id=args.dag_run_id)
        print(json.dumps(summary, indent=2))
        return 0 if summary["status"] == "passed" else 1

    except Exception as exc:
        logger.error("Error no recuperable en transform_silver: %s", exc)
        print(json.dumps({"status": "failed", "error": str(exc)}))
        return 1

    finally:
        conn.close()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
