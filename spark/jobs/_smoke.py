#!/usr/bin/env python3
"""
Smoke test de infraestructura (Fase 1).

Objetivo: demostrar que Spark 3.5.1 puede escribir y leer en las tres fronteras
del sistema ANTES de construir el pipeline:

    1. S3A  -> MinIO        (escribe un Parquet en el bucket Silver)
    2. JDBC -> PostgreSQL   (inserta filas de prueba en staging.earthquakes)

Reglas de seguridad del test:
    * Todo lo que crea lleva un `run_id` único en la ruta y en el `id`.
    * La limpieza borra EXCLUSIVAMENTE lo creado por esta ejecución.
    * No se toca ninguna ruta preexistente ni se hace DROP/TRUNCATE.

Salida: JSON con el resultado de cada comprobación. Código de salida != 0 si
algo falla, para que Airflow/Make marque la tarea como fallida.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import SparkSession

SMOKE_ID_PREFIX = "smoke-"


def env(name: str) -> str:
    """Lee una variable obligatoria y falla con un mensaje claro si no existe."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno obligatoria: {name}")
    return value


def build_spark() -> SparkSession:
    """Sesión Spark con el cliente S3A apuntando a MinIO."""
    return (
        SparkSession.builder.appName("sismowatch-smoke")
        # path.style.access=true es obligatorio en MinIO: evita el estilo
        # virtual-host (bucket.endpoint), que MinIO no resuelve.
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", env("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", env("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.aws.region", env("AWS_REGION"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def jdbc_url() -> str:
    """URL JDBC. stringtype=unspecified permite enviar texto a columnas uuid."""
    return (
        f"jdbc:postgresql://{env('SISMO_DB_HOST')}:{env('SISMO_DB_PORT')}"
        f"/{env('SISMO_DB_NAME')}?stringtype=unspecified"
    )


def jdbc_props() -> dict[str, str]:
    return {
        "user": env("SISMO_TRANSFORM_USER"),
        "password": env("SISMO_TRANSFORM_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }


def delete_spark_path(spark: SparkSession, path: str) -> bool:
    """Borra recursivamente SOLO la ruta indicada (la de este run_id)."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hpath.toUri(), spark._jsc.hadoopConfiguration())
    return bool(fs.delete(hpath, True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test MinIO + PostgreSQL con Spark")
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    args = parser.parse_args()
    run_id = args.run_id

    silver_bucket = env("SISMO_SILVER_BUCKET")
    parquet_path = f"s3a://{silver_bucket}/_smoke/run_id={run_id}"
    row_ids = [f"{SMOKE_ID_PREFIX}{run_id}-a", f"{SMOKE_ID_PREFIX}{run_id}-b"]
    now = datetime.now(timezone.utc)

    checks: dict[str, object] = {"run_id": run_id, "parquet_path": parquet_path}
    spark = None

    try:
        spark = build_spark()

        # ---------------- 1. Escritura en MinIO (S3A) ----------------------
        rows = [
            {
                "id": row_ids[0],
                "mag": 4.5,
                "mag_type": "ml",
                "place": "smoke test MinIO",
                "region": "smoke",
                "time_utc": now,
                "updated_utc": now,
                "tsunami": False,
                "sig": 1,
                "event_status": "reviewed",
                "event_type": "earthquake",
                "url": "https://example.invalid/smoke-a",
                "longitude": 0.0,
                "latitude": 0.0,
                "depth_km": 10.0,
                "ingest_run_id": run_id,
                "first_seen_at": now,
                "last_seen_at": now,
            },
            {
                "id": row_ids[1],
                "mag": 5.1,
                "mag_type": "mb",
                "place": "smoke test PostgreSQL",
                "region": "smoke",
                "time_utc": now,
                "updated_utc": now,
                "tsunami": True,
                "sig": 2,
                "event_status": "automatic",
                "event_type": "earthquake",
                "url": "https://example.invalid/smoke-b",
                "longitude": 1.0,
                "latitude": 1.0,
                "depth_km": 20.0,
                "ingest_run_id": run_id,
                "first_seen_at": now,
                "last_seen_at": now,
            },
        ]
        df = spark.createDataFrame(rows)

        # errorifexists: si la ruta existiera, el test falla en vez de pisar datos.
        df.write.mode("errorifexists").parquet(parquet_path)
        checks["parquet_written"] = True

        read_back = spark.read.parquet(parquet_path)
        checks["parquet_rows_read"] = read_back.count()
        checks["parquet_columns"] = len(read_back.columns)
        if checks["parquet_rows_read"] != len(rows):
            raise AssertionError("El Parquet leído no tiene el número de filas escrito")

        # ---------------- 2. Escritura en PostgreSQL (JDBC) ----------------
        df.write.jdbc(url=jdbc_url(), table="staging.earthquakes", mode="append", properties=jdbc_props())
        checks["jdbc_rows_written"] = len(rows)

        # ---------------- 3. Verificación independiente --------------------
        with psycopg2.connect(
            host=env("SISMO_DB_HOST"),
            port=int(env("SISMO_DB_PORT")),
            dbname=env("SISMO_DB_NAME"),
            user=env("SISMO_TRANSFORM_USER"),
            password=env("SISMO_TRANSFORM_PASSWORD"),
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM staging.earthquakes WHERE id = ANY(%s)",
                (row_ids,),
            )
            checks["jdbc_rows_in_db"] = cur.fetchone()[0]

        if checks["jdbc_rows_in_db"] != len(rows):
            raise AssertionError("PostgreSQL no contiene las filas del smoke test")

        # Lectura de vuelta con el driver JDBC de Spark (no solo psycopg2)
        df_db = spark.read.jdbc(url=jdbc_url(), table="staging.earthquakes", properties=jdbc_props())
        checks["jdbc_read_via_spark"] = df_db.filter(df_db.id.isin(row_ids)).count()
        if checks["jdbc_read_via_spark"] != len(rows):
            raise AssertionError("Spark no puede leer de vuelta lo escrito por JDBC")

        checks["status"] = "ok"
        return 0

    except Exception as exc:  # frontera de ejecución: se registra y se propaga
        checks["status"] = "failed"
        checks["error"] = f"{type(exc).__name__}: {exc}"
        print(json.dumps(checks, indent=2, default=str))
        return 1

    finally:
        # ---------------- 4. Limpieza acotada ------------------------------
        # Solo se borra el prefijo y los ids de ESTE run_id. La limpieza se
        # intenta siempre (aunque la escritura haya fallado a medias) y sus
        # errores se reportan aparte para no ocultar el error original.
        cleaned = {"parquet_deleted": False, "rows_deleted": 0}
        if spark is not None:
            try:
                cleaned["parquet_deleted"] = delete_spark_path(spark, parquet_path)
            except Exception as exc:  # noqa: BLE001 - se reporta, no se propaga
                cleaned["parquet_cleanup_error"] = f"{type(exc).__name__}: {exc}"
        try:
            with psycopg2.connect(
                host=env("SISMO_DB_HOST"),
                port=int(env("SISMO_DB_PORT")),
                dbname=env("SISMO_DB_NAME"),
                user=env("SISMO_TRANSFORM_USER"),
                password=env("SISMO_TRANSFORM_PASSWORD"),
            ) as conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM staging.earthquakes WHERE id = ANY(%s)",
                    (row_ids,),
                )
                cleaned["rows_deleted"] = cur.rowcount
        except Exception as exc:  # noqa: BLE001 - se reporta, no se propaga
            cleaned["rows_cleanup_error"] = f"{type(exc).__name__}: {exc}"

        checks["cleanup"] = cleaned
        if spark is not None:
            spark.stop()
        print(json.dumps(checks, indent=2, default=str))

        # Si el test pasó, se exige que la limpieza también haya funcionado:
        # de lo contrario quedarían residuos que ensucian las capas.
        if checks.get("status") == "ok" and not (
            cleaned["parquet_deleted"] and cleaned["rows_deleted"] == len(row_ids)
        ):
            print("ADVERTENCIA: limpieza incompleta, revisar residuos con prefijo smoke-", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
