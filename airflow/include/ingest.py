"""
Módulo de Ingesta para SismoWatch (Fase 2).

Responsabilidades:
  1. Conexión resiliente a USGS con timeout, reintentos y backoff.
  2. Validación estructural de GeoJSON (FeatureCollection).
  3. Cálculo de integridad criptográfica (SHA-256).
  4. Almacenamiento inmutable en MinIO Bronze (sismowatch-bronze).
  5. Registro de trazabilidad y auditoría en PostgreSQL (raw.ingestions)
     usando el rol de mínimo privilegio (sismo_ingest).
  6. Idempotencia garantizada por run_id (UUID v4).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config
import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger("sismowatch.ingest")

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    source: str
    source_url: str
    fetched_at: datetime
    http_status: int
    object_key: str
    bytes: int
    event_count: int
    sha256: str
    status: str  # 'ok' | 'empty' | 'invalid' | 'error'
    error_detail: str | None


def env(name: str, default: str | None = None) -> str:
    """Lee variable de entorno o devuelve valor por defecto, fallando si es obligatoria."""
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return val


def create_resilient_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Crea una sesión HTTP con estrategia de reintentos con backoff exponencial."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_usgs_feed(
    url: str = USGS_FEED_URL,
    timeout: int = 15,
    session: requests.Session | None = None,
) -> tuple[int, bytes]:
    """
    Descarga el feed de USGS retornando (código_http, contenido_bytes).
    Lanza requests.exceptions.RequestException ante errores de red irrecuperables.
    """
    http = session or create_resilient_session()
    logger.info("Consultando feed USGS en %s (timeout=%ds)", url, timeout)
    resp = http.get(url, timeout=timeout)
    return resp.status_code, resp.content


def validate_geojson(payload: bytes) -> tuple[bool, int, str | None]:
    """
    Valida que el contenido sea un FeatureCollection GeoJSON bien formado.
    Retorna (es_valido, cantidad_eventos, detalle_error).
    """
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        return False, 0, f"JSON mal formado o inválido: {type(exc).__name__}: {exc}"

    if not isinstance(data, dict):
        return False, 0, f"GeoJSON debe ser un objeto JSON (dict), no {type(data).__name__}"

    tipo = data.get("type")
    if tipo != "FeatureCollection":
        return False, 0, f"GeoJSON type inválido: esperado 'FeatureCollection', recibido '{tipo}'"

    features = data.get("features")
    if not isinstance(features, list):
        return False, 0, f"GeoJSON features debe ser una lista, recibido {type(features).__name__}"

    return True, len(features), None


def calculate_sha256(data: bytes) -> str:
    """Calcula el hash SHA-256 de los bytes del payload crudo."""
    return hashlib.sha256(data).hexdigest()


def get_minio_client() -> Any:
    """Crea cliente S3 apuntando a MinIO con credenciales del usuario de ingesta."""
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_INGEST_USER"),
        aws_secret_access_key=env("MINIO_INGEST_PASSWORD"),
        region_name=env("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def upload_to_bronze(
    s3_client: Any,
    bucket: str,
    object_key: str,
    data: bytes,
) -> None:
    """Sube el payload GeoJSON inmutable a MinIO en la capa Bronze."""
    logger.info("Subiendo payload crudo a s3://%s/%s (%d bytes)", bucket, object_key, len(data))
    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=data,
        ContentType="application/geo+json",
    )


def get_db_connection() -> psycopg2.extensions.connection:
    """Conecta a PostgreSQL usando el rol de mínimo privilegio para ingesta (sismo_ingest)."""
    return psycopg2.connect(
        host=env("POSTGRES_HOST", "postgres"),
        port=int(env("POSTGRES_PORT", "5432")),
        dbname=env("POSTGRES_DB", "sismowatch"),
        user=env("SISMO_INGEST_USER"),
        password=env("SISMO_INGEST_PASSWORD"),
    )


def record_manifest(conn: psycopg2.extensions.connection, result: IngestionResult) -> None:
    """
    Inserta el manifiesto de extracción en raw.ingestions de forma idempotente por run_id.
    """
    sql = """
    INSERT INTO raw.ingestions (
        run_id, source, source_url, fetched_at, http_status,
        object_key, bytes, event_count, sha256, status, error_detail
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (run_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                result.run_id,
                result.source,
                result.source_url,
                result.fetched_at,
                result.http_status,
                result.object_key,
                result.bytes,
                result.event_count,
                result.sha256,
                result.status,
                result.error_detail,
            ),
        )
    conn.commit()
    logger.info(
        "Manifiesto registrado en raw.ingestions (run_id=%s, status=%s, count=%d)",
        result.run_id,
        result.status,
        result.event_count,
    )


def run_feed_ingestion(
    run_id: str | None = None,
    now: datetime | None = None,
    source_url: str = USGS_FEED_URL,
    s3_client: Any | None = None,
    db_conn: psycopg2.extensions.connection | None = None,
    session: requests.Session | None = None,
) -> IngestionResult:
    """
    Orquesta el flujo completo de ingesta para el feed cada 5 min:
      1. Genera identificador de ejecución único (UUID v4).
      2. Descarga el GeoJSON con reintentos y timeout.
      3. Valida estructura GeoJSON y calcula SHA-256.
      4. Sube a MinIO Bronze inmutable: feed/dt=YYYY-MM-DD/hour=HH/<run_id>.json.
      5. Registra manifiesto en raw.ingestions con rol sismo_ingest.
      6. Si la ingesta falló o es corrupta, registra el estado pero lanza excepción
         para evitar que Airflow emita el Dataset.
    """
    run_id = run_id or str(uuid.uuid4())
    fetched_at = now or datetime.now(timezone.utc)
    dt_str = fetched_at.strftime("%Y-%m-%d")
    hour_str = fetched_at.strftime("%H")
    object_key = f"feed/dt={dt_str}/hour={hour_str}/{run_id}.json"
    bronze_bucket = env("SISMO_BRONZE_BUCKET", "sismowatch-bronze")

    payload_bytes = b""
    http_status = 0
    status = "error"
    error_detail: str | None = None
    event_count = 0
    sha_hash = ""

    # 1. Petición HTTP
    try:
        http_status, payload_bytes = fetch_usgs_feed(
            url=source_url,
            session=session,
        )
        if http_status == 200:
            status = "ok"
        else:
            status = "error"
            error_detail = f"Respuesta HTTP no exitosa: {http_status}"
    except Exception as exc:
        status = "error"
        error_detail = f"Error en conexión HTTP: {type(exc).__name__}: {exc}"
        logger.error("Fallo al obtener feed USGS: %s", error_detail)

    # 2. Si obtuvimos bytes, calculamos hash y validamos estructura
    if payload_bytes:
        sha_hash = calculate_sha256(payload_bytes)
        if status == "ok":
            is_valid, count, val_error = validate_geojson(payload_bytes)
            event_count = count
            if not is_valid:
                status = "invalid"
                error_detail = val_error
            elif count == 0:
                status = "empty"
            else:
                status = "ok"

    # 3. Almacenamiento en MinIO Bronze (incluso si el JSON vino inválido,
    # se almacena para evidencia y auditoría técnica)
    if payload_bytes:
        s3 = s3_client or get_minio_client()
        upload_to_bronze(s3, bronze_bucket, object_key, payload_bytes)

    # 4. Registro en raw.ingestions
    result = IngestionResult(
        run_id=run_id,
        source="feed",
        source_url=source_url,
        fetched_at=fetched_at,
        http_status=http_status,
        object_key=object_key,
        bytes=len(payload_bytes),
        event_count=event_count,
        sha256=sha_hash or ("0" * 64),
        status=status,
        error_detail=error_detail,
    )

    conn = db_conn or get_db_connection()
    try:
        record_manifest(conn, result)
    finally:
        if db_conn is None:
            conn.close()

    # 5. Garantía de calidad: si la ingesta falló o es corrupta, levantamos excepción
    # para que Airflow marque la tarea como fallida y NO emita el Dataset.
    if status in ("error", "invalid"):
        raise RuntimeError(f"Ingesta fallida (status={status}): {error_detail}")

    return result
