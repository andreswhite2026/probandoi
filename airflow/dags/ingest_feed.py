"""
DAG de Ingesta Recurrente de USGS (Fase 2).

Programa la extracción automática cada 5 minutos desde el feed de USGS,
guarda el payload original en MinIO Bronze, registra el manifiesto en
raw.ingestions y emite el Dataset 'sismowatch://bronze/feed' únicamente
cuando la ingesta es válida y exitosa.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

# Asegurar importabilidad de include/ tanto en contenedor como en local
AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from include.ingest import run_feed_ingestion

logger = logging.getLogger("airflow.task")

# Dataset de coordinación según arquitectura Airflow 2.10
BRONZE_FEED_DATASET = Dataset("sismowatch://bronze/feed")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def _execute_ingest(**context: object) -> dict[str, object]:
    """Tarea que ejecuta la ingesta y expone los metadatos a XCom."""
    logger.info("Iniciando tarea de ingesta periódica de USGS")
    result = run_feed_ingestion()
    logger.info(
        "Ingesta completada exitosamente: run_id=%s, count=%d, object=%s",
        result.run_id,
        result.event_count,
        result.object_key,
    )
    return {
        "run_id": result.run_id,
        "object_key": result.object_key,
        "bytes": result.bytes,
        "event_count": result.event_count,
        "sha256": result.sha256,
        "status": result.status,
    }


with DAG(
    dag_id="ingest_feed",
    default_args=default_args,
    description="Ingesta periódica cada 5 min del feed sísmico USGS hacia Bronze",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "usgs"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_usgs_all_hour",
        python_callable=_execute_ingest,
        outlets=[BRONZE_FEED_DATASET],
    )
