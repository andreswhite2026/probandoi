"""
DAG de Backfill Histórico Parametrizado (Fase 5).

Permite reprocesar o cargar rangos históricos desde la API FDSNWS de USGS
(por defecto los últimos 30 días).

Flujo de ejecución end-to-end:
  1. Ingesta Bronze: descarga el GeoJSON histórico, calcula SHA-256, almacena
     en sismowatch-bronze (backfill/dt=YYYY-MM-DD/<run_id>.json) y registra en raw.ingestions.
  2. Transformación Spark: tipa datos, aplica reglas declarativas R01-R05,
     cuarentena registros inválidos, genera Silver Parquet y hace upsert en staging.earthquakes.
  3. Publicación Gold: valida Quality Gate, hace upsert en curated.earthquakes,
     recalcula anomalías Z-Score en curated.anomalies y emite el Dataset 'sismowatch://gold/earthquakes'.

Idempotencia garantizada: Ejecutar dos veces el mismo rango temporal produce exactamente
el mismo conteo final de registros sin duplicados.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from include.ingest import run_backfill_ingestion

logger = logging.getLogger("airflow.task")

GOLD_DATASET = Dataset("sismowatch://gold/earthquakes")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def execute_ingest_backfill(**context: Any) -> str:
    """Extrae parámetros de dag_run.conf y ejecuta la ingesta histórica a Bronze."""
    conf = context.get("dag_run").conf or {}
    params = context.get("params") or {}

    # Prioridad: dag_run.conf > params > defaults
    start_date_str = conf.get("start_date") or params.get("start_date")
    end_date_str = conf.get("end_date") or params.get("end_date")
    min_mag = conf.get("min_magnitude") or params.get("min_magnitude")

    now_utc = datetime.now(timezone.utc)

    if not end_date_str:
        end_date = now_utc
    elif isinstance(end_date_str, str):
        end_date = datetime.strptime(end_date_str.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = end_date_str

    if not start_date_str:
        days = int(conf.get("days") or params.get("days") or 30)
        start_date = end_date - timedelta(days=days)
    elif isinstance(start_date_str, str):
        start_date = datetime.strptime(start_date_str.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_date = start_date_str

    logger.info(
        "Iniciando Backfill histórico: desde %s hasta %s (min_mag=%s)",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        min_mag,
    )

    result = run_backfill_ingestion(
        start_date=start_date,
        end_date=end_date,
        min_magnitude=min_mag,
    )

    logger.info("Backfill extraído con éxito: run_id=%s, eventos=%d", result.run_id, result.event_count)
    return result.run_id


with DAG(
    dag_id="backfill_history",
    default_args=default_args,
    description="Backfill histórico parametrizado de USGS (30 días por defecto) con procesamiento Medallion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "usgs", "medallion", "idempotent"],
    params={
        "days": Param(30, type="integer", description="Número de días hacia atrás si no se indican fechas exactas"),
        "start_date": Param("", type="string", description="Fecha de inicio YYYY-MM-DD (opcional)"),
        "end_date": Param("", type="string", description="Fecha de fin YYYY-MM-DD (opcional)"),
        "min_magnitude": Param(None, type=["null", "number"], description="Magnitud mínima opcional (ej: 2.5)"),
    },
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_backfill",
        python_callable=execute_ingest_backfill,
    )

    transform_task = BashOperator(
        task_id="transform_backfill",
        bash_command=(
            "spark-submit "
            "--master 'local[*]' "
            "--driver-memory 1g "
            "/opt/airflow/spark/jobs/transform_silver.py "
            "--run-id '{{ ti.xcom_pull(task_ids=\"ingest_backfill\") }}' "
            "--dag-run-id '{{ run_id }}'"
        ),
    )

    publish_task = BashOperator(
        task_id="publish_backfill",
        bash_command=(
            "python3 /opt/airflow/spark/jobs/publish_gold.py "
            "--run-id '{{ ti.xcom_pull(task_ids=\"ingest_backfill\") }}' "
            "--dag-run-id '{{ run_id }}'"
        ),
        outlets=[GOLD_DATASET],
    )

    ingest_task >> transform_task >> publish_task
