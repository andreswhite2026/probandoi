"""
DAG de Transformación Bronze -> Silver disparado por Dataset (Fase 3).

Se activa automáticamente cada vez que la ingesta periódica ('ingest_feed')
emite el Dataset 'sismowatch://bronze/feed'. Ejecuta el job de Spark
para tipar, validar con reglas de calidad bloqueantes, enviar registros
inválidos a cuarentena, publicar Parquet en MinIO Silver y hacer upsert
en PostgreSQL (staging.earthquakes). Al finalizar, emite el Dataset
'sismowatch://silver/earthquakes'.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

logger = logging.getLogger("airflow.task")

BRONZE_FEED_DATASET = Dataset("sismowatch://bronze/feed")
SILVER_DATASET = Dataset("sismowatch://silver/earthquakes")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="transform_silver",
    default_args=default_args,
    description="Transformación Bronze a Silver con Spark y calidad bloqueante",
    schedule=[BRONZE_FEED_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "spark", "quality", "quarantine"],
) as dag:

    transform_task = BashOperator(
        task_id="spark_transform_silver",
        bash_command=(
            "spark-submit "
            "--master 'local[*]' "
            "--driver-memory 1g "
            "/opt/airflow/spark/jobs/transform_silver.py "
            "--dag-run-id '{{ run_id }}'"
        ),
        outlets=[SILVER_DATASET],
    )
