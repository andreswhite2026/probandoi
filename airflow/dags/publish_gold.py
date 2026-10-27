"""
DAG de Publicación Gold y Detección de Anomalías disparado por Dataset (Fase 4).

Se activa automáticamente cada vez que el job de transformación Silver
('transform_silver') emite el Dataset 'sismowatch://silver/earthquakes'.
Ejecuta la validación de la compuerta de calidad, publica los eventos aprobados
en 'curated.earthquakes', calcula las anomalías estadísticas Z-score por región
y hora en 'curated.anomalies', y emite el Dataset 'sismowatch://gold/earthquakes'.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

logger = logging.getLogger("airflow.task")

SILVER_DATASET = Dataset("sismowatch://silver/earthquakes")
GOLD_DATASET = Dataset("sismowatch://gold/earthquakes")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="publish_gold",
    default_args=default_args,
    description="Publicación a capa Gold (curated) y detección estadística de anomalías",
    schedule=[SILVER_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "curated", "anomalies", "zscore"],
) as dag:

    publish_gold_task = BashOperator(
        task_id="publish_curated_gold",
        bash_command=(
            "python3 /opt/airflow/spark/jobs/publish_gold.py "
            "--dag-run-id '{{ run_id }}'"
        ),
        outlets=[GOLD_DATASET],
    )
