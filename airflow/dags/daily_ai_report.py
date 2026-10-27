"""
DAG de Reporte Diario Sísmico con Inteligencia Artificial (Fase 6).

Programa la generación diaria a la 01:00 UTC (o bajo demanda vía manual/API)
del reporte analítico basado en los eventos y anomalías de la capa Gold.

Resiliencia:
  - Invoca al modelo local Ollama con los indicadores deterministas calculados.
  - Si el proveedor no responde o presenta discrepancias numéricas, se activa
    automáticamente el fallback determinista garantizando que el DAG termine en éxito
    y con un reporte fidedigno en curated.ai_reports (status='fallback').
  - Audita cada ejecución en curated.ai_runs.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

logger = logging.getLogger("airflow.task")

REPORTS_DATASET = Dataset("sismowatch://gold/reports")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_ai_report",
    default_args=default_args,
    description="Generación diaria de reportes sísmicos con IA (Ollama) y fallback determinista",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "ai", "ollama", "resilience", "reports"],
    params={
        "date": Param("", type="string", description="Fecha YYYY-MM-DD del reporte (por defecto hoy UTC)"),
        "force_fallback": Param(False, type="boolean", description="Forzar modo fallback sin consultar al LLM"),
    },
) as dag:

    generate_report_task = BashOperator(
        task_id="generate_ai_report",
        bash_command=(
            "python3 /opt/airflow/spark/jobs/generate_daily_report.py "
            "{% if params.date %}--date '{{ params.date }}'{% endif %} "
            "{% if params.force_fallback %}--force-fallback{% endif %}"
        ),
        outlets=[REPORTS_DATASET],
    )
