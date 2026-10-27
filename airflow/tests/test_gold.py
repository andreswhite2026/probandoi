"""
Pruebas unitarias para la capa Gold (Fase 4):
  - Compuerta de calidad (check_quality_gate).
  - Cálculo de Z-Score y detección de anomalías (calculate_zscore).
  - Transformación por ventanas horarias (compute_anomalies_for_windows).
  - Integridad del DAG publish_gold y disparadores de Dataset.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from include.gold import (
    DEFAULT_MIN_SCORE,
    DEFAULT_ZSCORE_THRESHOLD,
    calculate_zscore,
    check_quality_gate,
    compute_anomalies_for_windows,
)


def test_check_quality_gate_passed():
    run = {
        "status": "passed",
        "score": 100.0,
        "approved": 5,
        "rejected": 0,
    }
    ok, msg = check_quality_gate(run)
    assert ok is True
    assert "Compuerta superada" in msg


def test_check_quality_gate_none():
    ok, msg = check_quality_gate(None)
    assert ok is False
    assert "No existe" in msg


def test_check_quality_gate_failed_status():
    run = {
        "status": "failed",
        "score": 95.0,
        "approved": 20,
    }
    ok, msg = check_quality_gate(run)
    assert ok is False
    assert "no aprobado" in msg


def test_check_quality_gate_low_score():
    run = {
        "status": "passed",
        "score": 65.0,
        "approved": 10,
    }
    ok, msg = check_quality_gate(run, min_score=80.0)
    assert ok is False
    assert "inferior al umbral" in msg


def test_check_quality_gate_zero_approved():
    run = {
        "status": "passed",
        "score": 100.0,
        "approved": 0,
    }
    ok, msg = check_quality_gate(run)
    assert ok is False
    assert "no contiene registros aprobados" in msg


def test_calculate_zscore_normal_non_anomaly():
    z, is_anom, exp = calculate_zscore(value=3.0, mean=2.0, std=1.0, threshold=2.5)
    assert z == 1.0
    assert is_anom is False
    assert exp is None


def test_calculate_zscore_zero_std():
    z, is_anom, exp = calculate_zscore(value=10.0, mean=10.0, std=0.0)
    assert z == 0.0
    assert is_anom is False
    assert exp is None


def test_calculate_zscore_nan_or_negative_std():
    z, is_anom, exp = calculate_zscore(value=5.0, mean=2.0, std=float("nan"))
    assert z == 0.0
    assert is_anom is False

    z, is_anom, exp = calculate_zscore(value=5.0, mean=2.0, std=-1.5)
    assert z == 0.0
    assert is_anom is False


def test_calculate_zscore_anomaly_triggered():
    # value=15, mean=2, std=2 -> z = (15-2)/2 = 6.5 >= 2.5
    z, is_anom, exp = calculate_zscore(value=15.0, mean=2.0, std=2.0, threshold=2.5)
    assert z == 6.5
    assert is_anom is True
    assert exp is not None
    assert "Actividad inusual" in exp
    assert "z-score=6.5" in exp


def test_compute_anomalies_for_windows():
    now = datetime(2026, 10, 27, 14, 0, tzinfo=timezone.utc)
    windows = [
        {"window_start": now, "region": "California", "event_count": 2},
        {"window_start": now, "region": "Alaska", "event_count": 25},
    ]
    baselines = {
        "California": (2.0, 0.5),
        "Alaska": (3.0, 2.0),
    }

    results = compute_anomalies_for_windows(windows, baselines, threshold=2.5)
    assert len(results) == 2

    # California: (2-2)/0.5 = 0.0 -> no anomalía
    cal = [r for r in results if r["region"] == "California"][0]
    assert cal["zscore"] == 0.0
    assert cal["is_anomaly"] is False

    # Alaska: (25-3)/2.0 = 11.0 -> anomalía
    ak = [r for r in results if r["region"] == "Alaska"][0]
    assert ak["zscore"] == 11.0
    assert ak["is_anomaly"] is True
    assert "Actividad inusual" in ak["explanation"]


def test_dag_publish_gold_integrity():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Errores de importación: {dagbag.import_errors}"

    dag = dagbag.get_dag("publish_gold")
    assert dag is not None

    timetable = getattr(dag, "timetable", None)
    assert timetable is not None
    assert "Dataset" in type(timetable).__name__
    cond_str = str(getattr(timetable, "dataset_condition", "")) or str(getattr(timetable, "_datasets", "")) or str(timetable)
    assert "sismowatch://silver/earthquakes" in cond_str or "DatasetTriggeredTimetable" in type(timetable).__name__

    task = dag.get_task("publish_curated_gold")
    assert task is not None
    assert any("sismowatch://gold/earthquakes" in str(outlet) for outlet in (task.outlets or []))
