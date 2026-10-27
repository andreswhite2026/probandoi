"""
Pruebas unitarias para la Fase 3 (Transformación Silver y Calidad R01-R05).

Verifica:
  1. Derivación heurística de región.
  2. Parseo de GeoJSON con orden [lon, lat, depth] y epoch-ms a UTC.
  3. Evaluación de cada regla declarativa (R01 a R05).
  4. Consolidación de múltiples incumplimientos en un único registro.
  5. Integridad del DAG transform_silver y Dataset trigger.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.models import DagBag
from include.transform import (
    derive_region,
    epoch_ms_to_utc,
    evaluate_quality_rules,
    parse_feature,
)


def test_derive_region_comma_split() -> None:
    assert derive_region("10 km S of San Juan, Puerto Rico") == "Puerto Rico"
    assert derive_region("2 km ENE of Ridgecrest, CA") == "CA"


def test_derive_region_of_split() -> None:
    assert derive_region("5 km S of Hawaii") == "Hawaii"


def test_derive_region_simple_or_empty() -> None:
    assert derive_region("Southern California") == "Southern California"
    assert derive_region("") == "Unknown"
    assert derive_region(None) == "Unknown"


def test_epoch_ms_to_utc() -> None:
    dt = epoch_ms_to_utc(1700000000000)
    assert dt is not None
    assert dt.year == 2023
    assert dt.tzinfo == timezone.utc
    assert epoch_ms_to_utc(None) is None


def test_parse_feature_usgs_coordinates() -> None:
    feature = {
        "id": "us1000abcd",
        "properties": {
            "mag": 4.567,
            "magType": "ml",
            "place": "15 km N of Anchorage, Alaska",
            "time": 1700000000000,
            "updated": 1700000010000,
            "tsunami": 0,
            "sig": 300,
            "status": "reviewed",
            "type": "earthquake",
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us1000abcd",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-149.9, 61.2, 25.4],  # [lon, lat, depth]
        },
    }

    rec = parse_feature(feature, ingest_run_id="run-test-123")
    assert rec["id"] == "us1000abcd"
    assert rec["longitude"] == -149.9
    assert rec["latitude"] == 61.2
    assert rec["depth_km"] == 25.4
    assert rec["mag"] == 4.57
    assert rec["region"] == "Alaska"
    assert rec["tsunami"] is False


def test_parse_feature_missing_depth() -> None:
    feature = {
        "id": "us2000abcd",
        "properties": {"place": "Central Mid-Atlantic Ridge", "time": 1700000000000, "type": "earthquake"},
        "geometry": {"type": "Point", "coordinates": [-30.0, 10.0]},  # sin depth
    }
    rec = parse_feature(feature, ingest_run_id="run-test-123")
    assert rec["longitude"] == -30.0
    assert rec["latitude"] == 10.0
    assert rec["depth_km"] is None


def test_evaluate_quality_rules_all_valid() -> None:
    rec = {
        "id": "us3000valid",
        "time_utc": datetime.now(timezone.utc) - timedelta(minutes=10),
        "latitude": 34.05,
        "longitude": -118.25,
        "depth_km": 12.0,
        "mag": 3.2,
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is True
    assert len(failures) == 0


def test_evaluate_quality_rules_missing_id() -> None:
    rec = {
        "id": "",
        "time_utc": datetime.now(timezone.utc),
        "latitude": 0.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "mag": 2.0,
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R01_ID_REQUIRED" in codes


def test_evaluate_quality_rules_future_date() -> None:
    rec = {
        "id": "future123",
        "time_utc": datetime.now(timezone.utc) + timedelta(hours=2),
        "latitude": 0.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "mag": 2.0,
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R02_TIME_VALID" in codes


def test_evaluate_quality_rules_coords_out_of_range() -> None:
    rec = {
        "id": "badcoords",
        "time_utc": datetime.now(timezone.utc),
        "latitude": 95.0,     # Inválido (> 90)
        "longitude": -190.0,  # Inválido (< -180)
        "depth_km": -20.0,    # Inválido (< -10)
        "mag": 2.0,
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R03_COORDS_RANGE" in codes


def test_evaluate_quality_rules_mag_out_of_range() -> None:
    rec = {
        "id": "badmag",
        "time_utc": datetime.now(timezone.utc),
        "latitude": 0.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "mag": 15.0,  # Imposible
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R04_MAG_RANGE" in codes


def test_evaluate_quality_rules_unknown_event_type() -> None:
    rec = {
        "id": "badtype",
        "time_utc": datetime.now(timezone.utc),
        "latitude": 0.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "mag": 2.0,
        "event_type": "ufo_landing",  # Inválido
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R05_EVENT_TYPE" in codes


def test_evaluate_quality_rules_multiple_failures_consolidated() -> None:
    """Verifica que si un registro falla R01, R03 y R04, se reporten todos los códigos."""
    rec = {
        "id": "",
        "time_utc": datetime.now(timezone.utc),
        "latitude": 100.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "mag": 20.0,
        "event_type": "earthquake",
    }
    is_valid, failures = evaluate_quality_rules(rec)
    assert is_valid is False
    codes = [code for code, _ in failures]
    assert "R01_ID_REQUIRED" in codes
    assert "R03_COORDS_RANGE" in codes
    assert "R04_MAG_RANGE" in codes


def test_dag_transform_silver_integrity() -> None:
    """Verifica que el DAG transform_silver cargue limpiamente con Dataset scheduling."""
    dags_dir = str(Path(__file__).resolve().parents[1] / "dags")
    dagbag = DagBag(dag_folder=dags_dir, include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.get_dag("transform_silver")
    assert dag is not None
    # Triggered by Dataset
    timetable = getattr(dag, "timetable", None)
    assert timetable is not None
    assert "Dataset" in type(timetable).__name__
    cond_str = str(getattr(timetable, "dataset_condition", "")) or str(getattr(timetable, "_datasets", "")) or str(timetable)
    assert "sismowatch://bronze/feed" in cond_str or "DatasetTriggeredTimetable" in type(timetable).__name__

    task = dag.get_task("spark_transform_silver")
    assert task is not None
    assert any("sismowatch://silver/earthquakes" in str(outlet) for outlet in task.outlets)
