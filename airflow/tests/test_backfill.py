"""
Pruebas unitarias para el Backfill Histórico (Fase 5):
  - Construcción de URL FDSNWS (build_usgs_query_url).
  - Flujo de extracción histórica a Bronze y manifiesto raw (run_backfill_ingestion).
  - Integridad del DAG backfill_history, parámetros y dependencias.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from include.ingest import (
    build_usgs_query_url,
    run_backfill_ingestion,
)


def test_build_usgs_query_url_basic():
    url = build_usgs_query_url("2026-09-27", "2026-10-27")
    assert "https://earthquake.usgs.gov/fdsnws/event/1/query" in url
    assert "format=geojson" in url
    assert "starttime=2026-09-27" in url
    assert "endtime=2026-10-27" in url
    assert "minmagnitude" not in url


def test_build_usgs_query_url_with_minmag():
    dt_from = datetime(2026, 9, 1, tzinfo=timezone.utc)
    dt_to = datetime(2026, 9, 30, tzinfo=timezone.utc)
    url = build_usgs_query_url(dt_from, dt_to, minmagnitude=3.5, limit=5000)
    assert "starttime=2026-09-01" in url
    assert "endtime=2026-09-30" in url
    assert "minmagnitude=3.5" in url
    assert "limit=5000" in url


def test_run_backfill_ingestion_valid():
    sample_geojson = {
        "type": "FeatureCollection",
        "metadata": {"count": 2},
        "features": [
            {
                "type": "Feature",
                "id": "eq_backfill_1",
                "properties": {"mag": 4.5, "place": "Near coast of Peru", "time": 1729000000000},
                "geometry": {"type": "Point", "coordinates": [-77.0, -12.0, 30.0]},
            },
            {
                "type": "Feature",
                "id": "eq_backfill_2",
                "properties": {"mag": 5.1, "place": "Southern California", "time": 1729000100000},
                "geometry": {"type": "Point", "coordinates": [-118.0, 34.0, 10.0]},
            },
        ],
    }
    raw_bytes = json.dumps(sample_geojson).encode("utf-8")

    # Mocks
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = raw_bytes
    mock_session.get.return_value = mock_resp

    mock_s3 = MagicMock()
    mock_db = MagicMock()

    result = run_backfill_ingestion(
        start_date="2026-09-27",
        end_date="2026-10-27",
        min_magnitude=2.5,
        run_id="test-backfill-uuid",
        now=datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc),
        s3_client=mock_s3,
        db_conn=mock_db,
        session=mock_session,
    )

    assert result.run_id == "test-backfill-uuid"
    assert result.source == "backfill"
    assert result.status == "ok"
    assert result.event_count == 2
    assert "backfill/dt=2026-09-27/test-backfill-uuid.json" == result.object_key

    # Verifica subida a MinIO
    mock_s3.put_object.assert_called_once()
    put_args = mock_s3.put_object.call_args[1]
    assert put_args["Bucket"] == "sismowatch-bronze"
    assert put_args["Key"] == "backfill/dt=2026-09-27/test-backfill-uuid.json"
    assert put_args["Body"] == raw_bytes

    # Verifica persistencia de manifiesto en PostgreSQL
    mock_db.cursor.assert_called_once()


def test_backfill_dag_integrity():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Errores de importación: {dagbag.import_errors}"

    dag = dagbag.get_dag("backfill_history")
    assert dag is not None

    # Parámetros por defecto
    assert "days" in dag.params
    assert "start_date" in dag.params
    assert "end_date" in dag.params
    assert getattr(dag.params["days"], "value", dag.params["days"]) == 30

    # Tareas y dependencias
    ingest = dag.get_task("ingest_backfill")
    transform = dag.get_task("transform_backfill")
    publish = dag.get_task("publish_backfill")

    assert ingest is not None
    assert transform is not None
    assert publish is not None

    assert transform in ingest.downstream_list
    assert publish in transform.downstream_list

    # El publish emite el Dataset de Gold
    assert any("sismowatch://gold/earthquakes" in str(o) for o in (publish.outlets or []))
