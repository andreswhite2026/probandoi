"""
Pruebas unitarias para la Fase 2 (Ingesta USGS).

Verifica:
  1. Ingesta válida de GeoJSON -> MinIO Bronze + raw.ingestions (status 'ok').
  2. Ingesta con 0 eventos -> MinIO Bronze + raw.ingestions (status 'empty').
  3. Payload corrupto o no-GeoJSON -> status 'invalid' + excepción controlada.
  4. Error de red/conexión -> status 'error' + excepción controlada.
  5. Integridad del DAG ingest_feed y presencia del Dataset 'sismowatch://bronze/feed'.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from airflow.models import DagBag
from include.ingest import (
    calculate_sha256,
    run_feed_ingestion,
    validate_geojson,
)

SAMPLE_VALID_GEOJSON = {
    "type": "FeatureCollection",
    "metadata": {"count": 2, "title": "USGS Earthquakes"},
    "features": [
        {
            "type": "Feature",
            "properties": {"mag": 4.5, "place": "10 km S of San Juan", "time": 1700000000000},
            "geometry": {"type": "Point", "coordinates": [-66.1, 18.4, 10.0]},
            "id": "us7000test1",
        },
        {
            "type": "Feature",
            "properties": {"mag": 5.2, "place": "Pacific-Antarctic Ridge", "time": 1700000010000},
            "geometry": {"type": "Point", "coordinates": [-120.5, -55.2, 15.0]},
            "id": "us7000test2",
        },
    ],
}

SAMPLE_EMPTY_GEOJSON = {
    "type": "FeatureCollection",
    "metadata": {"count": 0, "title": "USGS Earthquakes"},
    "features": [],
}


def test_validate_geojson_valid() -> None:
    payload = json.dumps(SAMPLE_VALID_GEOJSON).encode("utf-8")
    is_valid, count, error = validate_geojson(payload)
    assert is_valid is True
    assert count == 2
    assert error is None


def test_validate_geojson_empty() -> None:
    payload = json.dumps(SAMPLE_EMPTY_GEOJSON).encode("utf-8")
    is_valid, count, error = validate_geojson(payload)
    assert is_valid is True
    assert count == 0
    assert error is None


def test_validate_geojson_malformed_syntax() -> None:
    payload = b"esto no es json {{{ corrupto"
    is_valid, count, error = validate_geojson(payload)
    assert is_valid is False
    assert count == 0
    assert "JSON mal formado" in str(error)


def test_validate_geojson_not_feature_collection() -> None:
    payload = json.dumps({"type": "Point", "coordinates": [0, 0]}).encode("utf-8")
    is_valid, count, error = validate_geojson(payload)
    assert is_valid is False
    assert count == 0
    assert "FeatureCollection" in str(error)


def test_calculate_sha256() -> None:
    data = b'{"test": 123}'
    expected = hashlib.sha256(data).hexdigest()
    assert calculate_sha256(data) == expected


def test_run_feed_ingestion_valid_flow() -> None:
    """Verifica flujo exitoso: subida a Bronze e inserción en raw.ingestions con status 'ok'."""
    raw_bytes = json.dumps(SAMPLE_VALID_GEOJSON).encode("utf-8")
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = raw_bytes
    mock_session.get.return_value = mock_resp

    mock_s3 = MagicMock()
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    fixed_time = datetime(2026, 10, 27, 14, 0, 0, tzinfo=timezone.utc)
    test_run_id = "11111111-2222-3333-4444-555555555555"

    result = run_feed_ingestion(
        run_id=test_run_id,
        now=fixed_time,
        s3_client=mock_s3,
        db_conn=mock_db,
        session=mock_session,
    )

    assert result.run_id == test_run_id
    assert result.status == "ok"
    assert result.event_count == 2
    assert result.http_status == 200
    assert result.object_key == f"feed/dt=2026-10-27/hour=14/{test_run_id}.json"
    assert result.sha256 == hashlib.sha256(raw_bytes).hexdigest()

    # Verificar que MinIO recibió el objeto exacto
    mock_s3.put_object.assert_called_once_with(
        Bucket="sismowatch-bronze",
        Key=result.object_key,
        Body=raw_bytes,
        ContentType="application/geo+json",
    )

    # Verificar inserción en base de datos
    mock_cursor.execute.assert_called_once()
    sql_executed = mock_cursor.execute.call_args[0][0]
    sql_params = mock_cursor.execute.call_args[0][1]
    assert "INSERT INTO raw.ingestions" in sql_executed
    assert sql_params[0] == test_run_id
    assert sql_params[9] == "ok"


def test_run_feed_ingestion_empty_flow() -> None:
    """Verifica que un feed con 0 eventos se registre como 'empty' y no falle."""
    raw_bytes = json.dumps(SAMPLE_EMPTY_GEOJSON).encode("utf-8")
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = raw_bytes
    mock_session.get.return_value = mock_resp

    mock_s3 = MagicMock()
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    result = run_feed_ingestion(
        run_id="22222222-2222-3333-4444-555555555555",
        s3_client=mock_s3,
        db_conn=mock_db,
        session=mock_session,
    )

    assert result.status == "empty"
    assert result.event_count == 0


def test_run_feed_ingestion_corrupt_payload() -> None:
    """Verifica que un JSON corrupto se suba a Bronze para auditoría, se registre 'invalid' y lance excepción."""
    corrupt_bytes = b"CORRUPTED_RAW_STREAM"
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = corrupt_bytes
    mock_session.get.return_value = mock_resp

    mock_s3 = MagicMock()
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    with pytest.raises(RuntimeError, match="status=invalid"):
        run_feed_ingestion(
            run_id="33333333-2222-3333-4444-555555555555",
            s3_client=mock_s3,
            db_conn=mock_db,
            session=mock_session,
        )

    # Debe haber guardado el payload corrupto para inspección técnica
    mock_s3.put_object.assert_called_once()
    # Y haber registrado en base de datos con status 'invalid'
    sql_params = mock_cursor.execute.call_args[0][1]
    assert sql_params[9] == "invalid"


def test_run_feed_ingestion_http_error() -> None:
    """Verifica que un error HTTP o de red se registre como 'error' y lance excepción."""
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.ConnectionError("Host inalcanzable")

    mock_s3 = MagicMock()
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    with pytest.raises(RuntimeError, match="status=error"):
        run_feed_ingestion(
            run_id="44444444-2222-3333-4444-555555555555",
            s3_client=mock_s3,
            db_conn=mock_db,
            session=mock_session,
        )

    # Registró en base el error
    sql_params = mock_cursor.execute.call_args[0][1]
    assert sql_params[9] == "error"
    assert "Host inalcanzable" in sql_params[10]


def test_dag_integrity() -> None:
    """Verifica que el DAG ingest_feed sea sintácticamente válido en Airflow."""
    from pathlib import Path
    dags_dir = str(Path(__file__).resolve().parents[1] / "dags")
    dagbag = DagBag(dag_folder=dags_dir, include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.get_dag("ingest_feed")
    assert dag is not None
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.catchup is False

    task = dag.get_task("ingest_usgs_all_hour")
    assert task is not None
    # Verificar que el outlet contenga el Dataset de Bronze
    assert any("sismowatch://bronze/feed" in str(outlet) for outlet in task.outlets)
