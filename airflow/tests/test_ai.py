"""
Pruebas unitarias para el Reporte Diario con IA, Anti-Alucinaciones y Fallback (Fase 6):
  - Cálculo de indicadores deterministas (con y sin datos).
  - Generación de informe fallback determinista.
  - Validación anti-alucinaciones numéricas.
  - Resiliencia ante caída o timeout de Ollama (transición automática a fallback).
  - Auditoría e integridad del DAG daily_ai_report.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from include.ai import (
    calculate_daily_indicators,
    call_ollama,
    generate_daily_report,
    generate_deterministic_fallback_report,
    validate_anti_hallucination,
)


def test_calculate_daily_indicators_with_data():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock de agregados
    mock_cur.fetchone.side_effect = [
        # 1. query principal
        {
            "total_events": 12,
            "max_mag": 6.20,
            "avg_mag": 4.10,
            "tsunami_count": 1,
            "min_depth": 5.0,
            "max_depth": 120.0,
            "avg_depth": 35.5,
        },
        # 2. query max_event
        {
            "id": "eq_test_max",
            "mag": 6.20,
            "place": "Off the coast of Chile",
            "region": "Chile",
            "time_utc": datetime(2026, 10, 27, 8, 30, tzinfo=timezone.utc),
            "depth_km": 25.0,
            "url": "https://example.com/eq",
        },
    ]
    # 3. top regiones y 4. anomalías
    mock_cur.fetchall.side_effect = [
        [{"region": "Chile", "cnt": 8, "top_mag": 6.20}, {"region": "Peru", "cnt": 4, "top_mag": 4.50}],
        [
            {
                "window_start": datetime(2026, 10, 27, 8, 0, tzinfo=timezone.utc),
                "region": "Chile",
                "value": 5.0,
                "baseline_mean": 1.2,
                "baseline_std": 0.8,
                "zscore": 4.75,
                "explanation": "Actividad inusual",
            }
        ],
    ]

    ref_date = date(2026, 10, 27)
    ind = calculate_daily_indicators(mock_conn, ref_date)

    assert ind["has_data"] is True
    assert ind["total_events"] == 12
    assert ind["max_magnitude"] == 6.20
    assert ind["avg_magnitude"] == 4.10
    assert ind["tsunami_alerts"] == 1
    assert ind["max_event"]["place"] == "Off the coast of Chile"
    assert len(ind["top_regions"]) == 2
    assert ind["anomalies_count"] == 1


def test_calculate_daily_indicators_no_data():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "total_events": 0,
        "max_mag": None,
        "avg_mag": None,
        "tsunami_count": 0,
        "min_depth": None,
        "max_depth": None,
        "avg_depth": None,
    }
    mock_cur.fetchall.return_value = []

    ref_date = date(2026, 10, 27)
    ind = calculate_daily_indicators(mock_conn, ref_date)

    assert ind["has_data"] is False
    assert ind["total_events"] == 0
    assert ind["max_magnitude"] is None
    assert ind["anomalies_count"] == 0


def test_generate_deterministic_fallback_report_with_data():
    indicators = {
        "reference_date": "2026-10-27",
        "has_data": True,
        "total_events": 25,
        "max_magnitude": 5.8,
        "avg_magnitude": 3.4,
        "tsunami_alerts": 0,
        "max_event": {"place": "Central Alaska", "mag": 5.8},
        "top_regions": [{"region": "Alaska", "count": 18, "max_mag": 5.8}],
        "anomalies_count": 0,
        "anomalies_detail": [],
    }

    report = generate_deterministic_fallback_report(indicators, reason="Timeout con LLM")
    assert "# Informe de Actividad Sísmica Diaria — 2026-10-27" in report
    assert "Modo de Resiliencia / Fallback Activado" in report
    assert "Timeout con LLM" in report
    assert "25 eventos sísmicos" in report
    assert "M 5.80" in report
    assert "Alaska" in report


def test_generate_deterministic_fallback_report_no_data():
    indicators = {
        "reference_date": "2026-10-27",
        "has_data": False,
        "total_events": 0,
    }
    report = generate_deterministic_fallback_report(indicators, reason="Sin actividad registrada")
    assert "No se registró actividad sísmica" in report
    assert "Total de eventos analizados: **0**" in report


def test_validate_anti_hallucination_valid():
    indicators = {
        "has_data": True,
        "total_events": 10,
        "max_magnitude": 5.2,
    }
    llm_text = "El evento más relevante tuvo una magnitud M 5.2 en la región costera."
    ok, discrepancies = validate_anti_hallucination(llm_text, indicators)
    assert ok is True
    assert len(discrepancies) == 0


def test_validate_anti_hallucination_detects_magnitude_exaggeration():
    indicators = {
        "has_data": True,
        "total_events": 10,
        "max_magnitude": 4.5,
    }
    # El LLM alucina un terremoto de M 7.8
    llm_text = "Se registró un devastador terremoto de magnitud M 7.8."
    ok, discrepancies = validate_anti_hallucination(llm_text, indicators)
    assert ok is False
    assert any("Alucinación de magnitud" in d for d in discrepancies)


def test_validate_anti_hallucination_detects_events_when_no_data():
    indicators = {
        "has_data": False,
        "total_events": 0,
    }
    llm_text = "Durante la jornada se detectaron 24 sismos de variada intensidad."
    ok, discrepancies = validate_anti_hallucination(llm_text, indicators)
    assert ok is False
    assert any("no tiene datos registrados" in d for d in discrepancies)


def test_call_ollama_success():
    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "response": "# Resumen Sísmico\nActividad regular.",
            "prompt_eval_count": 150,
            "eval_count": 45,
        }
        mock_post.return_value = mock_resp

        text, in_tok, out_tok, lat = call_ollama(
            prompt="Escribe reporte",
            base_url="http://fake-ollama:11434",
            model="gemma:2b",
            timeout=10,
        )

        assert "Actividad regular" in text
        assert in_tok == 150
        assert out_tok == 45
        assert lat >= 0


def test_generate_daily_report_ollama_down_triggers_graceful_fallback():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        {"total_events": 5, "max_mag": 4.0, "avg_mag": 3.0, "tsunami_count": 0, "min_depth": 10, "max_depth": 20, "avg_depth": 15},
        {"id": "eq1", "mag": 4.0, "place": "California", "region": "CA", "time_utc": datetime(2026, 10, 27, tzinfo=timezone.utc), "depth_km": 10, "url": ""},
    ]
    mock_cur.fetchall.side_effect = [
        [{"region": "CA", "cnt": 5, "top_mag": 4.0}],
        [],
    ]

    with patch("requests.post", side_effect=requests.exceptions.ConnectionError("Ollama offline")):
        result = generate_daily_report(
            conn=mock_conn,
            ref_date=date(2026, 10, 27),
            base_url="http://host.docker.internal:11434",
        )

        assert result["status"] == "fallback"
        assert "ConnectionError" in result["error_detail"]
        assert result["total_events_reported"] == 5

        # Verifica que se grabó en ai_reports y ai_runs (4 consultas de indicadores + 2 de persistencia)
        assert mock_cur.execute.call_count == 6
        mock_conn.commit.assert_called_once()


def test_generate_daily_report_hallucination_triggers_fallback():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        {"total_events": 5, "max_mag": 4.0, "avg_mag": 3.0, "tsunami_count": 0, "min_depth": 10, "max_depth": 20, "avg_depth": 15},
        {"id": "eq1", "mag": 4.0, "place": "California", "region": "CA", "time_utc": datetime(2026, 10, 27, tzinfo=timezone.utc), "depth_km": 10, "url": ""},
    ]
    mock_cur.fetchall.side_effect = [
        [{"region": "CA", "cnt": 5, "top_mag": 4.0}],
        [],
    ]

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        # El LLM responde pero alucina un terremoto de M 8.9 cuando el max real es 4.0
        mock_resp.json.return_value = {
            "response": "# Reporte\nHubo un megaterremoto de magnitud M 8.9.",
            "prompt_eval_count": 100,
            "eval_count": 20,
        }
        mock_post.return_value = mock_resp

        result = generate_daily_report(
            conn=mock_conn,
            ref_date=date(2026, 10, 27),
        )

        assert result["status"] == "fallback"
        assert "Alucinación de magnitud" in result["error_detail"]


def test_daily_ai_report_dag_integrity():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Errores de importación: {dagbag.import_errors}"

    dag = dagbag.get_dag("daily_ai_report")
    assert dag is not None
    assert dag.schedule_interval == "0 1 * * *"

    task = dag.get_task("generate_ai_report")
    assert task is not None
    assert any("sismowatch://gold/reports" in str(o) for o in (task.outlets or []))
