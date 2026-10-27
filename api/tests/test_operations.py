"""
Pruebas para los endpoints de Operaciones, Calidad y Reglas (R01-R05).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_list_quality_runs(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/quality/runs", headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        run = data[0]
        assert "run_id" in run
        assert "layer" in run
        assert "evaluated" in run
        assert "status" in run


def test_list_quality_rules_defaults_to_single_run(client: TestClient, viewer_headers: dict) -> None:
    """Verifica que sin run_id no repita reglas de todas las ejecuciones históricas."""
    response = client.get("/api/v1/quality/rules", headers=viewer_headers)
    assert response.status_code == 200
    rules = response.json()
    assert isinstance(rules, list)
    if rules:
        # Deben ser 5 reglas R01 a R05
        assert len(rules) <= 5
        run_ids = {r["run_id"] for r in rules}
        assert len(run_ids) == 1, "Debe retornar las reglas de una única ejecución"
        for r in rules:
            assert "rule_code" in r
            assert "passed" in r
            assert isinstance(r["passed"], bool)


def test_list_pipeline_runs(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/pipeline/runs", headers=viewer_headers)
    assert response.status_code == 200
    assert isinstance(response.json(), list)
