"""
Pruebas para los endpoints de reportes diarios generados con IA.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_list_reports_unauthenticated_returns_401(client: TestClient) -> None:
    response = client.get("/api/v1/reports")
    assert response.status_code == 401


def test_list_reports_success(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/reports", headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        first = data[0]
        assert "report_id" in first
        assert "reference_date" in first
        assert "status" in first


def test_get_report_by_date_not_found(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/reports/1999-01-01", headers=viewer_headers)
    assert response.status_code == 404
    assert "no encontrado" in response.json()["detail"]


def test_get_report_by_date_existing_if_available(client: TestClient, viewer_headers: dict) -> None:
    list_resp = client.get("/api/v1/reports", headers=viewer_headers)
    reports = list_resp.json()
    if reports:
        ref_date = reports[0]["reference_date"]
        detail_resp = client.get(f"/api/v1/reports/{ref_date}", headers=viewer_headers)
        assert detail_resp.status_code == 200
        detail = detail_resp.json()
        assert "summary_md" in detail
        assert "indicators" in detail
        assert detail["reference_date"] == ref_date
