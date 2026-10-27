"""
Pruebas para los endpoints de sismos, KPIs y exportaciones CSV / GeoJSON.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_list_earthquakes_unauthenticated_returns_401(client: TestClient) -> None:
    response = client.get("/api/v1/earthquakes")
    assert response.status_code == 401


def test_list_earthquakes_success(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes?limit=10&offset=0", headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "items" in data
    assert data["limit"] == 10
    assert data["offset"] == 0
    assert isinstance(data["items"], list)


def test_list_earthquakes_filter_min_mag(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes?min_mag=5.0", headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    for item in data["items"]:
        if item["mag"] is not None:
            assert item["mag"] >= 5.0


def test_get_earthquake_by_id_not_found_404(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes/nonexistent_id_99999", headers=viewer_headers)
    assert response.status_code == 404
    assert "no encontrado" in response.json()["detail"]


def test_get_kpis_success(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes/kpis", headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    assert "total_24h" in data
    assert "total_7d" in data
    assert "tsunami_alerts_24h" in data
    assert "active_anomalies_count" in data


def test_export_earthquakes_csv(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes/export?format=csv&limit=5", headers=viewer_headers)
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")
    assert "attachment; filename=" in response.headers.get("content-disposition", "")
    content = response.text
    assert "id,mag,mag_type,place,region,time_utc" in content


def test_export_earthquakes_geojson(client: TestClient, viewer_headers: dict) -> None:
    response = client.get("/api/v1/earthquakes/export?format=geojson&limit=5", headers=viewer_headers)
    assert response.status_code == 200
    assert "application/geo+json" in response.headers.get("content-type", "")
    data = response.json()
    assert data.get("type") == "FeatureCollection"
    assert "features" in data
    assert isinstance(data["features"], list)
