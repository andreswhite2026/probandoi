"""
Pruebas de Control de Acceso Basado en Roles (RBAC: viewer vs admin).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


def test_quarantine_unauthenticated_returns_401(client: TestClient) -> None:
    """Sin token de autorización debe responder 401 Unauthorized."""
    response = client.get("/api/v1/quarantine")
    assert response.status_code == 401
    assert "Credenciales de autenticación no suministradas" in response.json()["detail"]


def test_quarantine_viewer_forbidden_returns_403(client: TestClient, viewer_headers: dict) -> None:
    """El rol 'viewer' no tiene permiso para consultar cuarentena: debe responder 403 Forbidden."""
    response = client.get("/api/v1/quarantine", headers=viewer_headers)
    assert response.status_code == 403
    assert "Permisos insuficientes" in response.json()["detail"]


def test_quarantine_admin_allowed_returns_200(client: TestClient, admin_headers: dict) -> None:
    """El rol 'admin' tiene permiso para consultar la cuarentena: debe responder 200 OK."""
    response = client.get("/api/v1/quarantine", headers=admin_headers)
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_backfill_trigger_unauthenticated_returns_401(client: TestClient) -> None:
    """Sin token de autorización debe responder 401 Unauthorized."""
    response = client.post("/api/v1/backfill/trigger", json={"days": 7})
    assert response.status_code == 401


def test_backfill_trigger_viewer_forbidden_returns_403(client: TestClient, viewer_headers: dict) -> None:
    """El rol 'viewer' no puede disparar backfills: debe responder 403 Forbidden."""
    response = client.post(
        "/api/v1/backfill/trigger",
        json={"days": 7},
        headers=viewer_headers,
    )
    assert response.status_code == 403
    assert "Permisos insuficientes" in response.json()["detail"]


@patch("requests.post")
def test_backfill_trigger_admin_allowed(mock_post, client: TestClient, admin_headers: dict) -> None:
    """El rol 'admin' puede disparar backfills; interactúa con la API de Airflow."""
    mock_post.return_value.status_code = 201
    mock_post.return_value.json.return_value = {
        "dag_run_id": "manual__test_run_123",
        "state": "queued",
    }

    response = client.post(
        "/api/v1/backfill/trigger",
        json={"days": 15, "min_magnitude": 5.0},
        headers=admin_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "triggered"
    assert data["dag_run_id"] == "manual__test_run_123"


def test_invalid_jwt_token_returns_401(client: TestClient) -> None:
    """Token corrupto o manipulado debe ser rechazado con 401 Unauthorized."""
    headers = {"Authorization": "Bearer invalid.token.payload"}
    response = client.get("/api/v1/earthquakes", headers=headers)
    assert response.status_code == 401
    assert "Token de acceso inválido" in response.json()["detail"]
