"""
Pruebas para el router de autenticación y emisión de tokens JWT.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import settings


def test_login_admin_success(client: TestClient) -> None:
    response = client.post(
        "/api/v1/auth/login",
        json={"email": settings.ADMIN_EMAIL, "password": settings.ADMIN_PASSWORD},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["role"] == "admin"
    assert data["email"] == settings.ADMIN_EMAIL


def test_login_viewer_success(client: TestClient) -> None:
    response = client.post(
        "/api/v1/auth/login",
        json={"email": settings.VIEWER_EMAIL, "password": settings.VIEWER_PASSWORD},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["role"] == "viewer"
    assert data["email"] == settings.VIEWER_EMAIL


def test_login_invalid_password(client: TestClient) -> None:
    response = client.post(
        "/api/v1/auth/login",
        json={"email": settings.ADMIN_EMAIL, "password": "wrong_password_123"},
    )
    assert response.status_code == 401
    assert "Credenciales incorrectas" in response.json()["detail"]


def test_login_nonexistent_user(client: TestClient) -> None:
    response = client.post(
        "/api/v1/auth/login",
        json={"email": "nobody@sismowatch.local", "password": "password"},
    )
    assert response.status_code == 401


def test_me_endpoint_with_auth(client: TestClient, admin_headers: dict) -> None:
    response = client.get("/api/v1/auth/me", headers=admin_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["email"] == settings.ADMIN_EMAIL
    assert data["role"] == "admin"
    assert data["is_active"] is True


def test_me_endpoint_without_auth_fails(client: TestClient) -> None:
    response = client.get("/api/v1/auth/me")
    assert response.status_code == 401
