"""
Pruebas para el asistente/agente analítico de IA sobre datos de Gold.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_agent_query_unauthenticated_returns_401(client: TestClient) -> None:
    response = client.post("/api/v1/agent/query", json={"question": "¿Cuál es la actividad actual?"})
    assert response.status_code == 401


def test_agent_query_kpis_tool(client: TestClient, viewer_headers: dict) -> None:
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos eventos sísmicos se han registrado en total?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert data["tool_used"] == "kpis"
    assert "total_eventos_curados" in data["data"]


def test_agent_query_strongest_tool(client: TestClient, viewer_headers: dict) -> None:
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuáles han sido los sismos más fuertes o de mayor magnitud?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "strongest_earthquakes"
    assert isinstance(data["data"], list)


def test_agent_query_regional_tool(client: TestClient, viewer_headers: dict) -> None:
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Qué regiones o zonas presentan mayor actividad sísmica?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "regional_activity"
    assert isinstance(data["data"], list)


def test_agent_query_report_tool(client: TestClient, viewer_headers: dict) -> None:
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "Muéstrame el último informe diario de IA generado"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "latest_report"
    assert "report_id" in data["data"] or "extracto" in data["data"] or {} == data["data"]
