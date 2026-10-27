"""
Pruebas exhaustivas para el asistente/agente analítico de IA sobre la capa Gold.
Verifica:
  - Saludos y conversación básica
  - Consultas temporales (hoy, 24 horas, ayer)
  - Consultas geográficas (Colombia con delimitación multicriterio, regiones conocidas)
  - Combinación temporal y espacial (Colombia en las últimas 24 horas, ayer en Colombia)
  - Métricas de magnitud media y sismos de mayor magnitud
  - Detección y explicación de ubicaciones no soportadas
  - Tratamiento controlado ante errores de base de datos
  - Preguntas fuera de dominio sísmico
  - Fallback determinista cuando Ollama no está disponible (sin retornar KPIs globales)
  - Verificación estricta de que una consulta de Colombia nunca retorne el total global
"""

from __future__ import annotations

import json
from unittest.mock import patch
import pytest
import requests
from fastapi.testclient import TestClient

from app.routers import agent as agent_module


def test_agent_query_unauthenticated_returns_401(client: TestClient) -> None:
    response = client.post("/api/v1/agent/query", json={"question": "¿Cuál es la actividad actual?"})
    assert response.status_code == 401


def test_agent_query_greeting(client: TestClient, viewer_headers: dict) -> None:
    """Verifica que el agente responda cordialmente ante saludos y consultas de presentación."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "Hola, ¿quién eres y qué puedes hacer?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "greeting"
    assert "asistente analítico" in data["answer"].lower() or "sismowatch" in data["answer"].lower()
    assert "capacidades" in data["data"]


def test_agent_query_out_of_domain_question(client: TestClient, viewer_headers: dict) -> None:
    """Verifica que preguntas ajenas a sismología sean rechazadas con mensaje pedagógico."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuál es el país más grande del mundo?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "out_of_domain"
    assert "fuera del ámbito" in data["answer"].lower() or "ámbito sísmico" in data["answer"].lower()
    assert data["data"].get("status") == "out_of_domain"


def test_agent_query_kpis_tool(client: TestClient, viewer_headers: dict) -> None:
    """Verifica que solo consultas de total histórico o KPIs globales invoquen la herramienta kpis."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos eventos sísmicos se han registrado en total?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "kpis"
    assert "total_eventos_curados" in data["data"]
    assert data["data"]["total_eventos_curados"] > 0


def test_agent_query_strongest_tool(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de sismos más fuertes."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuáles han sido los sismos más fuertes o de mayor magnitud?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "strongest_earthquakes"
    assert isinstance(data["data"], list)
    assert len(data["data"]) > 0


def test_agent_query_strongest_single_question(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta del sismo singular de mayor magnitud."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuál fue el terremoto con mayor magnitud?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "strongest_earthquakes"
    assert "mayor magnitud" in data["answer"].lower() or "m" in data["answer"].lower()


def test_agent_query_earthquakes_today(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de sismos del día actual (hoy en UTC)."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo hoy?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "conteo_eventos" in data["data"]
    assert data["data"]["ventana_temporal_utc"]["tipo"] == "dia_calendario_utc"
    assert "hoy" in data["answer"].lower() or "registraron" in data["answer"].lower()


def test_agent_query_earthquakes_last_24h(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de sismos de las últimas 24 horas (ventana móvil)."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo durante las últimas 24 horas?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "conteo_eventos" in data["data"]
    assert data["data"]["ventana_temporal_utc"]["tipo"] == "ventana_movil_24h"
    assert "24 horas" in data["answer"].lower()


def test_agent_query_earthquakes_colombia(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de sismos restringida geográficamente a Colombia."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo en Colombia?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "colombia" in data["data"]["ubicacion_consultada"].lower()
    assert "delimitación geográfica" in data["data"]["metodo_geografico"].lower() or "coordenadas" in data["data"]["metodo_geografico"].lower()


def test_agent_query_earthquakes_colombia_last_24h(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta combinada de Colombia durante las últimas 24 horas."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo durante las últimas 24 horas en Colombia?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "colombia" in data["data"]["ubicacion_consultada"].lower()
    assert data["data"]["ventana_temporal_utc"]["tipo"] == "ventana_movil_24h"
    assert "conteo_eventos" in data["data"]


def test_agent_query_earthquakes_colombia_yesterday(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de sismos de ayer en Colombia."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo ayer en Colombia?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "colombia" in data["data"]["ubicacion_consultada"].lower()
    assert data["data"]["ventana_temporal_utc"]["tipo"] == "dia_calendario_utc"


def test_agent_query_avg_mag_today(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de magnitud media de los sismos de hoy."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuál es la magnitud media de los sismos de hoy?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "magnitud promedio" in data["answer"].lower() or "m" in data["answer"].lower()


def test_agent_query_unsupported_location(client: TestClient, viewer_headers: dict) -> None:
    """Verifica que una ubicación no soportada o ficticia sea identificada y explicada."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo en Narnia?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert data["data"].get("status") == "unsupported_location"
    assert "narnia" in data["answer"].lower()
    assert "no es posible consultar" in data["answer"].lower() or "no corresponde" in data["answer"].lower()


def test_agent_query_db_error_handling(client: TestClient, viewer_headers: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que un error de base de datos no rompa la API y retorne un mensaje controlado sin inventar datos."""
    def fake_get_db_cursor(*args, **kwargs):
        raise RuntimeError("Conexión a PostgreSQL simulada caída")

    monkeypatch.setattr(agent_module, "get_db_cursor", fake_get_db_cursor)

    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo hoy?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert "no se pudo verificar" in data["answer"].lower()
    assert data["data"].get("status") == "db_error"


def test_agent_query_ollama_fallback_preserves_filtered_answer(client: TestClient, viewer_headers: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que ante la caída de Ollama, el fallback determinista preserve la respuesta filtrada y NO devuelva KPIs globales."""
    def fake_requests_post(*args, **kwargs):
        raise requests.exceptions.ConnectionError("Ollama offline simulado")

    monkeypatch.setattr(requests, "post", fake_requests_post)

    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo hoy?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "filtered_earthquakes"
    assert "total_eventos_curados" not in data["data"]
    assert "conteo_eventos" in data["data"]
    assert "hoy" in data["answer"].lower() or "registraron" in data["answer"].lower()


def test_colombia_does_not_mix_with_global_kpis(client: TestClient, viewer_headers: dict) -> None:
    """Comprueba que una consulta de Colombia no retorne accidentalmente el conteo global de KPIs."""
    # 1. Obtener KPIs globales
    resp_global = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos eventos sísmicos se han registrado en total?"},
        headers=viewer_headers,
    )
    global_count = resp_global.json()["data"]["total_eventos_curados"]

    # 2. Obtener sismos en Colombia
    resp_colombia = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuántos sismos hubo en Colombia?"},
        headers=viewer_headers,
    )
    colombia_count = resp_colombia.json()["data"]["conteo_eventos"]

    assert resp_colombia.json()["tool_used"] == "filtered_earthquakes"
    assert colombia_count != global_count, "El conteo de Colombia no debe ser idéntico al total global."


def test_agent_query_regional_tool(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de ranking de regiones con mayor actividad."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Qué regiones o zonas presentan mayor actividad sísmica?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "regional_activity"
    assert isinstance(data["data"], list)


def test_agent_query_country_clarification(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la distinción conceptual entre región sísmica y país político."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "¿Cuál es el país con más terremotos?"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "regional_activity"
    assert "región" in data["answer"].lower() or "pais" in data["answer"].lower() or "país" in data["answer"].lower()


def test_agent_query_report_tool(client: TestClient, viewer_headers: dict) -> None:
    """Verifica la consulta de informes diarios generados."""
    response = client.post(
        "/api/v1/agent/query",
        json={"question": "Muéstrame el último informe diario de IA generado"},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tool_used"] == "latest_report"
