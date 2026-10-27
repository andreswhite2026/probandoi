"""
Pruebas para el endpoint de Server-Sent Events (SSE).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_events_stream_media_type(client: TestClient) -> None:
    """Verifica que el canal de eventos SSE responda con text/event-stream."""
    with client.stream("GET", "/api/v1/events/stream") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")
        # Leer las primeras líneas del stream
        lines = []
        for line in response.iter_lines():
            if line:
                lines.append(line)
            if len(lines) >= 3:
                break
        assert any("event:" in l or "data:" in l for l in lines)
