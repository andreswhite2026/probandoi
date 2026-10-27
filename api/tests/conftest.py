"""
Configuración de fixtures para la suite de pruebas de FastAPI.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Asegurar que el directorio de la aplicación esté en el PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.config import settings
from app.main import app
from app.security import create_access_token


@pytest.fixture(scope="session")
def client() -> Generator[TestClient, None, None]:
    """Cliente HTTP síncrono para interactuar con la API FastAPI."""
    with TestClient(app) as c:
        yield c


@pytest.fixture
def admin_headers() -> Dict[str, str]:
    """Cabeceras de autenticación con Bearer token y rol 'admin'."""
    token = create_access_token(
        data={
            "sub": "00000000-0000-0000-0000-000000000001",
            "email": "admin@sismowatch.local",
            "role": "admin",
        }
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def viewer_headers() -> Dict[str, str]:
    """Cabeceras de autenticación con Bearer token y rol 'viewer'."""
    token = create_access_token(
        data={
            "sub": "00000000-0000-0000-0000-000000000002",
            "email": "viewer@sismowatch.local",
            "role": "viewer",
        }
    )
    return {"Authorization": f"Bearer {token}"}
