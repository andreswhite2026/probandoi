"""
Configuración centralizada de la API mediante variables de entorno.
"""

from __future__ import annotations

import os
from typing import List


def get_env(key: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(f"Variable de entorno requerida no configurada: {key}")
    return val or ""


class Settings:
    # Base de datos (Rol de mínimo privilegio sismo_api)
    DB_HOST: str = get_env("POSTGRES_HOST", "postgres")
    DB_PORT: int = int(get_env("POSTGRES_PORT", "5432"))
    DB_NAME: str = get_env("POSTGRES_DB", "sismowatch")
    DB_USER: str = get_env("SISMO_API_USER", "sismo_api")
    DB_PASSWORD: str = get_env("SISMO_API_PASSWORD", "sismo_api_pwd")

    # Seguridad y JWT
    JWT_SECRET: str = get_env("JWT_SECRET", "sismowatch_jwt_secret_dev_key_only_change_in_prod_12345")
    JWT_ALGORITHM: str = get_env("JWT_ALGORITHM", "HS256")
    JWT_EXPIRE_MINUTES: int = int(get_env("JWT_EXPIRE_MINUTES", "60"))

    # Usuarios iniciales / Seeds
    ADMIN_EMAIL: str = get_env("ADMIN_EMAIL", "admin@sismowatch.local").lower()
    ADMIN_PASSWORD: str = get_env("ADMIN_PASSWORD", "admin123")
    VIEWER_EMAIL: str = get_env("VIEWER_EMAIL", "viewer@sismowatch.local").lower()
    VIEWER_PASSWORD: str = get_env("VIEWER_PASSWORD", "viewer123")

    # CORS
    CORS_ORIGINS: List[str] = [
        origin.strip()
        for origin in get_env("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",")
        if origin.strip()
    ]

    # IA y orquestación
    OLLAMA_BASE_URL: str = get_env("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
    OLLAMA_MODEL: str = get_env("OLLAMA_MODEL", "gemma:2b")
    AIRFLOW_BASE_URL: str = get_env("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    AIRFLOW_USER: str = get_env("AIRFLOW_ADMIN_USER", "admin")
    AIRFLOW_PASSWORD: str = get_env("AIRFLOW_ADMIN_PASSWORD", "admin")


settings = Settings()
