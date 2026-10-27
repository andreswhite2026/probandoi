"""
Router de Autenticación y Perfil de Usuario.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status

from app.db import get_db_cursor
from app.models import TokenResponse, UserLoginRequest, UserResponse
from app.security import (
    create_access_token,
    get_current_user,
    verify_password,
)

logger = logging.getLogger("sismowatch.api.auth")
router = APIRouter(prefix="/auth", tags=["Autenticación"])


@router.post("/login", response_model=TokenResponse, summary="Iniciar sesión")
def login(request: UserLoginRequest) -> TokenResponse:
    """
    Autentica al usuario mediante email y contraseña.
    Emite un token JWT con expiración configurable y rol asignado (viewer o admin).
    """
    normalized_email = request.email.lower().strip()

    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, email, password_hash, role, is_active
            FROM app.users
            WHERE email = %s
            """,
            (normalized_email,),
        )
        user = cur.fetchone()

    if not user or not verify_password(request.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas o usuario no registrado",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.get("is_active"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La cuenta de usuario se encuentra inactiva",
        )

    # Actualizar fecha de último login
    now_utc = datetime.now(timezone.utc)
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE app.users SET last_login_at = %s WHERE id = %s",
                (now_utc, user["id"]),
            )
    except Exception as exc:
        logger.warning("No se pudo actualizar last_login_at para el usuario %s: %s", user["id"], exc)

    token = create_access_token(
        data={
            "sub": str(user["id"]),
            "email": user["email"],
            "role": user["role"],
        }
    )

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        role=user["role"],
        email=user["email"],
    )


@router.get("/me", response_model=UserResponse, summary="Obtener usuario actual autenticado")
def get_me(current_user: Dict[str, Any] = Depends(get_current_user)) -> UserResponse:
    """Retorna los datos del usuario correspondiente al token JWT suministrado."""
    return UserResponse(
        id=str(current_user["id"]),
        email=current_user["email"],
        role=current_user["role"],
        is_active=current_user["is_active"],
    )
