"""
SismoWatch — Utilidad CLI para Administración y Recuperación de Usuarios (RBAC).
Permite listar, crear y restablecer contraseñas de usuarios en app.users sin exponer secretos.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from typing import Optional

from app.db import get_db_cursor
from app.security import hash_password


def list_users() -> None:
    """Lista los usuarios registrados en app.users."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, email, role, is_active, created_at, last_login_at
            FROM app.users
            ORDER BY role, email
            """
        )
        users = cur.fetchall()

    if not users:
        print("[manage_users] No hay usuarios registrados en app.users.")
        return

    print("=" * 80)
    print(f"{'Email':<30} {'Rol':<10} {'Activo':<8} {'Último Login':<25}")
    print("-" * 80)
    for u in users:
        last_login = str(u["last_login_at"]) if u.get("last_login_at") else "Nunca"
        print(f"{u['email']:<30} {u['role']:<10} {str(u['is_active']):<8} {last_login:<25}")
    print("=" * 80)


def create_or_update_user(email: str, password: str, role: str, is_active: bool = True) -> None:
    """Crea un usuario o actualiza su contraseña de forma segura con hash bcrypt."""
    normalized_email = email.lower().strip()
    if role not in ("admin", "viewer"):
        print(f"[ERROR] Rol no válido: '{role}'. Debe ser 'admin' o 'viewer'.", file=sys.stderr)
        sys.exit(1)

    if len(password) < 6:
        print("[ERROR] La contraseña debe tener al menos 6 caracteres.", file=sys.stderr)
        sys.exit(1)

    pwd_hash = hash_password(password)
    user_id = str(uuid.uuid4())

    with get_db_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO app.users (id, email, password_hash, role, is_active)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE
            SET password_hash = EXCLUDED.password_hash,
                role = EXCLUDED.role,
                is_active = EXCLUDED.is_active
            """,
            (user_id, normalized_email, pwd_hash, role, is_active),
        )

    print(f"[manage_users] ✓ Usuario '{normalized_email}' (rol: {role}) guardado/actualizado exitosamente.")


def reset_password(email: str, new_password: str) -> None:
    """Restablece la contraseña de un usuario existente para recuperación administrativa."""
    normalized_email = email.lower().strip()
    if len(new_password) < 6:
        print("[ERROR] La contraseña debe tener al menos 6 caracteres.", file=sys.stderr)
        sys.exit(1)

    pwd_hash = hash_password(new_password)

    with get_db_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE app.users SET password_hash = %s WHERE email = %s",
            (pwd_hash, normalized_email),
        )
        updated = cur.rowcount

    if updated == 0:
        print(f"[ERROR] No se encontró el usuario con email '{normalized_email}'.", file=sys.stderr)
        sys.exit(1)

    print(f"[manage_users] ✓ Contraseña restablecida exitosamente para '{normalized_email}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Administración de usuarios y credenciales en SismoWatch.")
    parser.add_argument("--list", action="store_true", help="Listar todos los usuarios y sus roles")
    parser.add_argument("--upsert", action="store_true", help="Crear o actualizar un usuario")
    parser.add_argument("--reset-password", action="store_true", help="Restablecer contraseña de un usuario existente")
    parser.add_argument("--email", type=str, help="Email del usuario")
    parser.add_argument("--password", type=str, help="Contraseña en texto claro (se hashea con bcrypt)")
    parser.add_argument("--role", type=str, default="viewer", choices=["admin", "viewer"], help="Rol del usuario")

    args = parser.parse_args()

    if args.list:
        list_users()
    elif args.upsert:
        if not args.email or not args.password:
            print("[ERROR] Se requiere --email y --password para --upsert.", file=sys.stderr)
            sys.exit(1)
        create_or_update_user(args.email, args.password, args.role)
    elif args.reset_password:
        if not args.email or not args.password:
            print("[ERROR] Se requiere --email y --password para --reset-password.", file=sys.stderr)
            sys.exit(1)
        reset_password(args.email, args.password)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
