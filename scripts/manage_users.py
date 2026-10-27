#!/usr/bin/env python3
"""
SismoWatch — Wrapper de Host para Gestión de Usuarios y Recuperación de Acceso
Ejecuta la utilidad `app.manage_users` dentro del contenedor `api` sin requerir
exponer puertos de PostgreSQL ni dependencias locales de Python.
"""

import subprocess
import sys


def main() -> None:
    args = sys.argv[1:]
    cmd = ["docker", "compose", "exec", "-T", "api", "python3", "-m", "app.manage_users"] + args
    try:
        res = subprocess.run(cmd)
        sys.exit(res.returncode)
    except FileNotFoundError:
        print("[ERROR] Docker no encontrado en el PATH.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
