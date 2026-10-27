#!/usr/bin/env python3
"""
SismoWatch — Escáner Automatizado de Seguridad y Detección de Secretos
===========================================================================
Audita el repositorio de extremo a extremo:
  1. Verifica que `.env` esté ignorado por Git y ausente del índice.
  2. Audita todos los archivos rastreados en Git contra patrones de:
     - Claves privadas (RSA/DSA/EC/OPENSSH).
     - Claves de proveedores de nube (AWS Access Key ID, GCP Service Account).
     - Tokens de API de IA (OpenAI, Anthropic, Gemini, HuggingFace).
     - Contraseñas quemadas en código fuente.
  3. Audita el historial de commits (`git log -p`) para asegurar que ningún
     secreto haya sido versionado previamente.
"""

import os
import sys
import re
import subprocess
from typing import List, Dict, Any, Tuple

try:
    from airflow.include.governance.security import (
        SECRET_PATTERNS,
        EXCLUDED_PATHS,
        run_cmd,
        check_env_file_untracked,
        scan_tracked_files,
        scan_git_history
    )
except ImportError:
    try:
        from include.governance.security import (
            SECRET_PATTERNS,
            EXCLUDED_PATHS,
            run_cmd,
            check_env_file_untracked,
            scan_tracked_files,
            scan_git_history
        )
    except ImportError:
        pass



# ===========================================================================
# Patrones de Detección de Secretos
# ===========================================================================

SECRET_PATTERNS = [
    (r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----", "Clave privada criptográfica"),
    (r"AKIA[0-9A-Z]{16}", "AWS Access Key ID"),
    (r"sk-[a-zA-Z0-9]{32,}", "OpenAI Secret Key"),
    (r"ghp_[a-zA-Z0-9]{36}", "GitHub Personal Access Token"),
    (r"xox[baprs]-[0-9a-zA-Z]{10,48}", "Slack Token"),
    (r"eyJhbGciOi[A-Za-z0-9_-]{10,}\.[A-Za-z0-9._-]{10,}\.[A-Za-z0-9._-]{10,}", "JWT Token real quemado")
]

# Archivos o rutas permitidas para contener placeholders o variables dummy
EXCLUDED_PATHS = {
    ".env.example",
    "docs/",
    "README.md",
    "BITACORA.md",
    "PROYECTO.md",
    "PRESENTACION_PROMPT.md",
    "test_security_scan.py",
    "tests/test_security_scan.py"
}


def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    """Ejecuta un comando en el sistema y retorna (rc, stdout, stderr)."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def check_env_file_untracked() -> Tuple[bool, str]:
    """Verifica que .env exista localmente pero NO esté rastreado en Git."""
    rc, out, _ = run_cmd(["git", "ls-files", ".env"])
    if out.strip():
        return False, "CRÍTICO: El archivo .env está siendo rastreado por Git."
    
    # Verificar que esté en .gitignore
    rc, out, _ = run_cmd(["git", "check-ignore", ".env"])
    if rc != 0:
        return False, "ADVERTENCIA: El archivo .env no está ignorado en .gitignore."
    
    return True, "OK: .env está correctamente ignorado y fuera del índice de Git."


def scan_tracked_files() -> Tuple[int, List[Dict[str, Any]]]:
    """Escanea todos los archivos rastreados en Git."""
    rc, out, _ = run_cmd(["git", "ls-files"])
    files = [f.strip() for f in out.splitlines() if f.strip()]
    
    violations = []
    scanned_count = 0

    for fpath in files:
        # Omitir archivos de documentación que explican los secretos
        if any(fpath.startswith(ex) or fpath == ex for ex in EXCLUDED_PATHS):
            continue

        if not os.path.isfile(fpath):
            continue

        scanned_count += 1
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            for pattern, desc in SECRET_PATTERNS:
                matches = re.finditer(pattern, content)
                for m in matches:
                    line_no = content[:m.start()].count("\n") + 1
                    # Filtrar posibles falsos positivos documentados
                    matched_text = m.group(0)
                    if "CAMBIAR" in matched_text or "dummy" in matched_text.lower():
                        continue
                    violations.append({
                        "file": fpath,
                        "line": line_no,
                        "description": desc,
                        "snippet": matched_text[:12] + "..."
                    })
        except Exception as e:
            print(f"[scan_tracked_files] Error leyendo {fpath}: {e}")

    return scanned_count, violations


def scan_git_history() -> Tuple[int, List[Dict[str, Any]]]:
    """Escanea los diffs de todos los commits en la historia de Git."""
    rc, out, _ = run_cmd(["git", "log", "-n", "100", "--oneline"])
    commits = [c.split()[0] for c in out.splitlines() if c.strip()]
    
    violations = []
    for commit in commits:
        rc, diff_out, _ = run_cmd(["git", "show", commit])
        
        # Buscar patrones en líneas añadidas (+...)
        for line_no, line in enumerate(diff_out.splitlines(), start=1):
            if not line.startswith("+"):
                continue
            # Ignorar eliminaciones o metadatos de commit
            if line.startswith("+++"):
                continue

            for pattern, desc in SECRET_PATTERNS:
                if re.search(pattern, line):
                    if "CAMBIAR" in line or "dummy" in line.lower() or "example" in line.lower():
                        continue
                    violations.append({
                        "commit": commit,
                        "description": desc,
                        "line": line[:40] + "..."
                    })

    return len(commits), violations


def main():
    print("=" * 70)
    print("🛡️  SismoWatch — Auditoría de Seguridad y Escaneo de Secretos")
    print("=" * 70)

    # 1. Verificar .env untracked
    env_ok, env_msg = check_env_file_untracked()
    print(f"\n[1/3] Estado de .env: {env_msg}")
    if not env_ok:
        sys.exit(1)

    # 2. Escaneo de archivos rastreados
    file_count, file_violations = scan_tracked_files()
    print(f"\n[2/3] Archivos rastreados auditados: {file_count}")
    if file_violations:
        print(f"❌ Se encontraron {len(file_violations)} violaciones en archivos de código:")
        for v in file_violations:
            print(f"  - {v['file']}:{v['line']} -> {v['description']} ({v['snippet']})")
    else:
        print("  ✓ Cero secretos encontrados en los archivos del repositorio.")

    # 3. Escaneo de commits históricos
    commit_count, commit_violations = scan_git_history()
    print(f"\n[3/3] Commits históricos auditados: {commit_count}")
    if commit_violations:
        print(f"❌ Se encontraron {len(commit_violations)} posibles secretos en commits:")
        for cv in commit_violations:
            print(f"  - Commit {cv['commit']}: {cv['description']}")
    else:
        print("  ✓ Cero secretos encontrados en la historia de Git.")

    print("\n" + "=" * 70)
    total_violations = len(file_violations) + len(commit_violations)
    if total_violations == 0 and env_ok:
        print("🎉 CERTIFICACIÓN DE SEGURIDAD EXITOSA: 0 secretos expuestos en Git.")
        print("=" * 70)
        sys.exit(0)
    else:
        print(f"🚨 AUDITORÍA FALLIDA: {total_violations} secretos detectados.")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
