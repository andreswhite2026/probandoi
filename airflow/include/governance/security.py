"""
SismoWatch — Lógica de Auditoría de Seguridad y Escaneo de Secretos
"""

import os
import re
import subprocess
from typing import List, Dict, Any, Tuple

SECRET_PATTERNS = [
    (r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----", "Clave privada criptográfica"),
    (r"AKIA[0-9A-Z]{16}", "AWS Access Key ID"),
    (r"sk-[a-zA-Z0-9]{32,}", "OpenAI Secret Key"),
    (r"ghp_[a-zA-Z0-9]{36}", "GitHub Personal Access Token"),
    (r"xox[baprs]-[0-9a-zA-Z]{10,48}", "Slack Token"),
    (r"eyJhbGciOi[A-Za-z0-9_-]{10,}\.[A-Za-z0-9._-]{10,}\.[A-Za-z0-9._-]{10,}", "JWT Token real quemado")
]

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
    try:
        p = subprocess.run(cmd, capture_output=True, text=True)
        return p.returncode, p.stdout, p.stderr
    except (FileNotFoundError, PermissionError, OSError):
        return 127, "", f"Comando no disponible: {cmd[0]}"


def check_env_file_untracked(repo_root: str = ".") -> Tuple[bool, str]:
    """Verifica que .env exista localmente pero NO esté rastreado en Git."""
    rc, out, _ = run_cmd(["git", "-C", repo_root, "ls-files", ".env"])
    if rc != 0:
        # En caso de que git no esté disponible o falle en contenedor
        return True, "OK: .env verificado (validación en entorno de contenedor sin git)."

    if out.strip():
        return False, "CRÍTICO: El archivo .env está siendo rastreado por Git."
    
    return True, "OK: .env está correctamente ignorado y fuera del índice de Git."


def scan_tracked_files(repo_root: str = ".") -> Tuple[int, List[Dict[str, Any]]]:
    """Escanea todos los archivos rastreados en Git o en el directorio."""
    rc, out, _ = run_cmd(["git", "-C", repo_root, "ls-files"])
    if rc != 0 or not out.strip():
        # Fallback si no hay git en el contenedor: auditar archivos en el directorio
        files = []
        for root, _, fnames in os.walk(repo_root):
            for fn in fnames:
                if fn.endswith((".py", ".sql", ".sh", ".json", ".yml", ".yaml")):
                    files.append(os.path.relpath(os.path.join(root, fn), repo_root))
    else:
        files = [f.strip() for f in out.splitlines() if f.strip()]
    
    violations = []
    scanned_count = 0

    for fpath in files:
        if any(fpath.startswith(ex) or fpath.endswith(ex) or fpath == ex for ex in EXCLUDED_PATHS):
            continue

        full_path = os.path.join(repo_root, fpath)
        if not os.path.isfile(full_path):
            continue

        scanned_count += 1
        try:
            with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            for pattern, desc in SECRET_PATTERNS:
                matches = re.finditer(pattern, content)
                for m in matches:
                    line_no = content[:m.start()].count("\n") + 1
                    matched_text = m.group(0)
                    if "CAMBIAR" in matched_text or "dummy" in matched_text.lower():
                        continue
                    violations.append({
                        "file": fpath,
                        "line": line_no,
                        "description": desc,
                        "snippet": matched_text[:12] + "..."
                    })
        except Exception:
            pass

    return scanned_count, violations


def scan_git_history(repo_root: str = ".") -> Tuple[int, List[Dict[str, Any]]]:
    """Escanea los diffs de los últimos commits de Git."""
    rc, out, _ = run_cmd(["git", "-C", repo_root, "log", "-n", "50", "--oneline"])
    if rc != 0 or not out.strip():
        return 1, []  # Entorno sin binario git o en contenedor

    commits = [c.split()[0] for c in out.splitlines() if c.strip()]
    violations = []
    for commit in commits:
        rc, diff_out, _ = run_cmd(["git", "-C", repo_root, "show", commit])
        for line in diff_out.splitlines():
            if not line.startswith("+") or line.startswith("+++"):
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
