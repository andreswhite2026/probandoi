"""
Pruebas automatizadas de Auditoría de Seguridad y Detección de Secretos.
Verifica:
  1. Que `.env` no esté versionado en Git y esté ignorado.
  2. Que ningún archivo del repositorio contenga secretos expuestos.
  3. Que los commits históricos estén libres de claves o credenciales reales.
  4. Que el detector reconozca y alerte ante un patrón de clave sintética.
"""

import re
try:
    import pytest
except ImportError:
    pytest = None

try:
    from include.governance.security import (
        check_env_file_untracked,
        scan_tracked_files,
        scan_git_history,
        SECRET_PATTERNS
    )
except ImportError:
    from airflow.include.governance.security import (
        check_env_file_untracked,
        scan_tracked_files,
        scan_git_history,
        SECRET_PATTERNS
    )



def test_env_file_is_strictly_untracked():
    """Verifica que el archivo .env no esté rastreado y permanezca en .gitignore."""
    ok, msg = check_env_file_untracked()
    assert ok is True, msg


def test_tracked_files_have_zero_leaked_secrets():
    """Verifica que ningún archivo rastreado en Git contenga claves privadas o credenciales."""
    scanned_count, violations = scan_tracked_files()
    assert scanned_count > 0, "No se auditaron archivos rastreados."
    assert len(violations) == 0, f"Se detectaron violaciones de seguridad: {violations}"


def test_git_commit_history_is_clean():
    """Verifica que los últimos commits no contengan secretos expuestos."""
    commit_count, violations = scan_git_history()
    assert commit_count > 0, "No se encontraron commits para auditar."
    assert len(violations) == 0, f"Se detectaron posibles secretos en commits: {violations}"


def test_synthetic_secret_detection_logic():
    """Verifica que el escáner sea capaz de detectar un secreto sintético si se presentara."""
    header = "-----BEGIN " + "RSA " + "PRIVATE KEY-----"
    synthetic_key = f"{header}\nMIIEowIBAAKCAQEA0...\n-----END RSA PRIVATE KEY-----"
    detected = False
    for pat, desc in SECRET_PATTERNS:
        if re.search(pat, synthetic_key):
            detected = True
            break
    assert detected is True, "El escáner falló en detectar el patrón de clave RSA."
