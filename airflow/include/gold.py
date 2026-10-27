"""
Lógica pura y funciones auxiliares para la capa Gold (Fase 4).

Responsabilidades:
  1. Validación de la compuerta de calidad (Quality Gate): no publicar a Gold si el lote no fue aprobado.
  2. Detección estadística de anomalías mediante Z-Score por hora y región.
  3. Generación de explicaciones descriptivas legibles para analistas y reportes.
  4. Mapeo y preparación de registros para inserción idempotente en PostgreSQL.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_ZSCORE_THRESHOLD = 2.5
DEFAULT_MIN_SCORE = 80.0


def check_quality_gate(
    quality_run: Optional[Dict[str, Any]],
    min_score: float = DEFAULT_MIN_SCORE,
) -> Tuple[bool, str]:
    """
    Verifica si una ejecución de calidad cumple los criterios para publicarse en Gold.

    Retorna:
      (aprobado: bool, motivo: str)
    """
    if quality_run is None:
        return False, "No existe registro de calidad para el lote indicado"

    status = quality_run.get("status")
    if status != "passed":
        return False, f"Estado de calidad no aprobado: status='{status}'"

    score = float(quality_run.get("score") or 0.0)
    if score < min_score:
        return False, f"Puntaje de calidad ({score:.2f}) inferior al umbral mínimo requerido ({min_score:.2f})"

    approved_count = int(quality_run.get("approved") or 0)
    if approved_count <= 0:
        return False, "El lote no contiene registros aprobados para publicar"

    return True, f"Compuerta superada: {approved_count} registros aprobados con puntaje {score:.2f}"


def calculate_zscore(
    value: float,
    mean: float,
    std: float,
    threshold: float = DEFAULT_ZSCORE_THRESHOLD,
) -> Tuple[float, bool, Optional[str]]:
    """
    Calcula el Z-Score para un valor dado una media y desviación estándar de referencia.

    Fórmula: z = (value - mean) / std

    Casos borde:
      - Si std <= 0 o es NaN/inf: z = 0.0, is_anomaly = False.
      - Si value <= mean: z <= 0, no se considera anomalía de alta actividad (is_anomaly = False).
      - Si z >= threshold: is_anomaly = True y genera texto descriptivo.
    """
    if std is None or math.isnan(std) or std <= 0.0:
        return 0.0, False, None

    z = (value - mean) / std
    z_rounded = round(z, 3)

    if z >= threshold:
        explanation = (
            f"Actividad inusual detectada: {int(value)} eventos en la ventana horaria "
            f"supera el promedio regional de {mean:.2f} (std={std:.2f}, z-score={z_rounded:.2f} >= {threshold})"
        )
        return z_rounded, True, explanation

    return z_rounded, False, None


def compute_anomalies_for_windows(
    hourly_windows: List[Dict[str, Any]],
    baselines: Dict[str, Tuple[float, float]],
    threshold: float = DEFAULT_ZSCORE_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Evalúa una lista de ventanas horarias contra los baselines regionales.

    Cada elemento de hourly_windows debe contener:
      - window_start: datetime
      - region: str
      - event_count: int | float
      - window_hours: int (opcional, default 1)

    baselines es un dict:
      { region_name: (mean, std) }

    Retorna una lista de diccionarios listos para persistir en curated.anomalies.
    """
    results = []
    for win in hourly_windows:
        region = win.get("region") or "Unknown"
        val = float(win.get("event_count") or 0.0)
        mean, std = baselines.get(region, (val, 0.0))

        zscore, is_anomaly, explanation = calculate_zscore(
            value=val,
            mean=mean,
            std=std,
            threshold=threshold,
        )

        results.append({
            "window_start": win["window_start"],
            "window_hours": win.get("window_hours", 1),
            "region": region,
            "method": "zscore",
            "metric": "event_count",
            "value": val,
            "baseline_mean": round(mean, 2) if mean is not None else None,
            "baseline_std": round(std, 2) if std is not None else None,
            "zscore": zscore,
            "is_anomaly": is_anomaly,
            "explanation": explanation,
        })
    return results
