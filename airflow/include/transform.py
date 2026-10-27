"""
Lógica de validación, reglas de calidad y parsing para Silver (Fase 3).

Reglas declarativas (ADR-007):
  - R01_ID_REQUIRED: id presente y no vacío.
  - R02_TIME_VALID: fecha presente y no futura (margen máx 5 min por skew).
  - R03_COORDS_RANGE: orden USGS [lon, lat, depth], lat [-90, 90], lon [-180, 180], prof >= -10 km.
  - R04_MAG_RANGE: magnitud nula o entre -2.0 y 10.5.
  - R05_EVENT_TYPE: tipo de evento dentro del catálogo admitido por USGS.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

VALID_EVENT_TYPES = frozenset([
    "earthquake",
    "quarry blast",
    "ice quake",
    "explosion",
    "chemical explosion",
    "nuclear explosion",
    "rock burst",
    "other event",
    "other",
])


def derive_region(place: str | None) -> str:
    """
    Deriva una región normalizada a partir del texto descriptivo 'place' de USGS.
    Heurística documentada:
      1. Si contiene ', ', toma el segmento final (país o estado, ej: 'Puerto Rico', 'California').
      2. Si contiene ' of ', toma el segmento posterior a ' of '.
      3. En su defecto, usa el texto limpio o 'Unknown'.
    """
    if not place or not place.strip():
        return "Unknown"
    cleaned = place.strip()
    if ", " in cleaned:
        return cleaned.split(", ")[-1].strip()
    if " of " in cleaned:
        return cleaned.split(" of ")[-1].strip()
    return cleaned


def epoch_ms_to_utc(epoch_ms: int | float | None) -> datetime | None:
    """Convierte epoch en milisegundos a objeto datetime en UTC."""
    if epoch_ms is None:
        return None
    try:
        seconds = float(epoch_ms) / 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def parse_feature(feature: dict[str, Any], ingest_run_id: str) -> dict[str, Any]:
    """
    Parsea una feature cruda de GeoJSON y la aplana a estructura de Silver.
    Respeta el estándar USGS para coordenadas: [longitud, latitud, profundidad_km].
    """
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") if isinstance(geom, dict) else None

    # Extracción de coordenadas respetando orden USGS [lon, lat, depth]
    lon, lat, depth = None, None, None
    if isinstance(coords, (list, tuple)) and len(coords) >= 2:
        try:
            lon = float(coords[0])
            lat = float(coords[1])
            if len(coords) >= 3 and coords[2] is not None:
                depth = float(coords[2])
        except (ValueError, TypeError):
            pass

    time_utc = epoch_ms_to_utc(props.get("time"))
    updated_utc = epoch_ms_to_utc(props.get("updated"))

    mag = None
    if props.get("mag") is not None:
        try:
            mag = round(float(props["mag"]), 2)
        except (ValueError, TypeError):
            pass

    sig = None
    if props.get("sig") is not None:
        try:
            sig = int(props["sig"])
        except (ValueError, TypeError):
            pass

    tsunami_val = props.get("tsunami")
    tsunami = bool(tsunami_val) if tsunami_val is not None else False

    place = props.get("place")
    region = derive_region(place)

    event_id = str(feature.get("id") or "").strip()

    return {
        "id": event_id,
        "mag": mag,
        "mag_type": props.get("magType"),
        "place": place,
        "region": region,
        "time_utc": time_utc,
        "updated_utc": updated_utc,
        "tsunami": tsunami,
        "sig": sig,
        "event_status": props.get("status"),
        "event_type": props.get("type"),
        "url": props.get("url"),
        "longitude": lon,
        "latitude": lat,
        "depth_km": depth,
        "ingest_run_id": ingest_run_id,
        "raw_payload": feature,
    }


def evaluate_quality_rules(
    record: dict[str, Any],
    now: datetime | None = None,
) -> tuple[bool, list[tuple[str, str]]]:
    """
    Aplica las reglas declarativas R01 a R05.
    Retorna (es_valido, lista_de_fallas_con_codigo_y_detalle).
    """
    failures: list[tuple[str, str]] = []
    current_time = now or datetime.now(timezone.utc)

    # R01: Identificador requerido
    event_id = record.get("id")
    if not event_id:
        failures.append(("R01_ID_REQUIRED", "Identificador 'id' ausente o vacío"))

    # R02: Fecha válida y no futura
    time_utc = record.get("time_utc")
    if time_utc is None:
        failures.append(("R02_TIME_VALID", "Fecha 'time_utc' ausente o inválida"))
    else:
        # Permitimos hasta 5 min de skew por diferencias de reloj entre servidores
        diff_seconds = (time_utc - current_time).total_seconds()
        if diff_seconds > 300:
            failures.append(("R02_TIME_VALID", f"Fecha futura detectada ({time_utc} > {current_time})"))

    # R03: Coordenadas dentro de rango geodésico
    lat = record.get("latitude")
    lon = record.get("longitude")
    depth = record.get("depth_km")

    if lat is None or not (-90.0 <= lat <= 90.0):
        failures.append(("R03_COORDS_RANGE", f"Latitud fuera de rango [-90, 90]: {lat}"))
    if lon is None or not (-180.0 <= lon <= 180.0):
        failures.append(("R03_COORDS_RANGE", f"Longitud fuera de rango [-180, 180]: {lon}"))
    if depth is not None and depth < -10.0:
        failures.append(("R03_COORDS_RANGE", f"Profundidad anómala (< -10 km): {depth}"))

    # R04: Rango de magnitud
    mag = record.get("mag")
    if mag is not None and not (-2.0 <= mag <= 10.5):
        failures.append(("R04_MAG_RANGE", f"Magnitud fuera de rango [-2.0, 10.5]: {mag}"))

    # R05: Catálogo de tipos de evento
    event_type = record.get("event_type")
    if not event_type or str(event_type).lower() not in VALID_EVENT_TYPES:
        failures.append(("R05_EVENT_TYPE", f"Tipo de evento desconocido: '{event_type}'"))

    is_valid = len(failures) == 0
    return is_valid, failures
