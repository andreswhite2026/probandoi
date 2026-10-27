#!/usr/bin/env python3
"""
Job ejecutable de Reporte Diario de Inteligencia Artificial (Fase 6).

Responsabilidades:
  1. Recibir fecha de referencia (--date) o tomar por defecto la jornada en curso/anterior.
  2. Ejecutar generate_daily_report():
     - Indicadores deterministas desde Gold.
     - Invocación a Ollama o Fallback transparente ante caídas.
     - Validación anti-alucinaciones numéricas.
     - Persistencia en curated.ai_reports y auditoría en curated.ai_runs.
  3. Imprimir resumen en formato JSON estructurado.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, "/opt/airflow")
from include.ai import generate_daily_report, get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sismowatch.report_job")


def parse_date(date_str: str | None) -> date:
    if not date_str:
        # Por defecto la fecha actual UTC
        return datetime.now(timezone.utc).date()
    return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generador de Reporte Diario con IA y Resiliencia")
    parser.add_argument("--date", type=str, default=None, help="Fecha de referencia YYYY-MM-DD")
    parser.add_argument("--force-fallback", action="store_true", help="Forzar generación en modo fallback")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout para Ollama en segundos")
    parser.add_argument("--model", type=str, default=None, help="Modelo de Ollama (ej: gemma:2b)")
    args = parser.parse_args()

    ref_date = parse_date(args.date)
    conn = get_db_connection()

    try:
        logger.info("Iniciando generación de reporte para la fecha %s", ref_date)
        kwargs = {
            "conn": conn,
            "ref_date": ref_date,
            "timeout": args.timeout,
            "force_fallback": args.force_fallback,
        }
        if args.model:
            kwargs["model"] = args.model

        result = generate_daily_report(**kwargs)
        logger.info("Reporte finalizado: %s", result)
        print(json.dumps(result, indent=2))
        return 0

    except Exception as exc:
        logger.exception("Error crítico durante la generación del reporte: %s", exc)
        print(json.dumps({"status": "failed", "error": str(exc)}))
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
