"""
Router para Operaciones del Pipeline, Calidad, Cuarentena y Disparo de Backfills.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.config import settings
from app.db import get_db_cursor
from app.models import (
    BackfillTriggerRequest,
    BackfillTriggerResponse,
    PipelineRunResponse,
    QualityRuleResponse,
    QualityRunResponse,
    QuarantineResponse,
)
from app.security import get_current_user, require_admin

logger = logging.getLogger("sismowatch.api.operations")
router = APIRouter(tags=["Operaciones del Pipeline"])


@router.get("/quality/runs", response_model=List[QualityRunResponse], summary="Listar ejecuciones de calidad")
def list_quality_runs(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> List[QualityRunResponse]:
    """Retorna el historial de ejecuciones de validación de calidad de datos (Gold)."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT run_id, layer, evaluated, approved, rejected, score, status, started_at, finished_at
            FROM curated.quality_runs
            ORDER BY started_at DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()

    return [
        QualityRunResponse(
            run_id=str(r["run_id"]),
            layer=r["layer"],
            evaluated=r["evaluated"],
            approved=r["approved"],
            rejected=r["rejected"],
            score=float(r["score"]) if r["score"] is not None else None,
            status=r["status"],
            started_at=r["started_at"],
            finished_at=r["finished_at"],
        )
        for r in rows
    ]


@router.get("/quality/rules", response_model=List[QualityRuleResponse], summary="Listar detalle por regla de calidad")
def list_quality_rules(
    run_id: Optional[str] = Query(default=None, description="Filtrar por run_id específico"),
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> List[QualityRuleResponse]:
    """Retorna las reglas evaluadas (R01-R05) y su resultado (passed, failed, severidad)."""
    where_sql = "WHERE run_id = %s" if run_id else ""
    params = (run_id,) if run_id else ()

    with get_db_cursor() as cur:
        cur.execute(
            f"""
            SELECT id, run_id, rule_code, description, severity, evaluated, failed, passed
            FROM curated.quality_rules
            {where_sql}
            ORDER BY id ASC
            LIMIT 200
            """,
            params,
        )
        rows = cur.fetchall()

    return [
        QualityRuleResponse(
            id=r["id"],
            run_id=str(r["run_id"]),
            rule_code=r["rule_code"],
            description=r["description"],
            severity=r["severity"],
            evaluated=r["evaluated"],
            failed=r["failed"],
            passed=r["passed"],
        )
        for r in rows
    ]


@router.get("/pipeline/runs", response_model=List[PipelineRunResponse], summary="Estado de ejecuciones de DAGs")
def list_pipeline_runs(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> List[PipelineRunResponse]:
    """Retorna el estado agregado de ejecuciones de pipelines registrado en la capa Gold."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT dag_id, run_id, state, started_at, finished_at, updated_at, note
            FROM curated.pipeline_runs
            ORDER BY updated_at DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()

    return [
        PipelineRunResponse(
            dag_id=r["dag_id"],
            run_id=r["run_id"],
            state=r["state"],
            started_at=r["started_at"],
            finished_at=r["finished_at"],
            updated_at=r["updated_at"],
            note=r.get("note"),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Endpoints con RBAC Estricto (Solo Admin)
# ---------------------------------------------------------------------------
@router.get("/quarantine", response_model=List[QuarantineResponse], summary="Consultar cuarentena (Solo Admin)")
def list_quarantine(
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> List[QuarantineResponse]:
    """
    Retorna los registros que violaron reglas bloqueantes de calidad y fueron puestos en cuarentena.
    Acceso restringido exclusivamente a usuarios con rol 'admin'.
    """
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, run_id, event_id, raw_object_key, payload, reason_code, reason_detail, created_at
            FROM curated.quarantine
            ORDER BY created_at DESC
            LIMIT 100
            """
        )
        rows = cur.fetchall()

    return [
        QuarantineResponse(
            id=r["id"],
            run_id=str(r["run_id"]),
            event_id=r["event_id"],
            raw_object_key=r["raw_object_key"],
            payload=r["payload"] if isinstance(r["payload"], dict) else {},
            reason_code=r["reason_code"],
            reason_detail=r["reason_detail"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


@router.post("/backfill/trigger", response_model=BackfillTriggerResponse, summary="Disparar backfill (Solo Admin)")
def trigger_backfill(
    request: BackfillTriggerRequest,
    admin_user: Dict[str, Any] = Depends(require_admin),
) -> BackfillTriggerResponse:
    """
    Dispara la ejecución parametrizada del DAG backfill_history en Apache Airflow.
    Acceso restringido exclusivamente a usuarios con rol 'admin'.
    """
    conf_payload: Dict[str, Any] = {"days": request.days}
    if request.min_magnitude is not None:
        conf_payload["min_magnitude"] = request.min_magnitude

    airflow_url = f"{settings.AIRFLOW_BASE_URL}/api/v1/dags/backfill_history/dagRuns"

    try:
        resp = requests.post(
            airflow_url,
            auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASSWORD),
            json={"conf": conf_payload},
            timeout=10.0,
        )
    except Exception as exc:
        logger.error("Error al conectar con la API de Airflow (%s): %s", airflow_url, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"No se pudo contactar con el orquestador Airflow: {exc}",
        )

    if resp.status_code not in (200, 201):
        logger.error("Airflow rechazó la solicitud de backfill (%s): %s", resp.status_code, resp.text)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Airflow retornó error al disparar DAG: {resp.text}",
        )

    run_data = resp.json()
    dag_run_id = run_data.get("dag_run_id")

    return BackfillTriggerResponse(
        status="triggered",
        message=f"Backfill de {request.days} días iniciado exitosamente en Airflow",
        dag_run_id=dag_run_id,
    )
