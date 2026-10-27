"""
Router para Reportes Diarios con IA y Auditoría.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, status

from app.db import get_db_cursor
from app.models import AIReportDetailResponse, AIReportSummaryResponse
from app.security import get_current_user

logger = logging.getLogger("sismowatch.api.reports")
router = APIRouter(prefix="/reports", tags=["Reportes con IA"])


@router.get("", response_model=List[AIReportSummaryResponse], summary="Listar informes diarios generados")
def list_reports(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> List[AIReportSummaryResponse]:
    """Retorna la lista histórica de informes diarios de actividad sísmica generados por el pipeline."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT report_id, reference_date, status, provider, model, generated_at,
                   LEFT(summary_md, 300) AS summary_preview
            FROM curated.ai_reports
            ORDER BY reference_date DESC, generated_at DESC
            """
        )
        rows = cur.fetchall()

    return [
        AIReportSummaryResponse(
            report_id=str(r["report_id"]),
            reference_date=r["reference_date"],
            status=r["status"],
            provider=r["provider"],
            model=r["model"],
            generated_at=r["generated_at"],
            summary_preview=r["summary_preview"] or "",
        )
        for r in rows
    ]


@router.get("/{reference_date}", response_model=AIReportDetailResponse, summary="Obtener informe por fecha")
def get_report_by_date(
    reference_date: date,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> AIReportDetailResponse:
    """Retorna el contenido completo, indicadores calculados y auditoría del informe para una fecha dada."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            SELECT report_id, reference_date, status, provider, model,
                   summary_md, indicators, anomalies, generated_at,
                   latency_ms, error_detail
            FROM curated.ai_reports
            WHERE reference_date = %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (reference_date,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Informe de actividad sísmica para la fecha '{reference_date}' no encontrado",
        )

    return AIReportDetailResponse(
        report_id=str(row["report_id"]),
        reference_date=row["reference_date"],
        status=row["status"],
        provider=row["provider"],
        model=row["model"],
        summary_md=row["summary_md"],
        indicators=row["indicators"] if isinstance(row["indicators"], dict) else {},
        anomalies=row["anomalies"] if isinstance(row["anomalies"], list) else [],
        generated_at=row["generated_at"],
        latency_ms=row.get("latency_ms"),
        error_detail=row.get("error_detail"),
    )
