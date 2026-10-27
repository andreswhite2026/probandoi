"""
Modelos y esquemas Pydantic para validación y serialización de la API.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


# ---------------------------------------------------------------------------
# Autenticación y Usuarios
# ---------------------------------------------------------------------------
class UserLoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    password: str = Field(min_length=1)


class UserResponse(BaseModel):
    id: str
    email: str
    role: str
    is_active: bool


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    email: str


# ---------------------------------------------------------------------------
# Terremotos y KPIs
# ---------------------------------------------------------------------------
class EarthquakeResponse(BaseModel):
    id: str
    mag: Optional[float] = None
    mag_type: Optional[str] = None
    place: Optional[str] = None
    region: Optional[str] = None
    time_utc: datetime
    updated_utc: Optional[datetime] = None
    tsunami: bool = False
    sig: Optional[int] = None
    event_status: Optional[str] = None
    event_type: Optional[str] = None
    url: Optional[str] = None
    longitude: float
    latitude: float
    depth_km: Optional[float] = None
    ingest_run_id: str
    published_at: datetime


class EarthquakesListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[EarthquakeResponse]


class KPIsResponse(BaseModel):
    total_24h: int
    total_7d: int
    max_mag_24h: Optional[float] = None
    avg_mag_24h: Optional[float] = None
    tsunami_alerts_24h: int
    active_anomalies_count: int
    top_region_24h: Optional[str] = None


# ---------------------------------------------------------------------------
# Reportes y Agente de IA
# ---------------------------------------------------------------------------
class AIReportSummaryResponse(BaseModel):
    report_id: str
    reference_date: date
    status: str
    provider: str
    model: str
    generated_at: datetime
    summary_preview: str


class AIReportDetailResponse(BaseModel):
    report_id: str
    reference_date: date
    status: str
    provider: str
    model: str
    summary_md: str
    indicators: Dict[str, Any]
    anomalies: List[Dict[str, Any]]
    generated_at: datetime
    latency_ms: Optional[int] = None
    error_detail: Optional[str] = None


class AgentQueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)


class AgentQueryResponse(BaseModel):
    answer: str
    tool_used: str
    data: Any


# ---------------------------------------------------------------------------
# Operaciones, Calidad y Cuarentena
# ---------------------------------------------------------------------------
class QualityRunResponse(BaseModel):
    run_id: str
    layer: str
    evaluated: int
    approved: int
    rejected: int
    score: Optional[float] = None
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None


class QualityRuleResponse(BaseModel):
    id: int
    run_id: str
    rule_code: str
    description: str
    severity: str
    evaluated: int
    failed: int
    passed: bool


class QuarantineResponse(BaseModel):
    id: int
    run_id: str
    event_id: Optional[str] = None
    raw_object_key: Optional[str] = None
    payload: Dict[str, Any]
    reason_code: str
    reason_detail: Optional[str] = None
    created_at: datetime


class PipelineRunResponse(BaseModel):
    dag_id: str
    run_id: str
    state: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    updated_at: datetime
    note: Optional[str] = None


class BackfillTriggerRequest(BaseModel):
    days: int = Field(default=30, ge=1, le=365)
    min_magnitude: Optional[float] = Field(default=None, ge=-2.0, le=10.0)


class BackfillTriggerResponse(BaseModel):
    status: str
    message: str
    dag_run_id: Optional[str] = None
