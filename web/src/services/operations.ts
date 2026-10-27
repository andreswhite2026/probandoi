import { apiFetch } from './api'

export interface QualityRun {
  run_id: string
  layer: string
  evaluated: number
  approved: number
  rejected: number
  score: number | null
  status: string
  started_at: string
  finished_at: string | null
}

export interface QualityRule {
  id: number
  run_id: string
  rule_code: string
  description: string
  severity: string
  evaluated: number
  failed: number
  passed: boolean
}

export interface PipelineRun {
  dag_id: string
  run_id: string
  state: string
  started_at: string | null
  finished_at: string | null
  updated_at: string
  note: string | null
}

export interface QuarantineItem {
  id: number
  run_id: string
  event_id: string | null
  raw_object_key: string | null
  payload: Record<string, any>
  reason_code: string
  reason_detail: string | null
  created_at: string
}

export interface BackfillResponse {
  status: string
  message: string
  dag_run_id: string | null
}

export async function fetchQualityRuns(): Promise<QualityRun[]> {
  return apiFetch<QualityRun[]>('/api/v1/quality/runs')
}

export async function fetchQualityRules(runId?: string): Promise<QualityRule[]> {
  const query = runId ? `?run_id=${runId}` : ''
  return apiFetch<QualityRule[]>(`/api/v1/quality/rules${query}`)
}

export async function fetchPipelineRuns(): Promise<PipelineRun[]> {
  return apiFetch<PipelineRun[]>('/api/v1/pipeline/runs')
}

export async function fetchQuarantine(): Promise<QuarantineItem[]> {
  return apiFetch<QuarantineItem[]>('/api/v1/quarantine')
}

export async function triggerBackfill(days: number, minMagnitude?: number): Promise<BackfillResponse> {
  return apiFetch<BackfillResponse>('/api/v1/backfill/trigger', {
    method: 'POST',
    body: JSON.stringify({
      days,
      min_magnitude: minMagnitude
    })
  })
}
