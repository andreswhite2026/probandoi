import { apiFetch } from './api'

export interface AIReportSummary {
  report_id: string
  reference_date: string
  status: string
  provider: string
  model: string
  generated_at: string
  summary_preview: string
}

export interface AIReportDetail {
  report_id: string
  reference_date: string
  status: string
  provider: string
  model: string
  summary_md: string
  indicators: Record<string, any>
  anomalies: Array<Record<string, any>>
  generated_at: string
  latency_ms: number | null
  error_detail: string | null
}

export async function fetchReports(): Promise<AIReportSummary[]> {
  return apiFetch<AIReportSummary[]>('/api/v1/reports')
}

export async function fetchReportByDate(dateStr: string): Promise<AIReportDetail> {
  return apiFetch<AIReportDetail>(`/api/v1/reports/${dateStr}`)
}
