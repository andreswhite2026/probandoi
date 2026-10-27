import { apiFetch, API_BASE_URL } from './api'
import { useAuth } from './auth'

export interface Earthquake {
  id: string
  mag: number | null
  mag_type: string | null
  place: string | null
  region: string | null
  time_utc: string
  updated_utc: string | null
  tsunami: boolean
  sig: number | null
  event_status: string | null
  event_type: string | null
  url: string | null
  longitude: number
  latitude: number
  depth_km: number | null
  ingest_run_id: string
  published_at: string
}

export interface EarthquakesListResponse {
  total: number
  limit: number
  offset: number
  items: Earthquake[]
}

export interface KPIs {
  total_24h: number
  total_7d: number
  max_mag_24h: number | null
  avg_mag_24h: number | null
  tsunami_alerts_24h: number
  active_anomalies_count: number
  top_region_24h: string | null
}

export async function fetchEarthquakes(params: {
  limit?: number
  offset?: number
  min_mag?: number
  max_mag?: number
  region?: string
}): Promise<EarthquakesListResponse> {
  const query = new URLSearchParams()
  if (params.limit !== undefined) query.set('limit', String(params.limit))
  if (params.offset !== undefined) query.set('offset', String(params.offset))
  if (params.min_mag !== undefined && params.min_mag !== null) query.set('min_mag', String(params.min_mag))
  if (params.max_mag !== undefined && params.max_mag !== null) query.set('max_mag', String(params.max_mag))
  if (params.region) query.set('region', params.region)

  return apiFetch<EarthquakesListResponse>(`/api/v1/earthquakes?${query.toString()}`)
}

export async function fetchKPIs(): Promise<KPIs> {
  return apiFetch<KPIs>('/api/v1/earthquakes/kpis')
}

export async function downloadExport(format: 'csv' | 'geojson', params: { min_mag?: number; region?: string } = {}) {
  const { state } = useAuth()
  const query = new URLSearchParams({ format, limit: '5000' })
  if (params.min_mag !== undefined && params.min_mag !== null) query.set('min_mag', String(params.min_mag))
  if (params.region) query.set('region', params.region)

  const url = `${API_BASE_URL}/api/v1/earthquakes/export?${query.toString()}`
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${state.token}`
    }
  })

  if (!response.ok) {
    throw new Error('No se pudo descargar la exportación')
  }

  const blob = await response.blob()
  const blobUrl = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = blobUrl
  a.download = `sismowatch_export_${new Date().toISOString().slice(0, 10)}.${format === 'csv' ? 'csv' : 'geojson'}`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(blobUrl)
}
