import { reactive } from 'vue'
import { API_BASE_URL } from './api'

export interface RealtimeEarthquake {
  id: string
  mag: number | null
  place: string | null
  region: string | null
  time_utc: string | null
  longitude: number
  latitude: number
  depth_km: number | null
}

export type ConnectionStatus = 'connected' | 'reconnecting' | 'disconnected'

export const sseState = reactive({
  status: 'disconnected' as ConnectionStatus,
  lastHeartbeat: null as string | null,
  recentEvents: [] as RealtimeEarthquake[],
  error: null as string | null,
})

let eventSource: EventSource | null = null
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null
let retryDelay = 2000

export function initSSE() {
  if (eventSource) return

  const streamUrl = `${API_BASE_URL}/api/v1/events/stream`
  sseState.status = 'reconnecting'

  try {
    eventSource = new EventSource(streamUrl)

    eventSource.onopen = () => {
      sseState.status = 'connected'
      sseState.error = null
      retryDelay = 2000
    }

    eventSource.addEventListener('heartbeat', (e: MessageEvent) => {
      try {
        const payload = JSON.parse(e.data)
        sseState.lastHeartbeat = payload.timestamp || new Date().toISOString()
        sseState.status = 'connected'
      } catch {
        // Ignorar
      }
    })

    eventSource.addEventListener('earthquake', (e: MessageEvent) => {
      try {
        const earthquake: RealtimeEarthquake = JSON.parse(e.data)
        // Evitar duplicados recientes
        if (!sseState.recentEvents.some(item => item.id === earthquake.id)) {
          sseState.recentEvents.unshift(earthquake)
          if (sseState.recentEvents.length > 8) {
            sseState.recentEvents.pop()
          }
        }
      } catch {
        // Ignorar
      }
    })

    eventSource.onerror = () => {
      sseState.status = 'reconnecting'
      closeSSE()
      // Reintento con backoff exponencial
      reconnectTimeout = setTimeout(() => {
        retryDelay = Math.min(retryDelay * 1.5, 15000)
        initSSE()
      }, retryDelay)
    }
  } catch (err: any) {
    sseState.status = 'disconnected'
    sseState.error = err.message || 'Error al iniciar SSE'
  }
}

export function closeSSE() {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout)
    reconnectTimeout = null
  }
  if (eventSource) {
    eventSource.close()
    eventSource = null
  }
}
