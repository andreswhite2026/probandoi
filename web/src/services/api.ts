import { useAuth } from './auth'

export const API_BASE_URL = (import.meta as any).env?.VITE_API_BASE_URL || 'http://localhost:8000'

export class ApiError extends Error {
  status: number
  detail: string

  constructor(status: number, detail: string) {
    super(`Error ${status}: ${detail}`)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
  }
}

export async function apiFetch<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const { state, logout } = useAuth()
  const headers = new Headers(options.headers || {})

  if (!headers.has('Content-Type') && !(options.body instanceof FormData)) {
    headers.set('Content-Type', 'application/json')
  }

  if (state.token) {
    headers.set('Authorization', `Bearer ${state.token}`)
  }

  const url = endpoint.startsWith('http') ? endpoint : `${API_BASE_URL}${endpoint}`

  const response = await fetch(url, {
    ...options,
    headers,
  })

  if (response.status === 401) {
    logout()
    // Si no estamos en la página de login, redirigir
    if (!window.location.pathname.includes('/login')) {
      window.location.href = '/login'
    }
    throw new ApiError(401, 'Sesión expirada o no autorizada')
  }

  if (!response.ok) {
    let errorDetail = `Error en el servidor (${response.statusText})`
    try {
      const data = await response.json()
      errorDetail = data.detail || errorDetail
    } catch {
      // Ignorar error al parsear JSON
    }
    throw new ApiError(response.status, errorDetail)
  }

  // Si es un archivo (blob o texto)
  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('text/csv')) {
    return (await response.text()) as unknown as T
  }

  return (await response.json()) as T
}
