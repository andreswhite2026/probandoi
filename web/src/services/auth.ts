import { reactive, computed } from 'vue'

export interface UserState {
  token: string | null
  email: string | null
  role: 'admin' | 'viewer' | null
}

const TOKEN_KEY = 'sismowatch_jwt'
const EMAIL_KEY = 'sismowatch_email'
const ROLE_KEY = 'sismowatch_role'

const state = reactive<UserState>({
  token: localStorage.getItem(TOKEN_KEY),
  email: localStorage.getItem(EMAIL_KEY),
  role: (localStorage.getItem(ROLE_KEY) as 'admin' | 'viewer') || null,
})

export const useAuth = () => {
  const isAuthenticated = computed(() => !!state.token)
  const isAdmin = computed(() => state.role === 'admin')
  const user = computed(() => state)

  const setAuth = (token: string, email: string, role: 'admin' | 'viewer') => {
    state.token = token
    state.email = email
    state.role = role

    localStorage.setItem(TOKEN_KEY, token)
    localStorage.setItem(EMAIL_KEY, email)
    localStorage.setItem(ROLE_KEY, role)
  }

  const logout = () => {
    state.token = null
    state.email = null
    state.role = null

    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem(EMAIL_KEY)
    localStorage.removeItem(ROLE_KEY)
  }

  return {
    state,
    user,
    isAuthenticated,
    isAdmin,
    setAuth,
    logout,
  }
}
