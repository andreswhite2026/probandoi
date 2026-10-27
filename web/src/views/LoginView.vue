<script setup lang="ts">
import { ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useAuth } from '../services/auth'
import { apiFetch } from '../services/api'
import { Activity, Lock, Mail, ShieldAlert, KeyRound, UserCheck } from 'lucide-vue-next'

const router = useRouter()
const route = useRoute()
const { setAuth } = useAuth()

const email = ref('')
const password = ref('')
const errorMsg = ref('')
const loading = ref(false)

const handleLogin = async () => {
  if (!email.value || !password.value) {
    errorMsg.value = 'Por favor ingresa correo y contraseña'
    return
  }

  errorMsg.value = ''
  loading.value = true

  try {
    const data = await apiFetch<{
      access_token: string
      token_type: string
      role: 'admin' | 'viewer'
      email: string
    }>('/api/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify({
        email: email.value.trim(),
        password: password.value,
      }),
    })

    setAuth(data.access_token, data.email, data.role)
    const redirectPath = (route.query.redirect as string) || '/monitor'
    router.push(redirectPath)
  } catch (err: any) {
    errorMsg.value = err.detail || 'Credenciales inválidas o error de conexión'
  } finally {
    loading.value = false
  }
}

const fillViewer = () => {
  email.value = 'viewer@sismowatch.local'
  password.value = 'viewer123'
  errorMsg.value = ''
}

const fillAdmin = () => {
  email.value = 'admin@sismowatch.local'
  password.value = 'admin123'
  errorMsg.value = ''
}
</script>

<template>
  <div class="min-h-screen bg-slate-950 flex flex-col justify-center py-12 sm:px-6 lg:px-8">
    <div class="sm:mx-auto sm:w-full sm:max-w-md text-center">
      <div class="mx-auto w-12 h-12 rounded-xl bg-sky-500/20 border border-sky-500/40 flex items-center justify-center text-sky-400 mb-4 shadow-lg shadow-sky-500/10">
        <Activity class="w-7 h-7 animate-pulse" />
      </div>
      <h2 class="text-3xl font-extrabold text-white tracking-tight">Sismo<span class="text-sky-400">Watch</span></h2>
      <p class="mt-2 text-sm text-slate-400">
        Plataforma de Monitoreo Sísmico &bull; Medallion Architecture
      </p>
    </div>

    <div class="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
      <div class="bg-slate-900 py-8 px-6 shadow-xl border border-slate-800 sm:rounded-2xl sm:px-10">
        <form class="space-y-6" @submit.prevent="handleLogin">
          <!-- Alerta de error -->
          <div v-if="errorMsg" class="p-3 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs flex items-center space-x-2">
            <ShieldAlert class="w-4 h-4 flex-shrink-0 text-rose-400" />
            <span>{{ errorMsg }}</span>
          </div>

          <!-- Correo -->
          <div>
            <label class="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">Correo Electrónico</label>
            <div class="relative rounded-lg shadow-sm">
              <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-500">
                <Mail class="w-4 h-4" />
              </div>
              <input
                v-model="email"
                type="email"
                required
                class="block w-full pl-10 pr-3 py-2.5 bg-slate-800 border border-slate-700 rounded-lg text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-sky-500 focus:border-transparent transition-all"
                placeholder="usuario@sismowatch.local"
              />
            </div>
          </div>

          <!-- Contraseña -->
          <div>
            <label class="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">Contraseña</label>
            <div class="relative rounded-lg shadow-sm">
              <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-500">
                <Lock class="w-4 h-4" />
              </div>
              <input
                v-model="password"
                type="password"
                required
                class="block w-full pl-10 pr-3 py-2.5 bg-slate-800 border border-slate-700 rounded-lg text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-sky-500 focus:border-transparent transition-all"
                placeholder="&bull;&bull;&bull;&bull;&bull;&bull;&bull;&bull;"
              />
            </div>
          </div>

          <!-- Botón Iniciar Sesión -->
          <div>
            <button
              type="submit"
              :disabled="loading"
              class="w-full flex justify-center items-center py-2.5 px-4 border border-transparent rounded-lg shadow-md text-sm font-semibold text-white bg-sky-600 hover:bg-sky-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-sky-500 disabled:opacity-50 transition-colors"
            >
              <span v-if="loading" class="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2"></span>
              <span>{{ loading ? 'Autenticando...' : 'Iniciar Sesión' }}</span>
            </button>
          </div>
        </form>

        <!-- Accesos rápidos para la sustentación -->
        <div class="mt-6 pt-6 border-t border-slate-800">
          <span class="block text-center text-xs text-slate-400 mb-3 font-semibold uppercase tracking-wider">
            Accesos de Demostración (RBAC)
          </span>
          <div class="grid grid-cols-2 gap-3">
            <button
              type="button"
              @click="fillViewer"
              class="flex items-center justify-center space-x-1.5 py-2 px-3 rounded-lg border border-slate-700 bg-slate-800/80 hover:bg-slate-700 text-xs text-slate-200 transition-colors"
            >
              <UserCheck class="w-3.5 h-3.5 text-sky-400" />
              <span>Rol Viewer</span>
            </button>
            <button
              type="button"
              @click="fillAdmin"
              class="flex items-center justify-center space-x-1.5 py-2 px-3 rounded-lg border border-purple-800/60 bg-purple-950/40 hover:bg-purple-900/60 text-xs text-purple-200 transition-colors"
            >
              <KeyRound class="w-3.5 h-3.5 text-purple-400" />
              <span>Rol Admin</span>
            </button>
          </div>
          <p class="text-[11px] text-slate-400 text-center mt-3">
            Las credenciales seguras son generadas e inyectadas dinámicamente vía variables de entorno.
          </p>
        </div>
      </div>
    </div>
  </div>
</template>
