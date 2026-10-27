<script setup lang="ts">
import { computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useAuth } from '../services/auth'
import { sseState } from '../services/sse'
import { Activity, FileText, Settings2, LogOut } from 'lucide-vue-next'

const router = useRouter()
const route = useRoute()
const { user, isAdmin, logout } = useAuth()

const navItems = [
  { name: 'Monitoreo Sísmico', path: '/monitor', icon: Activity },
  { name: 'Reportes e IA', path: '/reports', icon: FileText },
  { name: 'Operaciones y Calidad', path: '/operations', icon: Settings2 },
]

const sseStatusColor = computed(() => {
  if (sseState.status === 'connected') return 'bg-emerald-500'
  if (sseState.status === 'reconnecting') return 'bg-amber-500'
  return 'bg-rose-500'
})

const sseStatusLabel = computed(() => {
  if (sseState.status === 'connected') return 'SSE En Vivo'
  if (sseState.status === 'reconnecting') return 'SSE Reconectando...'
  return 'SSE Desconectado'
})

const handleLogout = () => {
  logout()
  router.push('/login')
}
</script>

<template>
  <header class="bg-slate-900 border-b border-slate-800 sticky top-0 z-50">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="flex items-center justify-between h-16">
        <!-- Logo y navegación -->
        <div class="flex items-center space-x-8">
          <div class="flex items-center space-x-3">
            <div class="w-9 h-9 rounded-lg bg-sky-500/20 border border-sky-500/40 flex items-center justify-center text-sky-400">
              <Activity class="w-5 h-5 animate-pulse" />
            </div>
            <div>
              <span class="text-lg font-bold text-white tracking-tight">Sismo<span class="text-sky-400">Watch</span></span>
              <span class="hidden sm:inline-block ml-2 text-xs text-slate-400 font-mono">Medallion Gold</span>
            </div>
          </div>

          <!-- Tabs de navegación -->
          <nav class="hidden md:flex space-x-1">
            <router-link
              v-for="item in navItems"
              :key="item.path"
              :to="item.path"
              class="flex items-center space-x-2 px-3 py-2 rounded-md text-sm font-medium transition-colors"
              :class="route.path === item.path
                ? 'bg-slate-800 text-sky-400 border border-slate-700'
                : 'text-slate-300 hover:text-white hover:bg-slate-800/60'"
            >
              <component :is="item.icon" class="w-4 h-4" />
              <span>{{ item.name }}</span>
            </router-link>
          </nav>
        </div>

        <!-- Indicador SSE y Perfil -->
        <div class="flex items-center space-x-4">
          <!-- SSE Badge -->
          <div class="flex items-center space-x-2 px-2.5 py-1 rounded-full bg-slate-800/80 border border-slate-700/80 text-xs">
            <span class="relative flex h-2 w-2">
              <span v-if="sseState.status === 'connected'" class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span class="relative inline-flex rounded-full h-2 w-2" :class="sseStatusColor"></span>
            </span>
            <span class="text-slate-300 font-mono">{{ sseStatusLabel }}</span>
          </div>

          <!-- Usuario y Rol -->
          <div class="flex items-center space-x-2 text-xs">
            <span class="text-slate-300 hidden lg:inline">{{ user.email }}</span>
            <span
              class="px-2 py-0.5 rounded font-semibold uppercase tracking-wider"
              :class="isAdmin ? 'bg-purple-950/80 text-purple-300 border border-purple-700/50' : 'bg-sky-950/80 text-sky-300 border border-sky-700/50'"
            >
              {{ user.role }}
            </span>
          </div>

          <!-- Botón Salir -->
          <button
            @click="handleLogout"
            class="text-slate-400 hover:text-rose-400 transition-colors p-1.5 rounded-lg hover:bg-slate-800"
            title="Cerrar sesión"
          >
            <LogOut class="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  </header>
</template>
