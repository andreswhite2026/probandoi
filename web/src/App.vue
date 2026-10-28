<script setup lang="ts">
import { computed, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useAuth } from './services/auth'
import { initSSE, closeSSE } from './services/sse'
import Navbar from './components/Navbar.vue'

const route = useRoute()
const { isAuthenticated } = useAuth()
const isLoginPage = computed(() => route.path === '/login')

watch(
  isAuthenticated,
  (authed) => {
    if (authed) {
      initSSE()
    } else {
      closeSSE(true)
    }
  },
  { immediate: true }
)
</script>

<template>
  <div class="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
    <!-- Navbar solo en rutas autenticadas -->
    <Navbar v-if="!isLoginPage" />

    <!-- Contenido principal -->
    <main :class="isLoginPage ? 'flex-1' : 'flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8'">
      <router-view />
    </main>

    <!-- Footer -->
    <footer v-if="!isLoginPage" class="bg-slate-900/60 border-t border-slate-800/80 py-4 text-center text-xs text-slate-400">
      <div class="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-2">
        <span>SismoWatch &bull; Plataforma de Inteligencia Sísmica en Tiempo Real</span>
        <span class="font-mono text-[11px] text-slate-400">Arquitectura Medallion (Bronze / Silver / Gold) &bull; Vue 3 &bull; FastAPI</span>
      </div>
    </footer>
  </div>
</template>
