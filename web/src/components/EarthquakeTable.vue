<script setup lang="ts">
import type { Earthquake } from '../services/earthquakes'
import { ExternalLink, Waves } from 'lucide-vue-next'

defineProps<{
  earthquakes: Earthquake[]
  total: number
  page: number
  limit: number
  loading: boolean
}>()

const emit = defineEmits<{
  (e: 'changePage', newPage: number): void
}>()

const getMagBadgeClass = (mag: number | null): string => {
  if (mag === null) return 'bg-slate-800 text-slate-300 border-slate-700'
  if (mag >= 6.5) return 'bg-rose-950/80 text-rose-300 border-rose-700'
  if (mag >= 5.0) return 'bg-orange-950/80 text-orange-300 border-orange-700'
  if (mag >= 4.0) return 'bg-amber-950/80 text-amber-300 border-amber-700'
  return 'bg-emerald-950/80 text-emerald-300 border-emerald-700'
}
</script>

<template>
  <div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
    <div class="overflow-x-auto">
      <table class="w-full text-left text-sm text-slate-300">
        <thead class="bg-slate-800/80 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-700">
          <tr>
            <th class="py-3 px-4">Magnitud</th>
            <th class="py-3 px-4">Lugar</th>
            <th class="py-3 px-4">Región</th>
            <th class="py-3 px-4">Fecha y Hora UTC</th>
            <th class="py-3 px-4 text-right">Prof.</th>
            <th class="py-3 px-4 text-center">Tsunami</th>
            <th class="py-3 px-4 text-right">Detalle</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-800">
          <tr v-if="loading">
            <td colspan="7" class="py-8 text-center text-slate-400">
              <div class="inline-flex items-center space-x-2">
                <span class="w-4 h-4 border-2 border-sky-400 border-t-transparent rounded-full animate-spin"></span>
                <span>Cargando eventos sísmicos...</span>
              </div>
            </td>
          </tr>
          <tr v-else-if="earthquakes.length === 0">
            <td colspan="7" class="py-8 text-center text-slate-500">
              No se encontraron terremotos coincidentes con los filtros aplicados.
            </td>
          </tr>
          <tr
            v-for="eq in earthquakes"
            :key="eq.id"
            class="hover:bg-slate-800/50 transition-colors"
          >
            <!-- Magnitud -->
            <td class="py-3 px-4 font-mono font-bold">
              <span
                class="inline-block px-2 py-0.5 rounded border text-xs text-center min-w-[48px]"
                :class="getMagBadgeClass(eq.mag)"
              >
                {{ eq.mag !== null ? eq.mag.toFixed(1) : 'N/A' }}
              </span>
            </td>

            <!-- Lugar -->
            <td class="py-3 px-4 font-medium text-slate-200 max-w-xs truncate" :title="eq.place || ''">
              {{ eq.place || 'Sin ubicación' }}
            </td>

            <!-- Región -->
            <td class="py-3 px-4 text-slate-300">
              <span class="inline-block px-2 py-0.5 rounded bg-slate-800 text-xs border border-slate-700">
                {{ eq.region || 'Desconocida' }}
              </span>
            </td>

            <!-- Fecha -->
            <td class="py-3 px-4 font-mono text-xs text-slate-400">
              {{ eq.time_utc ? eq.time_utc.slice(0, 19).replace('T', ' ') : 'N/A' }}
            </td>

            <!-- Profundidad -->
            <td class="py-3 px-4 text-right font-mono text-xs text-slate-300">
              {{ eq.depth_km !== null ? eq.depth_km.toFixed(1) + ' km' : 'N/A' }}
            </td>

            <!-- Alerta Tsunami -->
            <td class="py-3 px-4 text-center">
              <span
                v-if="eq.tsunami"
                class="inline-flex items-center space-x-1 px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-300 border border-rose-500/30 text-xs font-semibold"
              >
                <Waves class="w-3 h-3" />
                <span>Alerta</span>
              </span>
              <span v-else class="text-slate-600 text-xs">&mdash;</span>
            </td>

            <!-- Enlace USGS -->
            <td class="py-3 px-4 text-right">
              <a
                v-if="eq.url"
                :href="eq.url"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center space-x-1 text-sky-400 hover:text-sky-300 text-xs transition-colors"
              >
                <span>USGS</span>
                <ExternalLink class="w-3 h-3" />
              </a>
              <span v-else class="text-slate-600">&mdash;</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Paginación -->
    <div class="px-4 py-3 bg-slate-800/40 border-t border-slate-800 flex items-center justify-between text-xs text-slate-400">
      <div>
        Mostrando <span class="font-semibold text-slate-200">{{ earthquakes.length }}</span> de <span class="font-semibold text-slate-200">{{ total }}</span> eventos
      </div>
      <div class="flex items-center space-x-2">
        <button
          @click="emit('changePage', page - 1)"
          :disabled="page <= 1 || loading"
          class="px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed text-slate-200"
        >
          Anterior
        </button>
        <span class="px-2 font-mono text-slate-300">Página {{ page }}</span>
        <button
          @click="emit('changePage', page + 1)"
          :disabled="page * limit >= total || loading"
          class="px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed text-slate-200"
        >
          Siguiente
        </button>
      </div>
    </div>
  </div>
</template>
