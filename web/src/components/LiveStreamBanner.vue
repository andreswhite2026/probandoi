<script setup lang="ts">
import { sseState } from '../services/sse'
import { Radio } from 'lucide-vue-next'

const getMagBadgeClass = (mag: number | null): string => {
  if (mag === null) return 'bg-slate-700 text-slate-300'
  if (mag >= 6.5) return 'bg-rose-500 text-white'
  if (mag >= 5.0) return 'bg-orange-500 text-white'
  if (mag >= 4.0) return 'bg-yellow-500 text-slate-950 font-bold'
  return 'bg-emerald-500 text-white'
}
</script>

<template>
  <div v-if="sseState.recentEvents.length > 0" class="bg-slate-900/90 border border-slate-800 rounded-xl p-3 shadow-md mb-6">
    <div class="flex items-center justify-between mb-2">
      <div class="flex items-center space-x-2 text-xs font-semibold text-slate-300 uppercase tracking-wider">
        <Radio class="w-4 h-4 text-emerald-400 animate-pulse" />
        <span>Transmisión en Vivo (SSE) &bull; Últimos Sismos Emitidos</span>
      </div>
      <span class="text-[11px] text-slate-500 font-mono">
        Latido: {{ sseState.lastHeartbeat ? sseState.lastHeartbeat.slice(11, 19) + ' UTC' : 'Sincronizando...' }}
      </span>
    </div>

    <!-- Ticker horizontal con chips de sismos en tiempo real -->
    <div class="flex items-center space-x-3 overflow-x-auto pb-1 scrollbar-thin">
      <div
        v-for="ev in sseState.recentEvents"
        :key="ev.id"
        class="flex-shrink-0 flex items-center space-x-2.5 bg-slate-800/80 border border-slate-700/80 rounded-lg px-3 py-1.5 text-xs shadow-sm"
      >
        <span
          class="px-1.5 py-0.5 rounded text-[11px] font-mono font-bold"
          :class="getMagBadgeClass(ev.mag)"
        >
          M{{ ev.mag !== null ? ev.mag.toFixed(1) : '?' }}
        </span>
        <div class="flex flex-col">
          <span class="font-medium text-slate-200 truncate max-w-[180px]" :title="ev.place || ''">
            {{ ev.place || 'Sismo detectado' }}
          </span>
          <span class="text-[10px] text-slate-400 font-mono">
            {{ ev.time_utc ? ev.time_utc.slice(11, 19) + ' UTC' : '' }} &bull; {{ ev.region || 'Región USGS' }}
          </span>
        </div>
      </div>
    </div>
  </div>
</template>
