<script setup lang="ts">
import { ref, onMounted } from 'vue'
import {
  fetchEarthquakes,
  fetchKPIs,
  downloadExport,
  type Earthquake,
  type KPIs,
} from '../services/earthquakes'
import { initSSE } from '../services/sse'
import KpiCard from '../components/KpiCard.vue'
import SeismicMap from '../components/SeismicMap.vue'
import EarthquakeTable from '../components/EarthquakeTable.vue'
import LiveStreamBanner from '../components/LiveStreamBanner.vue'
import {
  Activity,
  Calendar,
  Zap,
  TrendingUp,
  Waves,
  AlertTriangle,
  Download,
  Filter,
  RefreshCw,
  MapPin,
} from 'lucide-vue-next'

const kpis = ref<KPIs>({
  total_24h: 0,
  total_7d: 0,
  max_mag_24h: null,
  avg_mag_24h: null,
  tsunami_alerts_24h: 0,
  active_anomalies_count: 0,
  top_region_24h: null,
})

const earthquakes = ref<Earthquake[]>([])
const total = ref(0)
const page = ref(1)
const limit = ref(50)
const loading = ref(false)
const exporting = ref(false)

// Filtros
const minMagFilter = ref<number | undefined>(undefined)
const regionFilter = ref<string>('')

const loadData = async () => {
  loading.value = true
  try {
    const offset = (page.value - 1) * limit.value
    const [kpisRes, eqRes] = await Promise.all([
      fetchKPIs(),
      fetchEarthquakes({
        limit: limit.value,
        offset,
        min_mag: minMagFilter.value,
        region: regionFilter.value || undefined,
      }),
    ])

    kpis.value = kpisRes
    earthquakes.value = eqRes.items
    total.value = eqRes.total
  } catch (err) {
    console.error('Error al cargar datos sísmicos:', err)
  } finally {
    loading.value = false
  }
}

const handlePageChange = (newPage: number) => {
  page.value = newPage
  loadData()
}

const handleFilter = () => {
  page.value = 1
  loadData()
}

const handleResetFilters = () => {
  minMagFilter.value = undefined
  regionFilter.value = ''
  page.value = 1
  loadData()
}

const handleExport = async (format: 'csv' | 'geojson') => {
  exporting.value = true
  try {
    await downloadExport(format, {
      min_mag: minMagFilter.value,
      region: regionFilter.value || undefined,
    })
  } catch (err) {
    console.error('Error exportando datos:', err)
  } finally {
    exporting.value = false
  }
}

onMounted(() => {
  loadData()
  initSSE()
})
</script>

<template>
  <div class="space-y-6">
    <!-- Ticker de Eventos SSE en Tiempo Real -->
    <LiveStreamBanner />

    <!-- Encabezado de la Vista -->
    <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
      <div>
        <h1 class="text-2xl font-bold tracking-tight text-white flex items-center space-x-2">
          <span>Monitoreo Sísmico en Tiempo Real</span>
          <span class="text-xs font-mono font-normal px-2.5 py-0.5 rounded-full bg-slate-800 text-sky-400 border border-slate-700">
            Capa Gold &bull; Curated
          </span>
        </h1>
        <p class="text-xs text-slate-400 mt-1">
          Catálogo curado de eventos sísmicos de USGS procesados con calidad bloqueante e inferencia de anomalías.
        </p>
      </div>

      <!-- Acciones de Exportación y Actualización -->
      <div class="flex items-center space-x-2">
        <button
          @click="loadData"
          :disabled="loading"
          class="flex items-center space-x-1.5 px-3 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-xs font-medium text-slate-200 transition-colors"
          title="Actualizar datos"
        >
          <RefreshCw class="w-3.5 h-3.5" :class="{ 'animate-spin': loading }" />
          <span>Refrescar</span>
        </button>

        <button
          @click="handleExport('csv')"
          :disabled="exporting"
          class="flex items-center space-x-1.5 px-3 py-2 rounded-lg bg-sky-600/90 hover:bg-sky-500 text-white text-xs font-medium shadow-sm transition-colors"
        >
          <Download class="w-3.5 h-3.5" />
          <span>Exportar CSV</span>
        </button>

        <button
          @click="handleExport('geojson')"
          :disabled="exporting"
          class="flex items-center space-x-1.5 px-3 py-2 rounded-lg bg-emerald-600/90 hover:bg-emerald-500 text-white text-xs font-medium shadow-sm transition-colors"
        >
          <Download class="w-3.5 h-3.5" />
          <span>GeoJSON</span>
        </button>
      </div>
    </div>

    <!-- Tarjetas de KPIs -->
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
      <KpiCard
        title="Sismos 24 Horas"
        :value="kpis.total_24h"
        subtitle="Última jornada"
        :icon="Activity"
        color="sky"
      />
      <KpiCard
        title="Total 7 Días"
        :value="kpis.total_7d"
        subtitle="Ventana semanal"
        :icon="Calendar"
        color="purple"
      />
      <KpiCard
        title="Magnitud Máx (24h)"
        :value="kpis.max_mag_24h !== null ? 'M ' + kpis.max_mag_24h.toFixed(1) : 'N/A'"
        subtitle="Mayor impacto"
        :icon="Zap"
        color="rose"
      />
      <KpiCard
        title="Promedio Mag (24h)"
        :value="kpis.avg_mag_24h !== null ? 'M ' + kpis.avg_mag_24h.toFixed(2) : 'N/A'"
        subtitle="Intensidad media"
        :icon="TrendingUp"
        color="amber"
      />
      <KpiCard
        title="Alertas Tsunami"
        :value="kpis.tsunami_alerts_24h"
        subtitle="Eventos en 24h"
        :icon="Waves"
        color="emerald"
      />
      <KpiCard
        title="Anomalías Activas"
        :value="kpis.active_anomalies_count"
        :subtitle="kpis.top_region_24h ? 'Top: ' + kpis.top_region_24h : 'Z-score > 2.0'"
        :icon="AlertTriangle"
        color="rose"
      />
    </div>

    <!-- Mapa Interactivo de Terremotos -->
    <div>
      <div class="flex items-center justify-between mb-2">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
          <MapPin class="w-4 h-4 text-sky-400" />
          <span>Distribución Geoespacial de Terremotos (Leaflet)</span>
        </h2>
        <span class="text-xs text-slate-400">
          Visualizando {{ earthquakes.length }} epicentros cargados
        </span>
      </div>
      <SeismicMap :earthquakes="earthquakes" />
    </div>

    <!-- Barra de Filtros y Búsqueda -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
      <div class="flex flex-wrap items-center gap-3">
        <!-- Filtro Magnitud Mínima -->
        <div class="flex items-center space-x-2">
          <label class="text-xs font-medium text-slate-300">Mag Mínima:</label>
          <select
            v-model="minMagFilter"
            class="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:ring-1 focus:ring-sky-500"
          >
            <option :value="undefined">Todas</option>
            <option :value="2.5">&ge; 2.5</option>
            <option :value="4.0">&ge; 4.0</option>
            <option :value="4.5">&ge; 4.5 (FDSNWS)</option>
            <option :value="5.0">&ge; 5.0 (Fuerte)</option>
            <option :value="6.0">&ge; 6.0 (Severo)</option>
          </select>
        </div>

        <!-- Búsqueda por Región -->
        <div class="flex items-center space-x-2">
          <label class="text-xs font-medium text-slate-300">Región:</label>
          <input
            v-model="regionFilter"
            type="text"
            placeholder="Ej: Alaska, CA, Chile..."
            class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-sky-500 w-44"
            @keyup.enter="handleFilter"
          />
        </div>

        <!-- Botones Filtrar y Limpiar -->
        <button
          @click="handleFilter"
          class="flex items-center space-x-1 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-medium transition-colors"
        >
          <Filter class="w-3 h-3" />
          <span>Filtrar</span>
        </button>

        <button
          v-if="minMagFilter !== undefined || regionFilter"
          @click="handleResetFilters"
          class="text-xs text-slate-400 hover:text-slate-200 underline transition-colors"
        >
          Limpiar filtros
        </button>
      </div>

      <div class="text-xs text-slate-400 font-mono">
        Total en base de datos: <span class="font-bold text-sky-400">{{ total }}</span> eventos curados
      </div>
    </div>

    <!-- Tabla de Terremotos -->
    <div>
      <EarthquakeTable
        :earthquakes="earthquakes"
        :total="total"
        :page="page"
        :limit="limit"
        :loading="loading"
        @change-page="handlePageChange"
      />
    </div>
  </div>
</template>
