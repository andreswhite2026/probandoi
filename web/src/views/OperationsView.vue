<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useAuth } from '../services/auth'
import {
  fetchQualityRuns,
  fetchQualityRules,
  fetchPipelineRuns,
  fetchQuarantine,
  triggerBackfill,
  type QualityRun,
  type QualityRule,
  type PipelineRun,
  type QuarantineItem,
} from '../services/operations'
import {
  ShieldCheck,
  Play,
  RotateCcw,
  CheckCircle,
  XCircle,
  AlertOctagon,
  Clock,
  Lock,
} from 'lucide-vue-next'

const { isAdmin } = useAuth()

const activeTab = ref<'pipeline' | 'quality' | 'quarantine' | 'backfill'>('pipeline')

// Estados de datos
const pipelineRuns = ref<PipelineRun[]>([])
const qualityRuns = ref<QualityRun[]>([])
const qualityRules = ref<QualityRule[]>([])
const quarantineItems = ref<QuarantineItem[]>([])
const loading = ref(false)
const errorMsg = ref('')

// Formulario de Backfill (Solo Admin)
const backfillDays = ref(30)
const backfillMinMag = ref<number | undefined>(4.5)
const backfillLoading = ref(false)
const backfillSuccessMsg = ref('')
const backfillErrorMsg = ref('')

const loadOperationsData = async () => {
  loading.value = true
  errorMsg.value = ''
  try {
    const [pRuns, qRuns, qRules] = await Promise.all([
      fetchPipelineRuns(),
      fetchQualityRuns(),
      fetchQualityRules(),
    ])
    pipelineRuns.value = pRuns
    qualityRuns.value = qRuns
    qualityRules.value = qRules

    // Solo consultar cuarentena si es admin
    if (isAdmin.value) {
      try {
        quarantineItems.value = await fetchQuarantine()
      } catch (err: any) {
        console.warn('Error consultando cuarentena:', err)
      }
    }
  } catch (err: any) {
    errorMsg.value = 'Error cargando datos de operaciones: ' + (err.detail || err.message)
  } finally {
    loading.value = false
  }
}

const handleTriggerBackfill = async () => {
  if (!isAdmin.value) return
  backfillLoading.value = true
  backfillSuccessMsg.value = ''
  backfillErrorMsg.value = ''

  try {
    const resp = await triggerBackfill(backfillDays.value, backfillMinMag.value)
    backfillSuccessMsg.value = `${resp.message} (DAG Run ID: ${resp.dag_run_id || 'manual'})`
    // Recargar estado de pipelines
    setTimeout(() => {
      loadOperationsData()
    }, 2000)
  } catch (err: any) {
    backfillErrorMsg.value = err.detail || 'Error al disparar el backfill en Airflow'
  } finally {
    backfillLoading.value = false
  }
}

onMounted(() => {
  loadOperationsData()
})
</script>

<template>
  <div class="space-y-6">
    <!-- Encabezado de la Vista -->
    <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
      <div>
        <h1 class="text-2xl font-bold tracking-tight text-white flex items-center space-x-2">
          <span>Operaciones del Pipeline y Calidad de Datos</span>
          <span class="text-xs font-mono font-normal px-2.5 py-0.5 rounded-full bg-slate-800 text-sky-400 border border-slate-700">
            Airflow &bull; Spark &bull; PostgreSQL
          </span>
        </h1>
        <p class="text-xs text-slate-400 mt-1">
          Supervisión de DAGs, compuertas de calidad declarativas (R01-R05), cuarentena y orquestación de backfills históricos.
        </p>
      </div>

      <button
        @click="loadOperationsData"
        :disabled="loading"
        class="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-medium transition-colors"
      >
        <RotateCcw class="w-3.5 h-3.5" :class="{ 'animate-spin': loading }" />
        <span>Actualizar Estado</span>
      </button>
    </div>

    <!-- Navegación por pestañas de operaciones -->
    <div class="flex space-x-2 border-b border-slate-800 pb-2">
      <button
        @click="activeTab = 'pipeline'"
        class="flex items-center space-x-2 px-3 py-2 rounded-lg text-xs font-semibold transition-colors"
        :class="activeTab === 'pipeline' ? 'bg-sky-600 text-white' : 'text-slate-400 hover:text-white hover:bg-slate-800'"
      >
        <Clock class="w-3.5 h-3.5" />
        <span>Estado de DAGs (Pipeline)</span>
      </button>

      <button
        @click="activeTab = 'quality'"
        class="flex items-center space-x-2 px-3 py-2 rounded-lg text-xs font-semibold transition-colors"
        :class="activeTab === 'quality' ? 'bg-sky-600 text-white' : 'text-slate-400 hover:text-white hover:bg-slate-800'"
      >
        <ShieldCheck class="w-3.5 h-3.5" />
        <span>Reglas y Calidad (R01-R05)</span>
      </button>

      <button
        @click="activeTab = 'quarantine'"
        class="flex items-center space-x-2 px-3 py-2 rounded-lg text-xs font-semibold transition-colors"
        :class="activeTab === 'quarantine' ? 'bg-sky-600 text-white' : 'text-slate-400 hover:text-white hover:bg-slate-800'"
      >
        <AlertOctagon class="w-3.5 h-3.5" />
        <span>Cuarentena</span>
        <span v-if="!isAdmin" class="ml-1 text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 border border-slate-700">Admin</span>
      </button>

      <button
        @click="activeTab = 'backfill'"
        class="flex items-center space-x-2 px-3 py-2 rounded-lg text-xs font-semibold transition-colors"
        :class="activeTab === 'backfill' ? 'bg-sky-600 text-white' : 'text-slate-400 hover:text-white hover:bg-slate-800'"
      >
        <Play class="w-3.5 h-3.5" />
        <span>Disparar Backfill</span>
        <span v-if="!isAdmin" class="ml-1 text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 border border-slate-700">Admin</span>
      </button>
    </div>

    <!-- Pestaña 1: Pipeline Runs -->
    <div v-if="activeTab === 'pipeline'" class="space-y-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
        <table class="w-full text-left text-sm text-slate-300">
          <thead class="bg-slate-800/80 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-700">
            <tr>
              <th class="py-3 px-4">DAG Orquestador</th>
              <th class="py-3 px-4">Run ID</th>
              <th class="py-3 px-4 text-center">Estado</th>
              <th class="py-3 px-4">Iniciado UTC</th>
              <th class="py-3 px-4">Finalizado UTC</th>
              <th class="py-3 px-4">Nota / Registro</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-800">
            <tr v-if="pipelineRuns.length === 0">
              <td colspan="6" class="py-8 text-center text-slate-500">
                No hay ejecuciones registradas en curated.pipeline_runs.
              </td>
            </tr>
            <tr v-for="r in pipelineRuns" :key="r.dag_id + r.run_id" class="hover:bg-slate-800/40">
              <td class="py-3 px-4 font-mono font-semibold text-sky-400 text-xs">{{ r.dag_id }}</td>
              <td class="py-3 px-4 font-mono text-xs text-slate-300">{{ r.run_id }}</td>
              <td class="py-3 px-4 text-center">
                <span
                  class="px-2 py-0.5 rounded text-[11px] font-bold uppercase tracking-wider"
                  :class="r.state === 'success' ? 'bg-emerald-950 text-emerald-300 border border-emerald-800' : 'bg-amber-950 text-amber-300 border border-amber-800'"
                >
                  {{ r.state }}
                </span>
              </td>
              <td class="py-3 px-4 font-mono text-xs text-slate-400">{{ r.started_at ? r.started_at.slice(0, 19).replace('T', ' ') : 'N/A' }}</td>
              <td class="py-3 px-4 font-mono text-xs text-slate-400">{{ r.finished_at ? r.finished_at.slice(0, 19).replace('T', ' ') : 'N/A' }}</td>
              <td class="py-3 px-4 text-xs text-slate-400">{{ r.note || '&mdash;' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Pestaña 2: Calidad de Datos (Quality Gate R01-R05) -->
    <div v-if="activeTab === 'quality'" class="space-y-6">
      <!-- Ejecuciones Agregadas de Calidad -->
      <div>
        <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">Historial de Quality Gate (curated.quality_runs)</h3>
        <div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
          <table class="w-full text-left text-sm text-slate-300">
            <thead class="bg-slate-800/80 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-700">
              <tr>
                <th class="py-3 px-4">Capa</th>
                <th class="py-3 px-4 text-right">Evaluados</th>
                <th class="py-3 px-4 text-right">Aprobados</th>
                <th class="py-3 px-4 text-right">Rechazados</th>
                <th class="py-3 px-4 text-center">Score de Calidad</th>
                <th class="py-3 px-4 text-center">Resultado</th>
                <th class="py-3 px-4">Fecha Ejecución</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800">
              <tr v-for="q in qualityRuns" :key="q.run_id" class="hover:bg-slate-800/40">
                <td class="py-3 px-4 font-mono text-xs text-slate-300">{{ q.layer }}</td>
                <td class="py-3 px-4 text-right font-mono text-xs">{{ q.evaluated }}</td>
                <td class="py-3 px-4 text-right font-mono text-xs text-emerald-400">{{ q.approved }}</td>
                <td class="py-3 px-4 text-right font-mono text-xs text-rose-400">{{ q.rejected }}</td>
                <td class="py-3 px-4 text-center font-mono font-bold" :class="q.score && q.score >= 80 ? 'text-emerald-400' : 'text-amber-400'">
                  {{ q.score !== null ? q.score + '%' : 'N/A' }}
                </td>
                <td class="py-3 px-4 text-center">
                  <span
                    class="px-2 py-0.5 rounded text-[11px] font-bold uppercase tracking-wider"
                    :class="q.status === 'passed' ? 'bg-emerald-950 text-emerald-300 border border-emerald-800' : 'bg-rose-950 text-rose-300 border border-rose-800'"
                  >
                    {{ q.status }}
                  </span>
                </td>
                <td class="py-3 px-4 font-mono text-xs text-slate-400">{{ q.started_at ? q.started_at.slice(0, 19).replace('T', ' ') : 'N/A' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Detalle por Reglas R01-R05 -->
      <div>
        <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">Detalle de Reglas Evaluadas (curated.quality_rules)</h3>
        <div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
          <table class="w-full text-left text-sm text-slate-300">
            <thead class="bg-slate-800/80 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-700">
              <tr>
                <th class="py-3 px-4">Código</th>
                <th class="py-3 px-4">Descripción de la Regla</th>
                <th class="py-3 px-4 text-center">Severidad</th>
                <th class="py-3 px-4 text-right">Evaluados</th>
                <th class="py-3 px-4 text-right">Fallidos</th>
                <th class="py-3 px-4 text-center">Estado</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800">
              <tr v-for="r in qualityRules" :key="r.id" class="hover:bg-slate-800/40">
                <td class="py-3 px-4 font-mono font-bold text-sky-400 text-xs">{{ r.rule_code }}</td>
                <td class="py-3 px-4 text-xs text-slate-200">{{ r.description }}</td>
                <td class="py-3 px-4 text-center">
                  <span
                    class="px-2 py-0.5 rounded text-[10px] uppercase font-bold"
                    :class="r.severity === 'blocking' ? 'bg-rose-950 text-rose-300 border border-rose-800' : 'bg-amber-950 text-amber-300 border border-amber-800'"
                  >
                    {{ r.severity }}
                  </span>
                </td>
                <td class="py-3 px-4 text-right font-mono text-xs">{{ r.evaluated }}</td>
                <td class="py-3 px-4 text-right font-mono text-xs" :class="r.failed > 0 ? 'text-rose-400 font-bold' : 'text-slate-500'">{{ r.failed }}</td>
                <td class="py-3 px-4 text-center">
                  <CheckCircle v-if="r.passed" class="w-4 h-4 text-emerald-400 mx-auto" />
                  <XCircle v-else class="w-4 h-4 text-rose-400 mx-auto" />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Pestaña 3: Cuarentena (Solo Admin) -->
    <div v-if="activeTab === 'quarantine'" class="space-y-4">
      <!-- Banner de restricción para rol Viewer -->
      <div v-if="!isAdmin" class="bg-amber-500/10 border border-amber-500/30 rounded-xl p-8 text-center space-y-3">
        <Lock class="w-8 h-8 text-amber-400 mx-auto" />
        <h3 class="text-base font-bold text-white">Acceso Restringido por RBAC (Rol Viewer)</h3>
        <p class="text-xs text-slate-400 max-w-md mx-auto">
          El endpoint <code class="text-sky-300 bg-slate-900 px-1.5 py-0.5 rounded">/api/v1/quarantine</code> está protegido en el backend con HTTP 403 Forbidden. Solo los usuarios con rol <strong class="text-purple-300">Admin</strong> pueden inspeccionar los registros descartados.
        </p>
      </div>

      <!-- Tabla de cuarentena para Admin -->
      <div v-else class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
        <div class="p-4 border-b border-slate-800 flex items-center justify-between">
          <span class="text-xs font-semibold text-slate-300 uppercase tracking-wider">
            Registros Rechazados en Cuarentena ({{ quarantineItems.length }})
          </span>
          <span class="text-xs font-mono text-emerald-400">Autorizado como Admin</span>
        </div>

        <table class="w-full text-left text-sm text-slate-300">
          <thead class="bg-slate-800/80 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-700">
            <tr>
              <th class="py-3 px-4">ID Evento</th>
              <th class="py-3 px-4">Motivo de Rechazo</th>
              <th class="py-3 px-4">Detalle Técnico</th>
              <th class="py-3 px-4">Fecha Rechazo UTC</th>
              <th class="py-3 px-4">Payload Preservado</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-800">
            <tr v-if="quarantineItems.length === 0">
              <td colspan="5" class="py-8 text-center text-slate-500">
                La cuarentena está vacía. Todos los eventos han superado las reglas de calidad.
              </td>
            </tr>
            <tr v-for="item in quarantineItems" :key="item.id" class="hover:bg-slate-800/40">
              <td class="py-3 px-4 font-mono text-xs text-slate-300">{{ item.event_id || 'Sin ID' }}</td>
              <td class="py-3 px-4">
                <span class="px-2 py-0.5 rounded text-[10px] font-mono bg-rose-950 text-rose-300 border border-rose-800">
                  {{ item.reason_code }}
                </span>
              </td>
              <td class="py-3 px-4 text-xs text-slate-300">{{ item.reason_detail || '&mdash;' }}</td>
              <td class="py-3 px-4 font-mono text-xs text-slate-400">{{ item.created_at ? item.created_at.slice(0, 19).replace('T', ' ') : 'N/A' }}</td>
              <td class="py-3 px-4 text-xs font-mono text-slate-400 max-w-xs truncate" :title="JSON.stringify(item.payload)">
                {{ JSON.stringify(item.payload) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Pestaña 4: Disparo de Backfill (Solo Admin) -->
    <div v-if="activeTab === 'backfill'" class="space-y-4">
      <!-- Banner de restricción para rol Viewer -->
      <div v-if="!isAdmin" class="bg-amber-500/10 border border-amber-500/30 rounded-xl p-8 text-center space-y-3">
        <Lock class="w-8 h-8 text-amber-400 mx-auto" />
        <h3 class="text-base font-bold text-white">Acción Protegida por RBAC (Rol Viewer)</h3>
        <p class="text-xs text-slate-400 max-w-md mx-auto">
          El endpoint <code class="text-sky-300 bg-slate-900 px-1.5 py-0.5 rounded">/api/v1/backfill/trigger</code> rechaza con 403 Forbidden a los usuarios Viewer. Solo el rol <strong class="text-purple-300">Admin</strong> puede disparar la orquestación en Airflow.
        </p>
      </div>

      <!-- Formulario para Admin -->
      <div v-else class="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-sm max-w-2xl space-y-6">
        <div>
          <h2 class="text-base font-bold text-white flex items-center space-x-2">
            <Play class="w-4 h-4 text-sky-400" />
            <span>Disparar Backfill Histórico Parametrizado (Airflow)</span>
          </h2>
          <p class="text-xs text-slate-400 mt-1">
            Ejecuta el DAG <code class="text-sky-300">backfill_history</code> invocando la API REST de Apache Airflow con paso estricto de parámetros.
          </p>
        </div>

        <div v-if="backfillSuccessMsg" class="p-3 bg-emerald-500/10 border border-emerald-500/30 rounded-lg text-emerald-300 text-xs">
          {{ backfillSuccessMsg }}
        </div>
        <div v-if="backfillErrorMsg" class="p-3 bg-rose-500/10 border border-rose-500/30 rounded-lg text-rose-300 text-xs">
          {{ backfillErrorMsg }}
        </div>

        <form @submit.prevent="handleTriggerBackfill" class="space-y-4">
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label class="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">
                Días a Extraer (Ventana hacia el pasado)
              </label>
              <input
                v-model.number="backfillDays"
                type="number"
                min="1"
                max="365"
                required
                class="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="text-[11px] text-slate-500 mt-1 block">Por defecto: 30 días</span>
            </div>

            <div>
              <label class="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">
                Magnitud Mínima (Filtro USGS)
              </label>
              <input
                v-model.number="backfillMinMag"
                type="number"
                step="0.1"
                min="-2"
                max="10"
                class="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="text-[11px] text-slate-500 mt-1 block">Por defecto: 4.5</span>
            </div>
          </div>

          <div class="pt-2">
            <button
              type="submit"
              :disabled="backfillLoading"
              class="flex items-center space-x-2 px-5 py-2.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md transition-colors disabled:opacity-50"
            >
              <span v-if="backfillLoading" class="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin"></span>
              <Play v-else class="w-3.5 h-3.5" />
              <span>{{ backfillLoading ? 'Disparando en Airflow...' : 'Ejecutar Backfill en Airflow' }}</span>
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>
