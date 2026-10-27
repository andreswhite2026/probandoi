<script setup lang="ts">
import { ref, onMounted } from 'vue'
import {
  fetchReports,
  fetchReportByDate,
  type AIReportSummary,
  type AIReportDetail,
} from '../services/reports'
import { askAgent, type AgentQueryResponse } from '../services/agent'
import { downloadExport } from '../services/earthquakes'
import {
  Calendar,
  Bot,
  Send,
  Download,
  CheckCircle2,
} from 'lucide-vue-next'

const reportsList = ref<AIReportSummary[]>([])
const selectedDate = ref<string>('')
const currentReport = ref<AIReportDetail | null>(null)
const loadingReport = ref(false)

// Agente de IA
const userQuestion = ref('')
const askingAgent = ref(false)
const agentConversation = ref<Array<{
  sender: 'user' | 'agent'
  text: string
  toolUsed?: string
  data?: any
}>>([
  {
    sender: 'agent',
    text: '¡Hola! Soy el asistente analítico de SismoWatch. Puedo informarte sobre sismos destacados, actividad regional o indicadores calculados en la capa Gold sin ejecutar SQL arbitrario.',
  }
])

const quickQuestions = [
  '¿Cuáles son los sismos más fuertes registrados?',
  '¿Qué regiones presentan mayor actividad sísmica?',
  '¿Cuántos eventos se han registrado en total?',
  'Muéstrame el último informe diario generado',
]

const loadReportsHistory = async () => {
  try {
    const list = await fetchReports()
    reportsList.value = list
    if (list.length > 0 && !selectedDate.value) {
      selectedDate.value = list[0].reference_date
      await loadReportDetail(selectedDate.value)
    }
  } catch (err) {
    console.error('Error cargando historial de reportes:', err)
  }
}

const loadReportDetail = async (dateStr: string) => {
  selectedDate.value = dateStr
  loadingReport.value = true
  try {
    const detail = await fetchReportByDate(dateStr)
    currentReport.value = detail
  } catch (err) {
    console.error('Error cargando detalle del reporte:', err)
  } finally {
    loadingReport.value = false
  }
}

const handleAskAgent = async (questionText?: string) => {
  const q = (questionText || userQuestion.value).trim()
  if (!q) return

  agentConversation.value.push({ sender: 'user', text: q })
  userQuestion.value = ''
  askingAgent.value = true

  try {
    const response: AgentQueryResponse = await askAgent(q)
    agentConversation.value.push({
      sender: 'agent',
      text: response.answer,
      toolUsed: response.tool_used,
      data: response.data,
    })
  } catch (err: any) {
    agentConversation.value.push({
      sender: 'agent',
      text: 'Lo siento, ocurrió un error consultando el asistente: ' + (err.detail || err.message),
    })
  } finally {
    askingAgent.value = false
  }
}

const handleExport = async (format: 'csv' | 'geojson') => {
  try {
    await downloadExport(format)
  } catch (err) {
    console.error('Error descargando exportación:', err)
  }
}

onMounted(() => {
  loadReportsHistory()
})
</script>

<template>
  <div class="space-y-6">
    <!-- Encabezado de la Vista -->
    <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
      <div>
        <h1 class="text-2xl font-bold tracking-tight text-white flex items-center space-x-2">
          <span>Reportes Diarios y Agente Analítico de IA</span>
          <span class="text-xs font-mono font-normal px-2.5 py-0.5 rounded-full bg-purple-950/80 text-purple-300 border border-purple-800">
            Ollama Gemma:2b + Fallback
          </span>
        </h1>
        <p class="text-xs text-slate-400 mt-1">
          Informes analíticos generados a partir de indicadores deterministas de Gold, con auditoría completa y asistente conversacional.
        </p>
      </div>

      <!-- Exportaciones directas -->
      <div class="flex items-center space-x-2">
        <button
          @click="handleExport('csv')"
          class="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-medium transition-colors"
        >
          <Download class="w-3.5 h-3.5 text-sky-400" />
          <span>Exportar CSV</span>
        </button>
        <button
          @click="handleExport('geojson')"
          class="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-medium transition-colors"
        >
          <Download class="w-3.5 h-3.5 text-emerald-400" />
          <span>GeoJSON</span>
        </button>
      </div>
    </div>

    <!-- Layout en 2 columnas: Informe a la izquierda, Asistente a la derecha -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
      <!-- Columna Izquierda: Informe Diario (7 cols) -->
      <div class="lg:col-span-7 space-y-4">
        <!-- Selector de Historial de Informes -->
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-sm flex items-center justify-between">
          <div class="flex items-center space-x-3">
            <Calendar class="w-4 h-4 text-sky-400" />
            <span class="text-xs font-semibold uppercase tracking-wider text-slate-300">Fecha del Informe:</span>
            <select
              v-model="selectedDate"
              @change="loadReportDetail(selectedDate)"
              class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1 text-xs text-white focus:outline-none focus:ring-1 focus:ring-sky-500 font-mono"
            >
              <option v-for="r in reportsList" :key="r.report_id" :value="r.reference_date">
                {{ r.reference_date }} ({{ r.status === 'fallback' ? 'Fallback determinista' : 'IA OK' }})
              </option>
            </select>
          </div>

          <div v-if="currentReport" class="flex items-center space-x-2 text-xs">
            <span
              class="px-2 py-0.5 rounded text-[11px] font-semibold uppercase tracking-wider"
              :class="currentReport.status === 'ok' ? 'bg-emerald-950 text-emerald-300 border border-emerald-800' : 'bg-amber-950 text-amber-300 border border-amber-800'"
            >
              {{ currentReport.status === 'ok' ? 'Generado por IA' : 'Fallback Determinista' }}
            </span>
          </div>
        </div>

        <!-- Visualizador de Informe -->
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-sm min-h-[500px]">
          <div v-if="loadingReport" class="py-20 text-center text-slate-400">
            <div class="inline-flex items-center space-x-2">
              <span class="w-5 h-5 border-2 border-sky-400 border-t-transparent rounded-full animate-spin"></span>
              <span>Cargando informe oficial...</span>
            </div>
          </div>

          <div v-else-if="currentReport" class="space-y-6">
            <!-- Metadatos de auditoría -->
            <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 p-3 bg-slate-800/50 rounded-lg border border-slate-800 text-xs">
              <div>
                <span class="text-slate-500 block text-[10px] uppercase">Proveedor / Modelo</span>
                <span class="text-slate-200 font-mono">{{ currentReport.provider }} &bull; {{ currentReport.model }}</span>
              </div>
              <div>
                <span class="text-slate-500 block text-[10px] uppercase">Latencia</span>
                <span class="text-slate-200 font-mono">{{ currentReport.latency_ms !== null ? currentReport.latency_ms + ' ms' : '0 ms' }}</span>
              </div>
              <div>
                <span class="text-slate-500 block text-[10px] uppercase">Generado UTC</span>
                <span class="text-slate-200 font-mono">{{ currentReport.generated_at ? currentReport.generated_at.slice(11, 19) : 'N/A' }}</span>
              </div>
              <div>
                <span class="text-slate-500 block text-[10px] uppercase">ID Determinista</span>
                <span class="text-slate-300 font-mono text-[10px] truncate block" :title="currentReport.report_id">{{ currentReport.report_id.slice(0, 12) }}...</span>
              </div>
            </div>

            <!-- Badges de Indicadores Calculados en Código -->
            <div v-if="currentReport.indicators" class="p-4 bg-slate-800/30 rounded-lg border border-slate-700/60">
              <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3 flex items-center space-x-1.5">
                <CheckCircle2 class="w-3.5 h-3.5 text-emerald-400" />
                <span>Indicadores Calculados en Gold (Fuente Determinista)</span>
              </h3>
              <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                <div class="p-2.5 rounded bg-slate-800/80 border border-slate-700">
                  <span class="text-slate-400 block text-[11px]">Sismos Reportados</span>
                  <span class="text-lg font-bold text-white font-mono">{{ currentReport.indicators.total_sismos || 0 }}</span>
                </div>
                <div class="p-2.5 rounded bg-slate-800/80 border border-slate-700">
                  <span class="text-slate-400 block text-[11px]">Mag Máxima</span>
                  <span class="text-lg font-bold text-rose-400 font-mono">M {{ currentReport.indicators.max_magnitud || 'N/A' }}</span>
                </div>
                <div class="p-2.5 rounded bg-slate-800/80 border border-slate-700">
                  <span class="text-slate-400 block text-[11px]">Mag Promedio</span>
                  <span class="text-lg font-bold text-amber-400 font-mono">M {{ currentReport.indicators.avg_magnitud || 'N/A' }}</span>
                </div>
                <div class="p-2.5 rounded bg-slate-800/80 border border-slate-700">
                  <span class="text-slate-400 block text-[11px]">Región Principal</span>
                  <span class="text-xs font-bold text-sky-400 truncate block mt-1">{{ currentReport.indicators.top_region || 'N/A' }}</span>
                </div>
              </div>
            </div>

            <!-- Contenido Markdown del Informe -->
            <div class="prose prose-invert max-w-none text-slate-300 text-sm leading-relaxed border-t border-slate-800 pt-4 whitespace-pre-line font-sans">
              {{ currentReport.summary_md }}
            </div>
          </div>

          <div v-else class="py-20 text-center text-slate-500">
            No se encontró ningún informe generado en la capa Gold para la fecha seleccionada.
          </div>
        </div>
      </div>

      <!-- Columna Derecha: Asistente Analítico de IA (5 cols) -->
      <div class="lg:col-span-5 flex flex-col h-[650px] bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-sm">
        <!-- Header del Chat -->
        <div class="p-4 bg-slate-800/80 border-b border-slate-800 flex items-center justify-between">
          <div class="flex items-center space-x-2.5">
            <div class="w-8 h-8 rounded-lg bg-sky-500/20 border border-sky-500/30 flex items-center justify-center text-sky-400">
              <Bot class="w-4 h-4" />
            </div>
            <div>
              <h2 class="text-sm font-bold text-white">Agente Analítico de IA</h2>
              <span class="text-[10px] text-emerald-400 font-mono flex items-center space-x-1">
                <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 inline-block"></span>
                <span>Herramientas de solo lectura en Gold</span>
              </span>
            </div>
          </div>
        </div>

        <!-- Preguntas rápidas -->
        <div class="p-3 bg-slate-800/30 border-b border-slate-800/60 overflow-x-auto flex space-x-2 scrollbar-none">
          <button
            v-for="q in quickQuestions"
            :key="q"
            @click="handleAskAgent(q)"
            class="flex-shrink-0 px-2.5 py-1 rounded-full bg-slate-800 hover:bg-slate-700 border border-slate-700 text-[11px] text-slate-300 transition-colors"
          >
            {{ q }}
          </button>
        </div>

        <!-- Historial de Conversación -->
        <div class="flex-1 p-4 overflow-y-auto space-y-4">
          <div
            v-for="(msg, idx) in agentConversation"
            :key="idx"
            class="flex flex-col"
            :class="msg.sender === 'user' ? 'items-end' : 'items-start'"
          >
            <div
              class="max-w-[85%] rounded-xl px-4 py-3 text-xs leading-relaxed"
              :class="msg.sender === 'user'
                ? 'bg-sky-600 text-white rounded-tr-none'
                : 'bg-slate-800 text-slate-200 border border-slate-700 rounded-tl-none'"
            >
              <p>{{ msg.text }}</p>

              <!-- Indicador de herramienta utilizada -->
              <div v-if="msg.toolUsed" class="mt-2 pt-2 border-t border-slate-700/60 flex items-center justify-between text-[10px] text-slate-400 font-mono">
                <span>Herramienta: <strong class="text-sky-400">{{ msg.toolUsed }}</strong></span>
                <span class="text-emerald-400">&bull; Gold verificado</span>
              </div>
            </div>
          </div>

          <div v-if="askingAgent" class="flex items-center space-x-2 text-xs text-slate-400">
            <Bot class="w-4 h-4 text-sky-400 animate-bounce" />
            <span>Consultando datos y redactando respuesta...</span>
          </div>
        </div>

        <!-- Input para enviar pregunta -->
        <div class="p-3 bg-slate-800/80 border-t border-slate-800">
          <form @submit.prevent="handleAskAgent()" class="flex items-center space-x-2">
            <input
              v-model="userQuestion"
              type="text"
              placeholder="Haz una pregunta sobre sismos o regiones..."
              :disabled="askingAgent"
              class="flex-1 bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
            <button
              type="submit"
              :disabled="askingAgent || !userQuestion.trim()"
              class="p-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-40 transition-colors"
            >
              <Send class="w-4 h-4" />
            </button>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>
