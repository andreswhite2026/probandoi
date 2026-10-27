<script setup lang="ts">
import { ref, onMounted, watch, onBeforeUnmount } from 'vue'
import L from 'leaflet'
import type { Earthquake } from '../services/earthquakes'

const props = defineProps<{
  earthquakes: Earthquake[]
}>()

const mapContainer = ref<HTMLElement | null>(null)
let map: L.Map | null = null
let markersLayer: L.LayerGroup | null = null

const getMarkerColor = (mag: number | null): string => {
  if (mag === null) return '#94a3b8'
  if (mag >= 6.5) return '#f43f5e' // Rose
  if (mag >= 5.0) return '#f97316' // Orange
  if (mag >= 4.0) return '#eab308' // Yellow
  return '#10b981' // Emerald
}

const updateMarkers = () => {
  const layer = markersLayer
  if (!layer) return
  layer.clearLayers()

  props.earthquakes.forEach(eq => {
    if (eq.latitude !== undefined && eq.longitude !== undefined) {
      const color = getMarkerColor(eq.mag)
      const radius = Math.max(4, (eq.mag || 2) * 2.8)

      const marker = L.circleMarker([eq.latitude, eq.longitude], {
        radius,
        fillColor: color,
        color: '#ffffff',
        weight: 1,
        opacity: 0.9,
        fillOpacity: 0.75,
      })

      const popupHtml = `
        <div style="min-width: 180px; font-family: inherit;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
            <span style="font-size: 16px; font-weight: bold; color: ${color};">M ${eq.mag !== null ? eq.mag.toFixed(1) : 'N/A'}</span>
            ${eq.tsunami ? '<span style="background: #e11d48; color: #fff; font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: bold;">TSUNAMI</span>' : ''}
          </div>
          <p style="margin: 0 0 4px 0; font-size: 13px; font-weight: 500; color: #f8fafc;">${eq.place || 'Lugar no especificado'}</p>
          <div style="font-size: 11px; color: #94a3b8; display: grid; grid-template-columns: 1fr 1fr; gap: 4px; margin-top: 6px;">
            <div><strong>Región:</strong> ${eq.region || 'Desconocida'}</div>
            <div><strong>Prof.:</strong> ${eq.depth_km !== null ? eq.depth_km + ' km' : 'N/A'}</div>
            <div style="grid-column: span 2;"><strong>Fecha UTC:</strong> ${eq.time_utc ? eq.time_utc.slice(0, 19).replace('T', ' ') : 'N/A'}</div>
          </div>
          ${eq.url ? `<div style="margin-top: 8px;"><a href="${eq.url}" target="_blank" rel="noopener noreferrer" style="color: #38bdf8; font-size: 11px; text-decoration: underline;">Ver ficha en USGS &rarr;</a></div>` : ''}
        </div>
      `
      marker.bindPopup(popupHtml)
      layer.addLayer(marker)
    }
  })
}

onMounted(() => {
  if (!mapContainer.value) return

  map = L.map(mapContainer.value, {
    zoomControl: true,
    attributionControl: false,
  }).setView([15, 0], 2)

  // Capa base CartoDB Dark Matter (perfecto para mapas sísmicos nocturnos)
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
    subdomains: 'abcd',
  }).addTo(map)

  markersLayer = L.layerGroup().addTo(map)
  updateMarkers()
})

watch(() => props.earthquakes, () => {
  updateMarkers()
}, { deep: true })

onBeforeUnmount(() => {
  if (map) {
    map.remove()
    map = null
  }
})
</script>

<template>
  <div class="relative w-full h-[450px] rounded-xl overflow-hidden border border-slate-800 shadow-md">
    <div ref="mapContainer" class="w-full h-full"></div>
    
    <!-- Leyenda de magnitudes -->
    <div class="absolute bottom-4 right-4 z-[400] bg-slate-900/90 backdrop-blur-sm border border-slate-700/80 rounded-lg p-2.5 text-xs text-slate-300 shadow-lg">
      <div class="font-semibold text-slate-200 mb-1.5 uppercase tracking-wider text-[10px]">Magnitud</div>
      <div class="space-y-1">
        <div class="flex items-center space-x-2">
          <span class="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
          <span>&ge; 6.5 Fuerte</span>
        </div>
        <div class="flex items-center space-x-2">
          <span class="w-2.5 h-2.5 rounded-full bg-orange-500"></span>
          <span>5.0 &ndash; 6.4 Moderado</span>
        </div>
        <div class="flex items-center space-x-2">
          <span class="w-2.5 h-2.5 rounded-full bg-yellow-500"></span>
          <span>4.0 &ndash; 4.9 Menor</span>
        </div>
        <div class="flex items-center space-x-2">
          <span class="w-2.5 h-2.5 rounded-full bg-emerald-500"></span>
          <span>&lt; 4.0 Micro / Leve</span>
        </div>
      </div>
    </div>
  </div>
</template>
