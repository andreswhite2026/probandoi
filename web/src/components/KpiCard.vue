<script setup lang="ts">
import type { Component } from 'vue'

defineProps<{
  title: string
  value: string | number
  subtitle?: string
  icon: Component
  color?: 'sky' | 'amber' | 'rose' | 'emerald' | 'purple'
}>()
</script>

<template>
  <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-sm hover:border-slate-700 transition-all">
    <div class="flex items-center justify-between">
      <span class="text-xs font-medium uppercase tracking-wider text-slate-400">{{ title }}</span>
      <div
        class="w-8 h-8 rounded-lg flex items-center justify-center border"
        :class="{
          'bg-sky-500/10 text-sky-400 border-sky-500/20': !color || color === 'sky',
          'bg-amber-500/10 text-amber-400 border-amber-500/20': color === 'amber',
          'bg-rose-500/10 text-rose-400 border-rose-500/20': color === 'rose',
          'bg-emerald-500/10 text-emerald-400 border-emerald-500/20': color === 'emerald',
          'bg-purple-500/10 text-purple-400 border-purple-500/20': color === 'purple',
        }"
      >
        <component :is="icon" class="w-4 h-4" />
      </div>
    </div>
    <div class="mt-3">
      <span class="text-2xl font-bold tracking-tight text-white">{{ value }}</span>
      <p v-if="subtitle" class="mt-1 text-xs text-slate-400">{{ subtitle }}</p>
    </div>
  </div>
</template>
