# ADR-004 — Mapa de almacenamiento: MinIO vs PostgreSQL y nombres de capas

- **Fecha:** 2026-10-27
- **Estado:** Aprobada (sujeta a evidencia en Fase 1-4)
- **Decide:** qué guarda cada almacenamiento y cómo se mapean los nombres

## Contexto

Dos requisitos deben convivir:

- El enunciado exige PostgreSQL con esquemas `raw`, `staging` y `curated`.
- El TL exige MinIO y arquitectura Medallion (Bronze/Silver/Gold).

Duplicar todo en ambos sería costoso, incoherente y difícil de defender.

## Decisión

| Capa lógica | Nombre del enunciado | Canónico | Copia secundaria justificada |
|---|---|---|---|
| Bronze | `raw` | **MinIO** (GeoJSON inmutable) | Postgres `raw.ingestions` solo con metadatos del objeto |
| Silver | `staging` | **Postgres** `staging.earthquakes` (estado actual) | MinIO `silver/` Parquet (historia reprocesable) |
| Gold | `curated` | **Postgres** `curated.*` (serving, reportes, calidad) | ninguna |

Reglas asociadas:

- El payload crudo **nunca** se copia a Postgres: se guarda `object_key` + `sha256`.
- Silver Parquet no se usa para serving; existe para reprocesar y para análisis columnar.
- Gold no se replica en MinIO para no crear dos verdades.
- Quien escribe cada capa es un único componente (ver matriz completa en `docs/ARQUITECTURA.md`).

## Consecuencias

- La API solo necesita conexión a Postgres (rol `sismo_api` con `SELECT` sobre `curated`).
- Se pueden reconstruir `staging` y `curated` desde Bronze sin volver a USGS.
- La explicación en sustentación es clara: "MinIO es el lago; Postgres es el estado y el serving".

## Verificación

- [ ] Un objeto Bronze apunta a un `run_id` en `raw.ingestions`.
- [ ] Borrar `curated` y reprocesar desde Bronze reconstruye los datos (prueba de Fase 4).
- [ ] `sismo_api` recibe `permission denied` al intentar `SELECT` en `raw.ingestions`.
