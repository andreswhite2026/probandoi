# Sustentación — SismoWatch

Guion de preparación. Se completa con la implementación real; aquí queda la estructura y lo que ya
está decidido. **No se afirma nada que no se haya verificado.**

---

## 1. Qué problema resuelve

Una compañía necesita saber qué está pasando sísmicamente (dónde, cuándo, magnitud) para priorizar
decisiones. Hoy solo existe un feed público que cambia cada minuto: sin historial, sin validación y
sin resumen. SismoWatch lo convierte en un producto de datos con calidad controlada, trazabilidad y
consumo seguro.

**Frase de apertura (30 s):** "No construí un descargador de JSON; construí un pipeline con capas,
calidad bloqueante, cuarentena, reportes auditados y una plataforma que solo puede leer datos
aprobados."

## 2. Arquitectura en una frase por componente

| Componente | Qué diré |
|---|---|
| Airflow | Orquesta y programa; no transforma datos. Dispara la transformación por Dataset, no por hora. |
| Spark | Transforma y valida en un solo job: parsea GeoJSON, tipa, deduplica, aplica reglas y publica. |
| MinIO | Conserva el crudo inmutable y el Parquet de Silver; permite reprocesar sin volver a USGS. |
| PostgreSQL | Estado tipado, cuarentena, métricas, reportes y serving. La API solo ve `curated`. |
| OpenMetadata | Catálogo y linaje: qué activos existen y de dónde vienen. |
| FastAPI | Autenticación, autorización por rol, serving y SSE. |
| React | Tres vistas: Dashboard, Reportes, Operaciones. |
| Ollama | Redacta y explica; las cifras las calcula el código. |

## 3. Recorrido de un evento (lo que debo poder narrar sin leer)

1. USGS publica un evento en el feed `all_hour.geojson`.
2. A los ≤5 min, `ingest_feed` hace GET con timeout, valida el GeoJSON y sube el objeto a
   `bronze/feed/dt=/hour=/<run_id>.json`.
3. Inserta el manifiesto en `raw.ingestions` (`run_id`, URL, hora, bytes, `sha256`, `count`).
4. Emite el Dataset → `transform_publish` arranca.
5. Spark lee el objeto, aplana `features[]`, convierte `properties.time` (epoch ms) a `timestamptz`
   UTC y ordena `coordinates` como `[lon, lat, depth_km]`.
6. Aplica reglas: obligatorios, tipos, rangos, unicidad, duplicados, frescura, volumen.
7. Filas válidas → `staging.earthquakes` (upsert por `id`) + Parquet Silver.
8. Filas inválidas → `curated.quarantine` con `reason_code`.
9. Métricas → `quality_runs` y `quality_rules`.
10. Solo aprobados → `curated.earthquakes` + agregados + anomalías.
11. La API expone `/api/kpis`, `/api/events`, `/api/stream` (SSE) leyendo `curated`.
12. El dashboard muestra KPIs, mapa y eventos nuevos sin recargar.
13. A diario, el DAG de IA toma los agregados ya calculados, pide narrativa al modelo, valida el JSON
    y guarda en `curated.ai_reports` con sus metadatos en `ai_runs`.

## 4. Preguntas probables (y cómo responderlas)

| Pregunta | Línea de respuesta |
|---|---|
| ¿Por qué Airflow y no cron? | Reintentos con backoff, timeouts, dependencias por dato, historial y alertas. Cron no tiene estado ni dependencias. |
| ¿Por qué Dataset y no TriggerDagRun? | Coordina por dato, no por tarea: el DAG de transformación no necesita saber quién produjo el archivo, y se pueden añadir consumidores. |
| ¿Por qué Spark si son 2.000 filas? | El requisito lo exige y el patrón escala; el mismo job procesa 11 o 2.000 eventos, con esquema y escritura particionada. |
| ¿Por qué no Kafka? | Micro-batch de 5 min, sin consumidores independientes ni necesidad de replay; Bronze + backfill ya cubren recuperación. |
| ¿Cómo evitas duplicados? | `id` como clave natural + `ON CONFLICT DO UPDATE` condicionado por `updated` (USGS revisa eventos). |
| ¿Cómo garantizas que la IA no invente? | El código calcula; el modelo solo redacta; validación Pydantic + verificación de que cada número citado exista en el contexto. |
| ¿Qué pasa si Ollama falla? | Se registra `status='fallback'`, no se publica como válido, se conserva el reporte anterior y el DAG no se cae. |
| ¿Cómo proteges `raw`? | Rol `sismo_api` solo con `SELECT` en `curated`; la API no tiene permisos sobre `raw`/`staging`. |
| ¿Un viewer puede lanzar backfill? | No: `403` en el backend; el botón ni se renderiza, pero la seguridad está en el servidor. |
| ¿Cómo actualizas sin recargar? | SSE con reconexión y backoff; se muestra hora de última actualización. |
| ¿Qué evidencia tienes de OpenMetadata? | Activos y linaje visibles en la UI, capturados tras una ejecución real (se documenta qué es automático y qué declarado). |
| ¿Qué probaste? | `pytest`: transformación, importación de DAGs, regla de calidad válida/inválida, idempotencia, endpoint, 401/403, no publicación en Gold, fallo del proveedor IA simulado. |

## 5. Limitaciones que debo declarar yo mismo

- Modo `local[*]`: un solo proceso Spark; no demuestra escalado distribuido.
- OpenMetadata con heaps reducidos por RAM del equipo; en producción iría con OpenSearch dedicado.
- IA local pequeña (`gemma:2b`): buena para formato y redacción breve, no para análisis profundo.
- Sin Kafka: justificado por el patrón batch del caso.

## 6. Evidencias a capturar antes de entregar

- [ ] `docker compose ps` con todo healthy
- [ ] Airflow: DAGs sin errores + 3 ejecuciones exitosas
- [ ] MinIO: objetos en `bronze/` y `silver/`
- [ ] `psql`: conteos `raw`/`staging`/`curated` y cuarentena con motivo
- [ ] Dashboard con KPIs, mapa y "EN VIVO"
- [ ] Reporte IA con cifras y su fila en `ai_reports`
- [ ] OpenMetadata: activos y linaje
- [ ] `pytest` verde y salida de escaneos de seguridad
