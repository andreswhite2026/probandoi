# Arquitectura — SismoWatch

> Estado: Fase 0. Este documento describe el diseño aprobado; las secciones marcadas
> **(a verificar)** se completan con evidencia real cuando se implementen.

---

## 1. Propósito

SismoWatch convierte el feed público de terremotos de USGS en un producto de datos con historial,
calidad controlada y consumo seguro. El problema no es descargar JSON: es garantizar que lo que se
publica es **completo, tipado, deduplicado, trazable y actualizado**, y que cualquier consumidor
( dashboard, reporte, agente) vea exactamente la misma verdad.

Decisiones que habilita:

- Priorizar revisión operativa por región y magnitud en las últimas 24 h.
- Comparar la actividad reciente contra el histórico de 30 días.
- Detectar concentraciones anómalas de eventos con una regla estadística reproducible.
- Auditar de dónde salió cada cifra publicada (objeto crudo → ejecución → regla → reporte).

## 2. Diagrama de componentes

```
┌────────────┐   HTTPS (timeout, retries)   ┌───────────────────────────────┐
│   USGS     │ ───────────────────────────► │ Airflow 2.10.5 (LocalExecutor)│
│ feed/fdsnws│                              │  DAG ingest_feed   */5 * * * *│
└────────────┘                              │  DAG transform     (Dataset)  │
                                            │  DAG backfill      (params)   │
                                            │  DAG ai_daily_report 0 6 * * *│
                                            └───────┬───────────────┬───────┘
                                                    │ boto3         │ spark-submit local[*]
                                       JSON crudo   ▼               ▼
                                            ┌────────────┐   ┌───────────────┐
                                            │   MinIO    │◄─►│ Spark 3.5.1   │
                                            │ bronze/    │   │ (driver en el │
                                            │ silver/    │   │  contenedor   │
                                            └────────────┘   │  Airflow)     │
                                                             └───────┬───────┘
                                                                     │ JDBC upsert
                                                                     ▼
                                            ┌────────────────────────────────────┐
                                            │ PostgreSQL 16                      │
                                            │ raw.ingestions                     │
                                            │ staging.earthquakes                │
                                            │ curated.earthquakes / quality_*    │
                                            │ curated.quarantine / ai_reports    │
                                            │ app.users                          │
                                            └───────┬────────────────────┬───────┘
                                      SELECT solo   │                    │
                                       curated      ▼                    ▼
                                            ┌──────────────┐    ┌──────────────┐
                                            │ FastAPI      │───►│ Ollama local │
                                            │ JWT + RBAC   │    │ (host:11434) │
                                            │ SSE / export │    └──────────────┘
                                            └──────┬───────┘
                                                   ▼
                                            ┌──────────────┐      ┌────────────────────┐
                                            │ React + Vite │      │ OpenMetadata 2.0.6 │
                                            │ 3 vistas     │      │ catálogo + linaje  │
                                            └──────────────┘      └────────────────────┘
```

## 3. Responsabilidades

| Componente | Responsabilidad única | No debe hacer |
|---|---|---|
| Airflow | Orquestar, programar, reintentar, registrar estado | Transformar datos ni cálculo pesado |
| Spark | Transformar, validar, agregar, publicar capas | Descargar de USGS ni decidir agenda |
| MinIO | Preservar payloads crudos y Parquet de Silver | Servir la API |
| PostgreSQL | Estado tipado, cuarentena, métricas, reportes, serving | Guardar payloads crudos completos |
| FastAPI | Autenticar, autorizar, exponer curated, exportar | Leer raw/staging ni escribir datos |
| Ollama | Redactar narrativa sobre cifras ya calculadas | Calcular cifras ni consultar la base |
| OpenMetadata | Catalogar activos y linaje | Mover o transformar datos |

## 4. Mapa de almacenamiento (matriz)

| Capa | Contenido | Canónico | Formato | Escribe | Lee | Clave | Actualización / idempotencia |
|---|---|---|---|---|---|---|---|
| Bronze | GeoJSON original por extracción | **MinIO** `bronze/feed/dt=/hour=/<run_id>.json` | JSON inmutable | DAG ingesta (boto3) | Spark | `run_id` | Un objeto por `run_id`; nunca se sobrescribe |
| raw (PG) | Manifiesto de extracción | **PostgreSQL** `raw.ingestions` | fila | DAG ingesta | Airflow, API admin | `run_id` PK | `ON CONFLICT (run_id) DO NOTHING` |
| Silver lake | Evento tipado particionado por fecha | **MinIO** `silver/earthquakes/dt=YYYY-MM-DD/` | Parquet | Spark (s3a) | Spark | `id` + `dt` | `partitionOverwriteMode=dynamic` tras dedupe por `id` |
| Silver (PG) | Estado actual del evento | **PostgreSQL** `staging.earthquakes` | fila | Spark (JDBC) | Spark, auditoría | `id` PK | `ON CONFLICT (id) DO UPDATE` si `updated_utc` mayor o nulo |
| Gold | Eventos aprobados | **PostgreSQL** `curated.earthquakes` | fila | Spark (JDBC) | API | `id` PK | upsert por `id` |
| Gold | Agregados horarios / regiones / magnitudes | **PostgreSQL** `curated.*` | fila | Spark | API | clave natural compuesta | upsert por ventana |
| Gold | Anomalías | **PostgreSQL** `curated.anomalies` | fila | Spark | API | `(window_start, region)` | upsert por ventana + regla |
| Gold | Reportes IA y auditoría | **PostgreSQL** `curated.ai_reports`, `curated.ai_runs` | fila + JSONB | DAG IA | API | `report_id` / `run_id` | histórico; se conserva el anterior si falla |
| Calidad | `quality_runs`, `quality_rules`, `quarantine` | **PostgreSQL** | fila + JSONB | Spark | API (admin para cuarentena) | `run_id`, `(run_id, rule_code)` | append por ejecución |

### Por qué no se duplica todo

- El payload crudo **solo** existe en MinIO; Postgres guarda `object_key` + `sha256`, no el GeoJSON.
- Silver en MinIO es historia reprocesable en Parquet; `staging.earthquakes` es el estado actual
  consultable y la base del upsert. Son dos propósitos distintos, documentados, no una copia ciega.
- Gold **solo** existe en Postgres: la API únicamente puede leer `curated` y no necesita otra copia.
  No se publican Parquet de Gold para evitar dos verdades.

### Trazabilidad

`raw.ingestions.run_id` → `staging.earthquakes.ingest_run_id` →
`curated.earthquakes.ingest_run_id` + `curated.quality_runs.run_id` →
`curated.ai_reports.source_run_id`. Cada fila publicada se puede rastrear hasta el objeto de MinIO.

## 5. Modelo de datos (diseño inicial)

### `raw`

- **`raw.ingestions`**: `run_id uuid PK`, `source text` (`feed`|`backfill`), `source_url text`,
  `window_from timestamptz NULL`, `window_to timestamptz NULL`, `fetched_at timestamptz`,
  `http_status int`, `object_key text UNIQUE`, `bytes int`, `event_count int`, `sha256 char(64)`,
  `status text` (`ok`|`empty`|`invalid`|`error`), `error_detail text NULL`.

### `staging`

- **`staging.earthquakes`**: `id text PK`, `mag numeric(3,1) NULL`, `mag_type text`,
  `place text`, `region text NULL` (derivada de `place`, heurística documentada), `time_utc timestamptz`,
  `updated_utc timestamptz NULL`, `tsunami boolean`, `sig int`, `event_status text`,
  `event_type text`, `url text`, `longitude numeric(9,5)`, `latitude numeric(8,5)`,
  `depth_km numeric(7,2)`, `ingest_run_id uuid`, `first_seen_at timestamptz`, `last_seen_at timestamptz`.
  Índices: `(time_utc DESC)`, `(mag DESC)`, `(region)`.

### `curated`

- **`curated.earthquakes`**: proyección de serving de los eventos aprobados:
  `id`, `mag`, `mag_type`, `place`, `region`, `time_utc`, `updated_utc`, `tsunami`, `sig`,
  `event_status`, `event_type`, `url`, `longitude`, `latitude`, `depth_km`,
  `ingest_run_id`, `published_at`. Índices equivalentes a staging.
- **`curated.quality_runs`**: `run_id uuid PK`, `dag_run_id text`, `layer text`,
  `evaluated int`, `approved int`, `rejected int`, `duplicates int`, `score numeric(5,2)`,
  `status text`, `started_at`, `finished_at`.
- **`curated.quality_rules`**: `id bigserial PK`, `run_id uuid FK`, `rule_code text`,
  `description text`, `severity text`, `evaluated int`, `failed int`, `passed boolean`.
- **`curated.quarantine`**: `id bigserial PK`, `run_id uuid FK`, `event_id text`,
  `raw_object_key text`, `payload jsonb`, `reason_code text`, `reason_detail text`, `created_at`.
- **`curated.anomalies`**: `window_start timestamptz`, `window_hours int`, `region text`,
  `method text` (`zscore`), `metric text` (`event_count`), `value numeric`, `baseline_mean numeric`,
  `baseline_std numeric`, `zscore numeric`, `is_anomaly boolean`, `explanation text NULL`,
  `detected_at timestamptz`. Clave: `(window_start, region, method)`.
- **`curated.ai_reports`**: `report_id uuid PK`, `reference_date date`, `generated_at timestamptz`,
  `summary_md text`, `indicators jsonb`, `anomalies jsonb`, `provider text`, `model text`,
  `prompt_version text`, `input_tokens int NULL`, `output_tokens int NULL`, `latency_ms int`,
  `cost_usd numeric(10,6) NULL`, `status text` (`ok`|`fallback`|`failed`), `source_run_id uuid`,
  `error_detail text NULL`.
- **`curated.ai_runs`**: auditoría de toda invocación (incluida la del agente):
  `run_id uuid PK`, `report_id uuid NULL`, `operation text`, `provider`, `model`, `prompt_version`,
  `input_tokens`, `output_tokens`, `latency_ms`, `cost_usd`, `status`, `error_detail`, `created_at`.

### `app`

- **`app.users`**: `id uuid PK`, `email citext UNIQUE`, `password_hash text`, `role text`
  (`viewer`|`admin`), `is_active boolean`, `created_at`, `last_login_at NULL`.
  Es autenticación de aplicación, no dato sísmico: por eso no vive en `curated`.

### Roles PostgreSQL (mínimo privilegio)

| Rol | Permisos |
|---|---|
| `sismo_ingest` | `INSERT` en `raw.ingestions` |
| `sismo_transform` | `SELECT/INSERT/UPDATE` en `staging` y `curated` |
| `sismo_api` | `SELECT` en `curated`; `SELECT/UPDATE` en `app.users` |

La API se conecta con `sismo_api`: aunque un endpoint tuviera un bug, no puede leer `raw`/`staging`
ni escribir en `curated`.

## 6. Flujo Bronze → Silver → Gold

1. **Ingesta** (`*/5`): GET con timeout y reintentos → valida que sea GeoJSON `FeatureCollection` →
   sube el objeto a Bronze con `run_id` y `sha256` → inserta manifiesto en `raw.ingestions` →
   emite el **Dataset** `sismowatch://bronze/feed`.
2. **Transformación** (disparada por Dataset): Spark lee los objetos Bronze pendientes de procesar,
   parsea `features[]`, aplana `properties` y `geometry.coordinates` respetando el orden
   `[longitud, latitud, profundidad_km]`, convierte epoch-ms a `timestamptz` UTC, deriva `region`.
3. **Calidad** (misma ejecución Spark, antes de publicar): reglas declarativas → filas válidas a
   `staging.earthquakes` y Parquet Silver; filas inválidas a `curated.quarantine` con `reason_code`;
   métricas a `quality_runs` + `quality_rules`.
4. **Publicación Gold**: solo filas aprobadas → upsert en `curated.earthquakes` + agregados +
   anomalías. Si la calidad falla por debajo del umbral configurado, la tarea falla y **no** publica.
5. **Consumo**: API lee `curated` y emite SSE; el DAG diario de IA lee agregados y escribe
   `ai_reports` / `ai_runs`.

## 7. Dependencias entre DAGs

| DAG | Disparo | Depende de |
|---|---|---|
| `ingest_feed` | schedule `*/5 * * * *` | USGS |
| `transform_publish` | **Dataset** emitido por `ingest_feed` (Airflow 2.10) | Bronze disponible |
| `backfill_history` | manual / API admin, `conf` con `start_date` y `end_date` | USGS fdsnws |
| `ai_daily_report` | schedule diario | Gold actualizado |

Se elige **Dataset scheduling** (no `TriggerDagRunOperator`) porque desacopla productor y consumidor,
permite más de un consumidor en el futuro y refleja el dato (no la tarea) como contrato. En Airflow
2.10 el concepto se llama *Dataset*; en Airflow 3 se renombró a *Asset*.

## 8. IA

- **Cifras**: las calcula SQL/Spark y viajan al modelo como JSON ya agregado.
- **Salida**: JSON validado con Pydantic (`summary_md`, `sections[]`, `referenced_numbers[]`).
- **Anti-alucinación**: se verifica que cada número citado exista en el conjunto calculado; si no,
  el reporte se marca `failed` y no se publica como válido.
- **Fallo del proveedor**: se registra en `ai_runs` con `status='fallback'`, se conserva el reporte
  anterior y el DAG no se rompe.
- **Agente**: solo lectura sobre un repositorio con funciones permitidas
  (`list_reports`, `get_report`, `compare_reports`, `get_kpis`). Sin SQL libre, sin escritura.

## 9. OpenMetadata **(a verificar)**

- Servicios: PostgreSQL (metadatos), MinIO (lake), Airflow (pipeline).
- Activos previstos: bases/esquemas/tablas `raw.ingestions`, `staging.earthquakes`,
  `curated.earthquakes`, `curated.quality_runs`, `curated.ai_reports`; buckets `bronze` y `silver`.
- Linaje esperado: `raw.ingestions` → `staging.earthquakes` → `curated.earthquakes` →
  `curated.ai_reports`.
- Se documentará **qué es automático** (si se habilita OpenLineage) y **qué es declarado** vía API
  REST, sin afirmar que funciona sin evidencia en la UI.

## 10. Decisiones y trade-offs

- **Airflow y no cron**: reintentos por tarea, timeouts, dependencias por dato, backfill con estado,
  historial de ejecuciones y alertas. Un cron no ofrece nada de eso.
- **Spark para transformar**: el mismo código procesa 11 eventos de una hora o 2.000 de 30 días,
  con esquema, SQL y escritura particionada. Para 2.000 filas un `pandas` bastaría, pero el requisito
  exige Spark y demuestra el patrón de motor de transformación desacoplado del orquestador.
- **Modo `local[*]`**: sin master/worker, sin red de clúster, menos RAM y menos fallos. En producción
  se usaría Spark en Kubernetes/EMR con ejecutores separados.
- **MinIO + PostgreSQL**: el lake conserva el crudo y la historia; Postgres da integridad
  referencial, upsert y serving transaccional. Duplicar todo en ambos sería peor.
- **SSE y no WebSocket**: el flujo es unidireccional servidor→cliente; SSE reconecta solo y es más
  simple de operar.

### En producción cambiaría

- Managed Postgres con réplicas de lectura y PITR.
- Spark en clúster con ejecutores elásticos.
- Object storage con versionado, ciclo de vida y cifrado.
- Secretos en Vault/SSM en lugar de `.env`.
- Airflow con Celery/KubernetesExecutor y alertas reales (Slack/PagerDuty).
- OpenLineage automático en Airflow y Spark, y catálogo como parte del pipeline.
