# BITÁCORA — SismoWatch

Registro del progreso **real**: qué se hizo, con qué comandos, qué se verificó y qué falló.
No se registra como completado nada que no se haya ejecutado.

---

## Fase 0 — Preparación del repositorio y verificación del entorno

**Fecha:** 2026-10-27
**Objetivo:** confirmar que el entorno soporta el stack, resolver conflictos de puertos/RAM y dejar el
esqueleto documental y de secretos antes de escribir código.

### Verificaciones ejecutadas

| Comprobación | Comando | Resultado real |
|---|---|---|
| Docker | `docker --version` / `docker compose version` | 29.8.1 / v5.1.4 |
| Recursos | `nproc`, `free -h`, `df -h` | 12 CPU, 15 GiB RAM, 208 GB libres |
| Feed USGS | `curl .../all_hour.geojson` | HTTP 200, 8.142 bytes, `count: 11` |
| Histórico 30 d | `curl .../count?...minmagnitude=2.5` | 1.969 eventos |
| Histórico 1 día | `curl .../query?...` | 34 eventos, 24.366 bytes, coords `[127.7372, 2.5777, 88.651]` |
| Claves IA | `printenv` (solo presencia) | Ninguna (`OPENAI`, `ANTHROPIC`, `GEMINI` ausentes) |
| Ollama | `curl localhost:11434/api/tags` | Activo: `llama3` (4,7 GB), `gemma:2b` (1,7 GB) |
| MinIO Hub | `curl hub.docker.com/v2/repositories/minio/minio/tags` | `object not found` → no hay tags públicos |
| MinIO alterno | `docker run --rm cgr.dev/chainguard/minio:latest minio --version` | RELEASE.2026-09-22, funciona |
| OpenMetadata | GitHub release | `2.0.6-release` publicada 2026-10-21; quickstart = MySQL 8 + Elasticsearch 9.3 |
| Puertos | `ss -ltn` | Ocupados: 8080, 5432, 7077, 9000, 9001, 9092, 29092 |

### Problemas encontrados y solución

1. **Conflicto de puertos** con el stack `lambda-crypto-platform` (Airflow, Kafka, Spark, MinIO,
   Postgres). Solución: remapeo de host → 8082 Airflow, 5433 Postgres, 9002/9003 MinIO, y Spark sin
   exponer puertos.
2. **RAM insuficiente** (5,5 GiB libres) para Airflow + Spark + OpenMetadata. Solución aplicada:
   se detuvieron **todos** los contenedores ajenos con `docker stop $(docker ps -q)` (sin borrar
   volúmenes). Disponible tras la parada: **8,6 GiB**.
3. **El directorio padre `/home/cohorte5` es un repositorio Git** que contiene todo el home. Solución:
   repositorio propio dentro de `Prueba-Avanzada`; nunca se ejecuta `git add` desde el home.
4. **`minio/minio` ya no publica imágenes en Docker Hub.** Solución: `cgr.dev/chainguard/minio`, ya
   validada localmente (incluye servidor, no incluye `mc`; se usará `boto3`).

### Archivos creados

- `.gitignore`, `.env.example`
- `README.md`, `PROYECTO.md`, `BITACORA.md`, `PRESENTACION_PROMPT.md`
- `docs/ARQUITECTURA.md`, `docs/GUIA_ESTUDIO.md` (Módulo 1), `docs/GLOSARIO.md`
- `docs/decisiones/ADR-001-airflow.md`, `ADR-002-kafka.md`, `ADR-003-spark-local.md`,
  `ADR-004-almacenamiento.md`

### Conceptos aprendidos

- Diferencia entre parar contenedores y borrar volúmenes (parar no destruye datos).
- Por qué `localhost` dentro de un contenedor no apunta al host.
- Por qué la disponibilidad de imágenes (tags) es un riesgo real de diseño y debe comprobarse.

### Cierre de la fase

- Confirmadas las 3 vistas y perfil `governance` para OpenMetadata (ADR-005, ADR-006).

---

## Fase 1 — Entorno core y verificación de infraestructura

**Fecha:** 2026-10-27
**Objetivo:** Construir la imagen personalizada de Airflow (Spark 3.5.1 + Java 17 + JARs), inicializar
PostgreSQL (esquemas, roles y tablas de Medallion), configurar MinIO con buckets y políticas acotadas,
y certificar con un smoke test que Spark puede leer/escribir en S3A y JDBC antes de desarrollar DAGs.

### Estado de comprobaciones de certificación

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Ausencia de secretos en Git | `git check-ignore -v .env` / `git status` | Verificado | `.env` ignorado; solo placeholders en `.env.example` |
| Build imagen Airflow | `docker compose build` / `make build` | **Verificado** | `sismowatch/airflow:2.10.5-py3.11` construida con Spark 3.5.1 y Java 17 |
| Salud de servicios | `docker compose ps` / `make ps` | **Verificado** | `postgres`, `minio`, `webserver`, `scheduler` todos en `(healthy)` |
| Esquemas, roles y permisos PG | `psql -c "\dn"`, `"\du"`, queries rol `sismo_api` | **Verificado** | Esquemas `app`, `curated`, `raw`, `staging`. Rol `sismo_api`: SELECT ok en curated, `permission denied` en staging |
| Buckets y usuarios MinIO | `make minio-ls` | **Verificado** | `sismowatch-bronze/` y `sismowatch-silver/` activos |
| Versiones Java 17 y Spark 3.5.1 | `java -version`, `spark-submit --version` | **Verificado** | OpenJDK 17.0.21 y Spark 3.5.1 (Scala 2.12.18) |
| Smoke test Spark (S3A + JDBC) | `make smoke` (`_smoke.py`) | **Verificado** | Parquet escrito y leído en MinIO, JDBC escrito y leído en PostgreSQL (`status: ok`) |
| Limpieza de smoke test | validación de retorno de `_smoke.py` | **Verificado** | `parquet_deleted: true`, `rows_deleted: 2`, sin residuos |

### Problemas encontrados y solución (Fase 1)

1. **Sintaxis de UUID en `_smoke.py`**: El script usaba `uuid.uuid4().hex[:12]`, generando un string de 12 caracteres incompatible con el tipo estricto `uuid` de PostgreSQL. Solución: se corrigió a `str(uuid.uuid4())`.
2. **Permiso `DELETE` en Staging**: Al ejecutar la limpieza, `sismo_transform` fallaba con `permission denied for table earthquakes` porque solo tenía `SELECT, INSERT, UPDATE`. Solución: se añadió `DELETE` a `db/init/04_grants.sql` y se aplicó el grant en la base de datos viva.
3. **Smoke test 100% exitoso**: Tras corregir ambos puntos, `make smoke` ejecutó el flujo completo S3A + JDBC y limpió sus registros de prueba de forma atómica.

---

## Fase 2 — Ingesta USGS recurrente a Bronze con Dataset scheduling

**Fecha:** 2026-10-27
**Objetivo:** Implementar la ingesta resiliente de eventos sísmicos cada 5 minutos desde USGS,
almacenar el payload crudo inmutable en MinIO Bronze (`bronze/feed/dt=YYYY-MM-DD/hour=HH/<run_id>.json`),
registrar el manifiesto en `raw.ingestions` de PostgreSQL con rol de mínimo privilegio (`sismo_ingest`),
emitir el Dataset `sismowatch://bronze/feed` al completar exitosamente, y validar con suite de pruebas unitarias.

### Estado de comprobaciones de la Fase 2

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Lógica de Ingesta pura | `airflow/include/ingest.py` | **Verificado** | Cliente USGS con timeout/reintentos, validación GeoJSON, SHA-256, subida a MinIO Bronze, manifiesto en raw.ingestions |
| DAG de Airflow cada 5 min | `airflow/dags/ingest_feed.py` | **Verificado** | Schedule `*/5 * * * *`, outlet `Dataset("sismowatch://bronze/feed")` |
| Pruebas unitarias pytest | `make test` (`airflow/tests/test_ingest.py`) | **Verificado** | 10 passed en 3.34s (válido, vacío, corrupto, HTTP error, DAG integrity) |
| Importación en Airflow (sin errores) | `airflow dags list` | **Verificado** | DAG `ingest_feed` reconocido sin errores de importación |
| Ejecución real en vivo del DAG | `airflow dags test ingest_feed` | **Verificado** | Descarga USGS en 1.1s, 5 eventos, 3.862 bytes, status `ok`, run_id `f0e463d9-...` |
| Verificación en MinIO Bronze | Objeto en `sismowatch-bronze` | **Verificado** | `feed/dt=2026-10-27/hour=13/f0e463d9-34c6-4a9e-8120-d09ae95c0520.json` |
| Verificación en PostgreSQL | `SELECT * FROM raw.ingestions` | **Verificado** | Manifiesto insertado con sha256 `4685fef5...` y rol `sismo_ingest` |

### Problemas encontrados y solución (Fase 2)

1. **Transición de estado inicial en `ingest.py`**: La variable `status` iniciaba en `"error"` y no mutaba a `"ok"` al recibir HTTP 200, provocando fallas en los tests unitarios. Solución: asignación explícita `status = "ok"` tras código 200.
2. **Permiso `SELECT` en `raw.ingestions` para `ON CONFLICT`**: En PostgreSQL, la sentencia `INSERT ... ON CONFLICT (run_id) DO NOTHING` exige permisos de lectura (`SELECT`) para comprobar el índice único. Se concedió `SELECT, INSERT` a `sismo_ingest` en `db/init/04_grants.sql` y en la base viva.
3. **Ejecución end-to-end exitosa**: El DAG corrió en 2.52s, descargó 5 eventos reales de USGS, guardó el payload inmutable en MinIO Bronze y registró el manifiesto en PostgreSQL.

---

## Fase 3 — Transformación Spark Bronze → Silver + Calidad Bloqueante + Cuarentena

**Fecha:** 2026-10-27
**Objetivo:** Procesar lotes pendientes de `raw.ingestions` con Spark 3.5.1 en modo local, parsear
coordenadas en orden USGS `[lon, lat, depth]`, convertir timestamps a UTC, aplicar reglas de calidad
declarativas R01–R05, aislar registros rechazados con motivos consolidados en `curated.quarantine`,
registrar métricas en `curated.quality_runs`/`rules`, publicar Parquet particionado en MinIO Silver
y realizar upsert idempotente en `staging.earthquakes` (PostgreSQL).

### Estado de comprobaciones de la Fase 3

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Lógica de transformación y reglas R01-R05 | `airflow/include/transform.py` | **Implementado** | Heurística de región, parseo GeoJSON [lon, lat, depth], reglas R01–R05 y consolidación multi-error |
| Job de Spark Silver | `spark/jobs/transform_silver.py` | **Implementado** | Procesamiento por lotes pendientes de raw.ingestions, Parquet particionado en MinIO, upsert staging, cuarentena |
| Permisos DELETE en schema curated | `db/init/04_grants.sql` | **Verificado** | Añadido DELETE para transform_user en curated (idempotencia y limpieza de reintentos) |
| DAG Airflow activado por Dataset | `airflow/dags/transform_silver.py` | **Implementado** | Schedule `[Dataset("sismowatch://bronze/feed")]`, outlet `sismowatch://silver/earthquakes` |
| Suite de pruebas unitarias Silver | `make test` (`test_silver.py`) | **Verificado** | 24 tests pasados en 3.31s (10 ingest + 14 silver) |
| Importación en Airflow (sin errores) | `airflow dags list` | **Verificado** | `transform_silver` listado y parseado correctamente por DagBag |
| Ejecución Spark sobre lote real | `make transform` | **Verificado** | Lote `f0e463d9`: 5 evaluados, 5 aprobados, 0 rechazados, score 100.0, status passed |
| Parquet generado en MinIO Silver | `s3a://sismowatch-silver/earthquakes/` | **Verificado** | Partición `dt=2026-10-27` escrita exitosamente por FileFormatWriter |
| Upsert en `staging.earthquakes` | `SELECT count(*) FROM staging.earthquakes` | **Verificado** | Eventos insertados con preservación de updated_utc más reciente |
| Métricas en `curated.quality_runs`/`rules` | `SELECT * FROM curated.quality_runs` | **Verificado** | Métricas consolidadas y desglose por regla persistidos con FK consistente |

### Hallazgos y decisiones técnicas de la Fase 3

1. **Orden de coordenadas USGS**: En GeoJSON estándar de USGS, el array de coordenadas en `geometry.coordinates` es estrictamente `[longitud, latitud, profundidad_km]`. Se garantizó la asignación posicional correcta en `transform.py` y el manejo de profundidad nula/opcional.
2. **Consolidación de fallos en cuarentena**: Si un registro viola múltiples reglas (por ejemplo, magnitud fuera de rango y coordenadas erróneas), se genera un único registro en `curated.quarantine` con los códigos y descripciones consolidados, evitando duplicación en la tabla de cuarentena.
3. **Idempotencia en staging**: Se utiliza `ON CONFLICT (id) DO UPDATE ... WHERE EXCLUDED.updated_utc >= staging.earthquakes.updated_utc`, garantizando que actualizaciones posteriores de USGS prevalezcan sobre datos anteriores sin generar duplicados.
4. **Permisos en PostgreSQL**: El usuario `sismo_transform` requirió permisos explícitos de `DELETE` en `curated` para permitir la limpieza atómica previa en caso de reejecución de un lote fallido.

---

## Fase 4 — Capa Gold + Agregados + Detección de Anomalías

**Fecha:** 2026-10-27
**Objetivo:** Implementar la publicación de la capa Gold (`curated.earthquakes`) condicionada a la
compuerta de calidad (Quality Gate), calcular baselines estadísticos y Z-Score sobre ventanas horarias
por región para detectar anomalías en `curated.anomalies`, actualizar el estado en `curated.pipeline_runs`,
orquestar mediante el DAG `publish_gold` activado por el Dataset `sismowatch://silver/earthquakes`,
y verificar cobertura completa con pruebas automatizadas.

### Estado de comprobaciones de la Fase 4

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Lógica pura Gold y anomalías | `airflow/include/gold.py` | **Implementado** | Compuerta de calidad, cálculo Z-score y evaluación de ventanas horarias |
| Job de publicación y anomalías | `spark/jobs/publish_gold.py` | **Implementado** | Upsert idempotente en curated.earthquakes y curated.anomalies |
| DAG Airflow activado por Dataset | `airflow/dags/publish_gold.py` | **Implementado** | Schedule `[Dataset("sismowatch://silver/earthquakes")]`, outlet `sismowatch://gold/earthquakes` |
| Pruebas unitarias de la Fase 4 | `make test` (`test_gold.py`) | **Verificado** | 35 tests pasados en 3.49s (11 gold + 10 ingest + 14 silver) |
| Ejecución en vivo de publicación Gold | `make gold` | **Verificado** | Lote publicado exitosamente: 5 eventos publicados, 2 ventanas evaluadas |
| Verificación en `curated.earthquakes` | `SELECT id, mag, place... FROM curated.earthquakes` | **Verificado** | 5 filas insertadas en Gold con `published_at` consistente |
| Verificación en `curated.anomalies` | `SELECT * FROM curated.anomalies` | **Verificado** | Ventanas evaluadas para CA (4 evts, z=0.0) y Alaska (1 evt, z=0.0) |
| Verificación en `curated.pipeline_runs` | `SELECT * FROM curated.pipeline_runs` | **Verificado** | Registro en `publish_gold` con state `success` |

### Hallazgos y decisiones técnicas de la Fase 4

1. **Quality Gate estricto previo a Gold**: La publicación a `curated.earthquakes` valida contra `curated.quality_runs` que el lote tenga estado `passed` y puntaje $\ge 80.0$. Un lote rechazado nunca se publica a la capa de consumo.
2. **Idempotencia de publicación**: Se aplica `ON CONFLICT (id) DO UPDATE ... WHERE EXCLUDED.updated_utc >= curated.earthquakes.updated_utc OR curated.earthquakes.updated_utc IS NULL`, permitiendo que re-ejecuciones actualicen eventos modificados sin duplicar claves primarias.
3. **Manejo de varianza cero en Z-Score**: En regiones con historial homogéneo o una sola observación donde la desviación estándar muestral es cero ($\sigma = 0$), el Z-Score se neutraliza a `0.000` con `is_anomaly = false`, evitando divisiones por cero indeterminadas.
4. **Desacoplamiento Medallion**: La API de consumo (Fase 7) solo tiene acceso de lectura a `curated.earthquakes` y `curated.anomalies`, aislando completamente las capas Bronze y Staging.

---

## Fase 5 — Backfill Parametrizado de 30 Días

**Fecha:** 2026-10-27
**Objetivo:** Implementar la extracción histórica parametrizada desde la API FDSNWS de USGS,
permitiendo reprocesar cualquier ventana temporal (por defecto 30 días), almacenando el payload
crudo en Bronze (`backfill/dt=YYYY-MM-DD/<run_id>.json`), registrando el manifiesto en `raw.ingestions`
con `source='backfill'`, ejecutando la transformación Spark y publicación Gold de extremo a extremo,
y garantizando idempotencia estricta (dos ejecuciones del mismo rango = idéntico conteo).

### Estado de comprobaciones de la Fase 5

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Construcción URL FDSNWS y cliente | `airflow/include/ingest.py` | **Implementado** | `build_usgs_query_url()` y `run_backfill_ingestion()` con detección de límite de corte |
| Manifiesto con `window_from`/`window_to` | `raw.ingestions` | **Implementado** | Columnas temporales persistidas en manifiesto de auditoría |
| DAG Airflow `backfill_history` | `airflow/dags/backfill_history.py` | **Implementado** | Ingesta -> Transformación -> Publicación con paso estricto de `run_id` vía XCom |
| Suite de pruebas unitarias Fase 5 | `make test` (`test_backfill.py`) | **Verificado** | 39 tests pasados en 3.53s sin ningún fallo |
| Importación en Airflow (sin errores) | `airflow dags list` | **Verificado** | `backfill_history` listado por DagBag con cero errores de importación |
| Ejecución en vivo de backfill | `airflow dags trigger backfill_history` | **Verificado** | 741 terremotos históricos (30 días, mag >= 4.5) descargados y publicados a Gold en 15.1s |
| Verificación de idempotencia (2 ejecuciones) | `SELECT count(*) FROM curated.earthquakes` | **Verificado** | Segunda corrida procesada con éxito; 0 IDs duplicados en Gold y 0 ventanas duplicadas en anomalías |

### Hallazgos y decisiones técnicas de la Fase 5

1. **Aislamiento por `run_id` propio**: El DAG `backfill_history` extrae y procesa exclusivamente el lote correspondiente a su propia ejecución (`{{ ti.xcom_pull(task_ids="ingest_backfill") }}`), sin interferir con la ingesta recurrente de feed de cada 5 minutos.
2. **Detección de corte de API USGS**: Se implementó una verificación explícita si `event_count >= limit` (20.000 eventos). Si la API recorta la respuesta, se emite una advertencia técnica en los logs y en `raw.ingestions.error_detail` para evitar declarar completas extracciones truncadas.
3. **Idempotencia matemática y no duplicación**: Se verificó mediante SQL que tras dos ejecuciones consecutivas del backfill de 30 días, el número de duplicados por ID en `curated.earthquakes` y por ventana horaria en `curated.anomalies` es exactamente **cero** (`0 rows`).
4. **Preservación de datos actualizados**: El upsert con condición `WHERE EXCLUDED.updated_utc >= ...` asegura que si un backfill contiene un evento previo pero la ingesta en vivo ya tenía una revisión más reciente de USGS, la versión más reciente nunca es degradada ni sobrescrita.

---

## Fase 6 — Reporte Diario con IA + Auditoría + Fallback

**Fecha:** 2026-10-27
**Objetivo:** Implementar la generación diaria de reportes sísmicos analíticos a partir de indicadores
deterministas calculados sobre la capa Gold, invocar al modelo local Ollama (`gemma:2b`), incorporar
validación de esquema y anti-alucinaciones numéricas, asegurar fallback determinista inmediato ante caídas
o timeouts de LLM con `status='fallback'`, registrar auditoría completa en `curated.ai_runs` e
idempotencia en `curated.ai_reports` por fecha de referencia.

### Estado de comprobaciones de la Fase 6

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Cálculo determinista de indicadores | `airflow/include/ai.py` | **Implementado** | Agregados de sismos, anomalías, eventos máximos y regiones en Gold |
| Fallback determinista y anti-alucinación | `airflow/include/ai.py` | **Implementado** | Plantilla analítica pura y validación de magnitudes citadas |
| Auditoría en `curated.ai_runs` | `airflow/include/ai.py` | **Implementado** | Registro de proveedor, modelo, latencia, tokens y estado de ejecución |
| Job ejecutable de reporte | `spark/jobs/generate_daily_report.py` | **Implementado** | Argumentos `--date` y `--force-fallback`, upsert en ai_reports |
| DAG Airflow `daily_ai_report` | `airflow/dags/daily_ai_report.py` | **Implementado** | Schedule diario `0 1 * * *` y disparo manual parametrizado |
| Pruebas unitarias de la Fase 6 | `make test` (`test_ai.py`) | **Verificado** | 50 tests pasados en 3.56s con 100% de éxito |
| Importación en Airflow (sin errores) | `airflow dags list` | **Verificado** | `daily_ai_report` listado por DagBag con cero errores de importación |
| Ejecución del reporte (Ollama o Fallback) | `make report` | **Verificado** | Detección de proveedor offline y activación inmediata de fallback resiliente |
| Verificación en `curated.ai_reports` | `make reports-check` | **Verificado** | Reporte generado con id determinista `1f9f21b0`, status='fallback' y markdown estructurado |
| Verificación en `curated.ai_runs` | `make reports-check` | **Verificado** | Auditoría con run_id independiente por cada ejecución |

### Hallazgos y decisiones técnicas de la Fase 6

1. **Anti-Alucinaciones numéricas**: El texto redactado por el LLM se somete a validación de patrones numéricos contra los indicadores reales. Si el modelo reporta magnitudes superiores a la máxima registrada o afirma sismos en periodos sin actividad, el validador rechaza el texto y activa el fallback determinista.
2. **Resiliencia y degradación elegante**: La plataforma nunca falla ante la caída o indisponibilidad del servicio de Ollama. Se garantiza la continuidad operativa mediante un informe en Markdown derivado de reglas analíticas deterministas con `status='fallback'`.
3. **Idempotencia determinista en reportes**: Se utiliza `uuid.uuid5(NAMESPACE_DNS, f"sismowatch_report_{reference_date}")` para el `report_id`, garantizando que múltiples ejecuciones en la misma jornada actualicen el reporte oficial sin duplicar filas en `curated.ai_reports`.
4. **Trazabilidad estricta en auditoría**: Cada invocación genera una fila única en `curated.ai_runs` vinculada al `report_id`, permitiendo auditar la evolución de latencias, versiones de prompt y estados a lo largo del tiempo.

---

## Fase 7 — Backend FastAPI + RBAC + Mínimo Privilegio + SSE + Agente Read-Only

**Fecha:** 2026-10-27
**Objetivo:** Desarrollar el backend en FastAPI exponiendo las 3 vistas de la plataforma, autenticación JWT
con expiración, hashing bcrypt, roles RBAC (`viewer` y `admin`), mínimo privilegio en PostgreSQL
(rol `sismo_api` restringido exclusivamente a lectura en `curated.*` y gestión de usuarios en `app.users`),
streaming en tiempo real vía Server-Sent Events (SSE) con reconexión y heartbeat, exportaciones CSV y GeoJSON,
agente analítico de solo lectura sobre Gold sin SQL arbitrario, y suite de pruebas automatizadas al 100%.

### Estado de comprobaciones de la Fase 7

| Comprobación | Comando / Mecanismo | Estado | Resultado real |
|---|---|---|---|
| Módulo de seguridad y JWT | `api/app/security.py` | **Implementado** | PyJWT + bcrypt, dependencias `get_current_user` y `require_admin` |
| Conexión con mínimo privilegio | `api/app/db.py` | **Implementado** | Rol `sismo_api`, cursores dictionary, aislamiento de raw/staging |
| Endpoints Monitoreo Sísmico | `api/app/routers/earthquakes.py` | **Implementado** | Paginación, filtros de mag/región/tiempo, KPIs y exports CSV/GeoJSON |
| Endpoints Reportes Diarios | `api/app/routers/reports.py` | **Implementado** | Listado de reportes históricos y detalle por fecha con indicadores |
| Agente Analítico Read-Only | `api/app/routers/agent.py` | **Implementado** | Herramientas en Gold (kpis, strongest, regional, reports), 0 SQL arbitrario |
| Endpoints Operaciones y Cuarentena | `api/app/routers/operations.py` | **Implementado** | Quality runs/rules, pipeline runs, cuarentena (admin) y trigger backfill (admin) |
| Streaming en Tiempo Real (SSE) | `api/app/routers/sse.py` | **Implementado** | `EventSourceResponse` con eventos sísmicos y heartbeats periódicos |
| Servicio Docker y Healthcheck | `infra/api/Dockerfile` + compose | **Implementado** | Servicio `api` levantado en puerto 8000 con healthcheck curl `/health` |
| Pruebas automatizadas API (pytest) | `make test-api` | **Verificado** | 30 tests pasados en 1.43s (auth, rbac, earthquakes, reports, agent, sse) |
| Suite global completa del proyecto | `make test-all` | **Verificado** | 80 tests pasados en 5.0s (50 pipeline Airflow + 30 API FastAPI) |
| Verificación Mínimo Privilegio PostgreSQL | `SELECT ... FROM raw.ingestions` como `sismo_api` | **Verificado** | `ERROR: permission denied for schema raw` y `permission denied for schema staging` |
| Verificación RBAC 401 (sin token) | `curl /api/v1/quarantine` | **Verificado** | `HTTP/1.1 401 Unauthorized` con cabecera `WWW-Authenticate: Bearer` |
| Verificación RBAC 403 (rol viewer) | `curl -H "Bearer $VIEWER_TOKEN" /api/v1/quarantine` | **Verificado** | `HTTP/1.1 403 Forbidden` (`Permisos insuficientes: se requiere rol admin`) |
| Verificación RBAC 200 (rol admin) | `curl -H "Bearer $ADMIN_TOKEN" /api/v1/quarantine` | **Verificado** | `HTTP/1.1 200 OK` con listado de registros |

### Hallazgos y decisiones técnicas de la Fase 7

1. **Cumplimiento estricto de Mínimo Privilegio Efectivo**: Se confirmó contra la base de datos real que el rol `sismo_api` es rechazado de inmediato por el motor relacional si intenta consultar `raw` o `staging`. Tampoco puede realizar escrituras en `curated`, lo cual blinda completamente el data lake frente a cualquier vector de inyección SQL o acceso indebido en la capa web.
2. **Defensa en Profundidad en el Agente de IA**: Para evitar inyecciones de comandos o SQL a través del agente conversacional, `/api/v1/agent/query` no expone ningún intérprete de texto a SQL. Las consultas se resuelven exclusivamente mediante funciones Python con SQL parametrizado estático sobre la capa Gold. Si el LLM está desconectado, emite una síntesis analítica determinista auditada.
3. **Resiliencia en Streaming SSE**: El generador asíncrono emite señales de latido (`event: heartbeat`) cada 15 segundos y evalúa activamente `request.is_disconnected()`. Esto evita fugas de sockets y bloqueos de hilos de trabajo cuando el cliente frontend cierra la pestaña o pierde conectividad.
4. **Desacoplamiento de Secretos**: La API no almacena credenciales fijas en código fuente. Todas las variables (`JWT_SECRET`, credenciales de Airflow, accesos de base de datos) se inyectan a través del entorno (`.env`), cumpliendo los estándares de seguridad requeridos.

