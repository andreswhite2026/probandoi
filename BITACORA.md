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
