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

### Pendientes al cierre de la fase

- Confirmar las 3 vistas y el uso de `profile: governance` para OpenMetadata (ver `PROYECTO.md` §5).
- Fase 1: `docker-compose.yml`, `db/init`, imagen de Airflow con Spark y smoke test.
