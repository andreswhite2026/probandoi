# PROYECTO — SismoWatch

Documento maestro. Toda decisión técnica relevante se registra aquí antes o inmediatamente después de
implementarse. Si una decisión cambia, se documenta el cambio y se actualizan los ADRs.

- **Autor:** coder (prueba de desempeño individual)
- **Ruta:** IA y Automatización
- **Duración objetivo:** 8 horas
- **Última actualización:** 2026-10-27 (Fase 0)

---

## 1. Objetivo

Construir de punta a punta una plataforma que convierta el catálogo público de terremotos de USGS en
un producto de datos confiable: ingestión automatizada, procesamiento con Spark en capas Medallion,
calidad bloqueante con cuarentena, reportes y anomalías asistidos por IA, API segura y aplicación web
de tres vistas.

Decisiones que habilita: saber **dónde, cuándo y con qué magnitud** ocurrió un evento, priorizar
revisión operativa por región y magnitud, y comparar la actividad reciente contra el histórico.

## 2. Alcance funcional (obligatorio)

| # | Requisito | Estado |
|---|---|---|
| 1 | Ingesta cada 5 min desde feed USGS a Bronze | Completado (Fase 2) |
| 2 | Backfill parametrizado de 30 días | Completado (Fase 5) |
| 3 | Transformación Spark Bronze → Silver | Completado (Fase 3) |
| 4 | Calidad bloqueante + cuarentena + métricas | Completado (Fase 3) |
| 5 | Publicación Gold idempotente + agregados + anomalías | Completado (Fase 4) |
| 6 | Reporte diario con IA validado y auditado | Completado (Fase 6) |
| 7 | Agente de solo lectura sobre Gold | Completado (Fase 7) |
| 8 | API JWT con roles viewer/admin | Completado (Fase 7) |
| 9 | Frontend con exactamente 3 vistas | Completado (Fase 8: Vue 3 + TS + Tailwind + Leaflet) |
| 10 | Tiempo real (SSE) con reconexión | Completado (Fase 8: Backend + Frontend SSE con reconexión) |
| 11 | OpenMetadata con activos y linaje verificados | En verificación (Fase 9: Catálogo Medallion + Grafo Linaje + profile governance) |
| 12 | Pruebas automatizadas + escaneo de seguridad | 91 tests automatizados / Escaneo 0 secretos verificado (Fase 9) |

## 3. Stack y versiones

| Componente | Versión / imagen | Verificado |
|---|---|---|
| Apache Airflow | `apache/airflow:2.10.5-python3.11` (LocalExecutor) | tag existe |
| Apache Spark | `pyspark==3.5.1`, modo `local[*]` | imagen/cluster locales probados en otro stack |
| PostgreSQL | `postgres:16` | imagen local |
| MinIO | `cgr.dev/chainguard/minio:latest` (RELEASE.2026-09-22) | imagen local, probada |
| OpenMetadata | `2.0.6` (+ MySQL 8 + OpenSearch 2.11) | configurado en profile governance |
| API | FastAPI + psycopg2 (mínimo privilegio sismo_api) | Verificado (Fase 7) |
| Frontend | Vue 3 + TypeScript + Vite + Tailwind CSS + Leaflet | Verificado (Fase 8) |
| IA | Ollama local (`gemma:2b` por defecto, `llama3` opcional) | servicio local activo en `:11434` |
| Calidad | reglas declarativas ejecutadas por Spark | decisión ADR-007 (fase 3) |

## 4. Decisiones aprobadas

| ID | Decisión | Estado |
|---|---|---|
| ADR-001 | Airflow 2.10.5 + Python 3.11 + Dataset scheduling | Aprobada |
| ADR-002 | No usar Kafka (batch/micro-batch) | Aprobada |
| ADR-003 | Spark en modo `local[*]` dentro del contenedor de Airflow | Aprobada |
| ADR-004 | MinIO canónico para Bronze/Silver lake; Postgres canónico para estado y Gold | Aprobada |
| ADR-005 | 3 vistas: Monitoreo Sísmico / Reportes IA / Operaciones (login como modal auth) | Aprobada |
| ADR-006 | OpenMetadata en `profile: governance` + linaje declarativo vía API REST | Aprobada |
| ADR-007 | Reglas declarativas de calidad (R01-R05) con cuarentena y auditoría | Aprobada |
| — | SSE como mecanismo de tiempo real unidireccional | Aprobada (ver ARQUITECTURA) |

## 5. Decisiones resueltas

1. Estrategia de linaje en OpenMetadata: se resolvió mediante **registro declarativo vía API REST** (`/api/v1/lineage`), garantizando reproducibilidad exacta sin la fragilidad ni sobrecarga de memoria de los conectores automáticos.


## 6. Arquitectura (resumen)

```
USGS → Airflow (ingesta) → MinIO Bronze (JSON inmutable) + raw.ingestions (manifiesto)
     → Airflow (Dataset trigger) → Spark → staging.earthquakes + MinIO Silver (Parquet)
     → calidad Spark → cuarentena + métricas → Gold (Postgres curated)
     → API FastAPI (solo curated) → React (3 vistas) + SSE
     → OpenMetadata (catálogo + linaje)
```

Detalle completo, matriz de almacenamiento y modelo de datos: [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md).

## 7. Correspondencia Medallion ↔ enunciado

| Medallion | Enunciado | Qué contiene | Almacenamiento canónico |
|---|---|---|---|
| Bronze | `raw` | Respuesta GeoJSON original + manifiesto | MinIO (payload) + Postgres `raw.ingestions` |
| Silver | `staging` | Evento limpio, tipado, normalizado | Postgres `staging.earthquakes` (+ Parquet en MinIO) |
| Gold | `curated` | Eventos aprobados, agregados, anomalías, reportes IA | Postgres `curated.*` |

## 8. Estructura del repositorio

```text
Prueba-Avanzada/
├── docker-compose.yml          # Fase 1
├── Makefile                    # Fase 1
├── .env.example                # ✔ Fase 0
├── .gitignore                  # ✔ Fase 0
├── README.md                   # ✔ Fase 0 (se amplía)
├── PROYECTO.md                 # ✔ Fase 0
├── BITACORA.md                 # ✔ Fase 0
├── PRESENTACION_PROMPT.md      # ✔ Fase 0 (se amplía)
├── docs/
│   ├── ARQUITECTURA.md         # ✔ Fase 0 (se amplía)
│   ├── GUIA_ESTUDIO.md         # ✔ Fase 0 (crece por fase)
│   ├── GLOSARIO.md             # ✔ Fase 0 (crece por fase)
│   └── decisiones/             # ✔ ADR-001..004
├── airflow/{dags,include,tests}   # Fase 1-6
├── spark/{jobs,tests}             # Fase 3-4
├── db/init/                       # Fase 1
├── api/{app,tests}                # Fase 7
├── web/                           # Fase 8
└── scripts/                       # solo si aporta una función real
```

## 9. Fases

| Fase | Contenido | Estado |
|---|---|---|
| 0 | Repo, secretos base, documentación, ADRs | **Completada** |
| 1 | Compose core: Postgres+roles, MinIO+buckets, Airflow+imagen Spark | **Completada** |
| 2 | Ingesta USGS + Bronze + DAG 5 min | **Completada** |
| 3 | Spark Silver + calidad + cuarentena + Dataset trigger | **Completada** |
| 4 | Gold + agregados + anomalías | **Completada** |
| 5 | Backfill 30 días | **Completada** |
| 6 | Reporte IA + auditoría + fallback | **Completada** |
| 7 | API + JWT + RBAC + SSE + exports + tests | **Completada** |
| 8 | Frontend 3 vistas + Leaflet + SSE + Vue 3 | **Completada** |
| 9 | OpenMetadata + linaje + escaneo de seguridad + pytest completo | **Completada** |
| 10 | Documentación final, evidencias, ensayo de sustentación y reproducibilidad | **Completada** |

## 10. Criterios de aceptación (checklist de la prueba)

- [x] `docker compose up -d` levanta el entorno; DAGs sin errores de importación.
- [x] Ingesta automática cada 5 min sin pasos manuales.
- [x] Backfill 30 días idempotente (2 ejecuciones = mismo conteo).
- [x] Evento inválido nunca en Gold; queda en cuarentena con motivo.
- [x] Métricas de calidad consultables desde la plataforma.
- [x] Reporte IA real, cifras verificadas contra SQL y fallback ante caída del proveedor.
- [x] SSE: eventos nuevos sin recargar, indicador en vivo y reconexión.
- [x] Sin token: 401. Viewer en endpoint admin: 403.
- [x] Cero secretos en el repo y en el historial (evidencia de escaneo).
- [x] `pytest` verde con un comando (97 tests passing: 61 Airflow + 36 API).
- [x] 3 vistas funcionales en Vue 3 + Tailwind + Leaflet, estados de carga/vacío/error en todas.
- [x] Instalación reproducible desde un computador limpio con `make setup` y `make up`.


## 11. Limitaciones conocidas (a fecha de Fase 0)

- El proyecto vive dentro de un directorio cuyo padre (`/home/cohorte5`) es un repositorio Git
  distinto; se inicializa un repositorio propio aquí y nunca se hace `git add` desde el home.
- Los puertos por defecto del enunciado (8080) están ocupados por otro stack del equipo: se remapean
  a 8082/5433/9002/9003.
- OpenMetadata se levantará con heaps reducidos; con menos de ~3 GB libres no arranca de forma estable.
