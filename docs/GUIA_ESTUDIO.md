# Guía de estudio — SismoWatch

Guía progresiva. Cada módulo se completa **cuando la funcionalidad existe y fue verificada**, no
antes: así lo que estudias es lo que el proyecto realmente hace.

| Módulo | Tema | Cuándo se completa | Estado |
|---|---|---|---|
| 1 | Fundamentos de la arquitectura | Fase 0 | **Completado** |
| 2 | Docker y Docker Compose | Fase 1 | Pendiente |
| 3 | Apache Airflow | Fase 2-3 | Pendiente |
| 4 | Apache Spark | Fase 3-4 | Pendiente |
| 5 | MinIO y almacenamiento | Fase 1-2 | Pendiente |
| 6 | PostgreSQL | Fase 1-4 | Pendiente |
| 7 | OpenMetadata y linaje | Fase 9 | Pendiente |
| 8 | Kafka (decisión de no usarlo) | Fase 0 | Ver ADR-002 |
| 9 | Calidad de datos | Fase 3 | Pendiente |
| 10 | IA y agente | Fase 6-7 | Pendiente |
| 11 | API y frontend | Fase 7-8 | Pendiente |
| 12 | Testing y diagnóstico | Fase 3 en adelante | Pendiente |
| 13 | Recorrido completo | Fase 10 | Pendiente |

---

## Módulo 1. Fundamentos de la arquitectura

### 1.1 ¿Qué es ingeniería de datos?

Es la disciplina que construye y opera las tuberías que llevan datos desde donde se producen hasta
donde se usan, garantizando que lleguen **completos, correctos, a tiempo y con trazabilidad**.

Analogía: una planta potabilizadora. El agua llega turbia (fuente), se filtra y se analiza
(transformación y calidad) y se sirve potable (consumo). Nadie bebe directamente del río.

En SismoWatch: USGS es el río, Airflow es el operario que abre la válvula cada 5 minutos, Spark es la
planta de filtrado, Postgres es el tanque de agua lista y la API es el grifo.

### 1.2 ¿Qué es un pipeline?

Un **pipeline** es una secuencia de etapas donde la salida de una es la entrada de la siguiente. Se
modela como un grafo dirigido sin ciclos (DAG): cada nodo es una tarea, cada flecha una dependencia.

```text
extraer → validar → guardar crudo → transformar → validar calidad → publicar → consumir
```

Dos propiedades que se evalúan siempre:

- **Idempotencia**: repetir una etapa no cambia el resultado.
- **Trazabilidad**: cada fila final se puede rastrear hasta su origen.

### 1.3 ¿Qué es la arquitectura Medallion?

Es una forma de organizar el dato en tres calidades crecientes, de modo que cada capa tenga una
responsabilidad clara y se pueda reprocesar sin volver a la fuente.

| Capa | Pregunta que responde | Estado del dato |
|---|---|---|
| **Bronze** | ¿Qué llegó exactamente? | Crudo, inmutable, sin interpretar |
| **Silver** | ¿Qué significa y es válido? | Tipado, normalizado, deduplicado |
| **Gold** | ¿Qué se puede consumir? | Agregado, aprobado, listo para negocio |

En este proyecto los nombres físicos son `raw`, `staging` y `curated` (los exige el enunciado):

- **Bronze = raw**: el GeoJSON tal como lo devolvió USGS.
- **Silver = staging**: `staging.earthquakes`, con `mag` numérica, `time_utc` como `timestamptz`,
  coordenadas separadas y región derivada.
- **Gold = curated**: `curated.earthquakes` + agregados + anomalías + reportes de IA.

¿Por qué no transformar directamente de la fuente a Gold? Porque si una regla cambia o un cálculo
está mal, sin Bronze no puedes reprocesar: tendrías que volver a pedirle los mismos datos a USGS, que
solo conserva una ventana limitada. Bronze es tu seguro.

### 1.4 Batch y streaming

- **Batch**: procesas un conjunto acotado cada cierto tiempo (por ejemplo, todo lo de la última hora).
- **Streaming**: procesas eventos continuamente, uno a uno, con latencia de segundos.

En SismoWatch el núcleo es **micro-batch de 5 minutos**: USGS publica un feed que se consulta por
polling, el backfill es histórico y el reporte es diario. No hay un productor de eventos propio que
exija un bus de streaming. Por eso Kafka no se usa (ver `docs/decisiones/ADR-002`).

El tiempo real del dashboard **no** implica streaming en el backend: la API empuja por SSE los
eventos que ya están en Gold, consultando cada pocos segundos.

### 1.5 Idempotencia en este proyecto

Un evento sísmico puede **cambiar después de publicarse** (USGS revisa la magnitud, el estado, etc.).
Por eso `id` es la clave natural y toda escritura es un **upsert**:

```sql
INSERT INTO staging.earthquakes (id, mag, ...) VALUES (...)
ON CONFLICT (id) DO UPDATE SET mag = EXCLUDED.mag, ...
WHERE staging.earthquakes.updated_utc IS NULL
   OR EXCLUDED.updated_utc > staging.earthquakes.updated_utc;
```

Consecuencia práctica: ejecutar dos veces el mismo backfill deja el mismo número de filas. Es uno de
los criterios de aceptación y se prueba de forma automatizada.

### 1.6 Linaje y catálogo

- **Linaje**: el mapa de "de dónde viene este dato y por qué transformaciones pasó". Permite responder
  "¿por qué el KPI de hoy dice 42 y ayer 40?" sin adivinar.
- **Catálogo**: el inventario buscable de activos (tablas, buckets, dashboards) con descripción,
  esquema, dueño y linaje. OpenMetadata cumple ese papel.

En SismoWatch el linaje mínimo es:

```text
raw.ingestions → staging.earthquakes → curated.earthquakes → curated.ai_reports
```

Y cada fila lleva `ingest_run_id` para poder llegar hasta el objeto JSON exacto en MinIO.

### 1.7 Comprobación rápida

1. ¿Por qué Bronze se guarda inmutable si después se transforma?
2. Si USGS corrige la magnitud de un evento ya cargado, ¿qué debe pasar en `staging` y en `curated`?
3. ¿Por qué Kafka no es necesario si el dashboard se actualiza en vivo?
4. ¿Qué diferencia hay entre "el DAG corrió" y "el dato es correcto"?
