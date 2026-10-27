# Guía de estudio — SismoWatch

Guía progresiva. Cada módulo se completa **cuando la funcionalidad existe y fue verificada**, no
antes: así lo que estudias es lo que el proyecto realmente hace.

| Módulo | Tema | Cuándo se completa | Estado |
|---|---|---|---|
| 1 | Fundamentos de la arquitectura | Fase 0 | **Completado** |
| 2 | Docker y Docker Compose | Fase 1 | **Completado** |
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

---

## Módulo 2. Docker y Docker Compose

### 2.1 Contenedor, imagen, volumen, red

| Concepto | Qué es | Analogía | En SismoWatch |
|---|---|---|---|
| **Imagen** | Plantilla inmutable con el sistema de archivos y las dependencias | El molde de una figura | `sismowatch/airflow:2.10.5-py3.11` |
| **Contenedor** | Instancia en ejecución de una imagen | La figura ya moldeada | `sismowatch-airflow-scheduler` |
| **Volumen** | Almacenamiento gestionado por Docker que sobrevive al contenedor | Disco externo | `sismowatch_pgdata`, `sismowatch_minio_data` |
| **Red** | Red virtual privada donde los contenedores se ven por nombre | La oficina interna | `sismowatch-net` |
| **Puerto publicado** | Ventana del host hacia un puerto del contenedor | La puerta de la oficina | solo `8082 → 8080` (Airflow) |

Un contenedor es, técnicamente, un proceso de Linux aislado con *namespaces* (qué ve) y *cgroups*
(cuánto consume). No es una máquina virtual: comparte el kernel del host.

### 2.2 Por qué `localhost` dentro de un contenedor no es el host

Cada contenedor tiene su propia interfaz de red. Desde `airflow-scheduler`, `localhost` es
**el propio contenedor**, no el equipo. Por eso:

- La base de datos se llama `postgres` (nombre del servicio en la red de Compose), no `localhost`.
- MinIO se llama `minio:9000`.
- Para llegar al host (por ejemplo, a Ollama en el `:11434`) se usa `host.docker.internal`, que en
  Linux se resuelve con `extra_hosts: ["host.docker.internal:host-gateway"]`.

### 2.3 Los comandos, uno por uno

```bash
docker compose build          # construye la imagen propia (JDK + Spark + JAR)
docker compose up -d          # crea y arranca contenedores en segundo plano
docker compose ps             # estado + salud real de cada servicio
docker compose logs -f airflow-scheduler   # logs en vivo
docker compose exec postgres psql -U postgres -d sismowatch  # shell SQL
docker compose down           # detiene y elimina CONTENEDORES (los volúmenes quedan)
docker compose down -v        # ¡PELIGRO! además borra volúmenes (pierdes datos)
```

`-d` significa *detached*: el comando vuelve y los servicios siguen corriendo.

### 2.4 Healthcheck: "running" no es "funcional"

Un contenedor puede estar `Up` y no responder. El `healthcheck` ejecuta un comando periódico dentro
del contenedor y marca `healthy` solo si devuelve éxito:

| Servicio | Comando | Qué prueba |
|---|---|---|
| `postgres` | `pg_isready -U ... -d ...` | acepta conexiones SQL |
| `minio` | `bash -c 'exec 3<>/dev/tcp/127.0.0.1/9000'` | el puerto S3 acepta conexiones |
| `airflow-webserver` | `curl -f localhost:8080/health` | la UI responde |
| `airflow-scheduler` | `airflow jobs check --job-type SchedulerJob` | el scheduler está vivo |

`depends_on: condition: service_healthy` hace que un servicio **espere** a que el otro esté sano, no
solo creado. `service_completed_successfully` espera a que un contenedor de un solo uso termine bien
(aquí `airflow-init` y `minio-init`).

### 2.5 Qué hace cada archivo de esta fase

| Archivo | Responsabilidad |
|---|---|
| `docker-compose.yml` | Declara servicios, redes, volúmenes, salud y puertos |
| `infra/airflow/Dockerfile` | Construye la imagen con Java 17, PySpark 3.5.1 y los JAR |
| `db/init/00-run-sql.sh` | Ordena y ejecuta los SQL pasando las credenciales como variables |
| `db/init/01..04_*.sql` | Roles, esquemas, tablas y permisos |
| `infra/minio/init.sh` | Crea buckets y usuarios de MinIO con políticas acotadas |
| `spark/jobs/_smoke.py` | Prueba real de las conexiones (y limpia solo lo suyo) |
| `Makefile` | Atajos; no duplica lógica |

### 2.6 Variables de entorno y secretos

`.env` guarda valores reales y **está en `.gitignore`**; `.env.example` documenta las claves con
placeholders. Docker Compose lee `.env` automáticamente para interpolar `${VARIABLES}` y además lo
inyecta con `env_file`. Los `.sql` reciben las contraseñas como variables de `psql`, así no quedan
escritas en el repositorio.

Comprobación: `git check-ignore -v .env` debe responder que `.gitignore` lo ignora.

### 2.7 Diagnóstico de un servicio que no arranca

1. `docker compose ps` → ¿está `healthy` o `restarting`?
2. `docker compose logs --tail=50 <servicio>` → el error real suele estar en la última línea.
3. `docker compose config` → valida que el YAML y las variables interpolen.
4. **Conectividad**: desde otro contenedor, `getent hosts postgres` o `bash -c 'exec 3<>/dev/tcp/postgres/5432'`.
5. **Credenciales**: `psql` con el rol correcto; un `password authentication failed` no es un fallo de red.
6. **Permisos**: `permission denied for table` sí es un fallo de `GRANT`, no de conexión.

### 2.8 Comprobación rápida

1. ¿Qué diferencia hay entre `docker compose down` y `down -v`?
2. ¿Por qué la API no publica el puerto de PostgreSQL?
3. Si `airflow-scheduler` está `Up` pero `unhealthy`, ¿qué comando miras primero?
4. ¿Por qué el healthcheck de MinIO usa `bash` y no `curl`?
