# Guía de estudio — SismoWatch

Guía progresiva. Cada módulo se completa **cuando la funcionalidad existe y fue verificada**, no
antes: así lo que estudias es lo que el proyecto realmente hace.

| Módulo | Tema | Cuándo se completa | Estado |
|---|---|---|---|
| 1 | Fundamentos de la arquitectura | Fase 0 | **Completado** |
| 2 | Docker y Docker Compose | Fase 1 | **Completado** |
| 3 | Apache Airflow (Ingesta y Datasets) | Fase 2 | **Completado (Parte 1)** |
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

---

## Módulo 3. Apache Airflow (Ingesta y Datasets)

### 3.1 Qué hace Airflow y qué NO debe hacer
Airflow es un **orquestador**, no un motor de procesamiento.
- **Lo que hace:** Programa la agenda (`*/5 * * * *`), gestiona dependencias, reintenta tareas fallidas con backoff, monitorea la salud del pipeline y registra metadatos de ejecución.
- **Lo que NO hace:** No transforma datos masivos en memoria ni reemplaza a Spark o a la base de datos. En la Fase 2, la tarea de Python solo extrae el GeoJSON de USGS, lo valida estructuralmente, lo deposita en Bronze y escribe el manifiesto.

### 3.2 Dataset Scheduling vs TriggerDagRun
En Airflow tradicional se usaba `TriggerDagRunOperator` para encadenar tareas entre DAGs.
En SismoWatch (Airflow 2.10+) usamos **Dataset Scheduling** (ADR-001):
- `ingest_feed` declara un **outlet**: `outlets=[Dataset("sismowatch://bronze/feed")]`.
- El DAG de transformación (Fase 3) no se dispara por un horario fijo, sino cuando el Dataset `sismowatch://bronze/feed` recibe una actualización exitosa.
- **Ventaja de diseño:** Desacopla productor y consumidor. Si la ingesta falla, el Dataset **no** se emite y Spark no procesa datos inválidos. Si mañana queremos añadir otro consumidor (ej. un auditor externo), simplemente se suscribe al mismo Dataset sin modificar el DAG de ingesta.

### 3.3 Por qué ON CONFLICT requiere SELECT en PostgreSQL
Durante la certificación real de la Fase 2 observamos:
```text
psycopg2.errors.InsufficientPrivilege: permission denied for table ingestions
```
Al ejecutar `INSERT INTO raw.ingestions (...) ON CONFLICT (run_id) DO NOTHING`, el motor PostgreSQL necesita leer los índices de la tabla para determinar si la clave ya existe. Por esa razón, un rol con solo permiso de `INSERT` fallará. Para que la cláusula `ON CONFLICT` funcione con mínimo privilegio, el rol requiere tanto `INSERT` como `SELECT`.

### 3.4 Comprobación rápida
1. ¿Por qué un fallo en la descarga no emite el Dataset a Spark?
2. ¿Qué ventaja tiene guardar el archivo crudo en MinIO incluso si viene con un JSON corrupto?
3. ¿Por qué el cálculo del hash SHA-256 se realiza sobre los bytes crudos antes de cualquier parseo?

---

## Módulo 4. Spark, Medallion Silver y Calidad Bloqueante

### 4.1 Por qué Spark en modo local y no standalone / cluster
- En hardware acotado (~8 GB RAM de host para todo el stack de evaluación), levantar un cluster Spark distribuido (Master + N Workers) consumiría más de 3 GB solo en overhead de JVMs y procesos ociosos.
- `local[*]` ejecuta el driver y executors en la misma JVM dentro del contenedor de Airflow, aprovechando todos los cores disponibles con mínima huella de memoria (1 GB de heap asignado) y sin latencia de red de shuffle innecesaria.
- Si en producción el volumen creciera a decenas de millones de registros, el código PySpark se mantiene idéntico y únicamente se altera el `--master yarn` o `--master k8s://...`.

### 4.2 Orden de coordenadas USGS (GeoJSON estándar)
El formato GeoJSON (RFC 7946) y la API de USGS definen la geometría como `[longitud, latitud, elevación/profundidad]`.
- Longitud: rango [-180, 180] (eje X).
- Latitud: rango [-90, 90] (eje Y).
- Profundidad: en kilómetros (eje Z, opcional o nulo si no se registra).
Confundir latitud con longitud es un error clásico que desplaza los eventos a océanos o polos opuestos.

### 4.3 Reglas de calidad declarativas (R01–R05) y aislamiento en cuarentena
- **R01**: `id` no nulo ni vacío.
- **R02**: `time_utc` no nulo ni en el futuro (`time_utc <= now + 5 min`).
- **R03**: Coordenadas en rango válido (`-90 <= lat <= 90`, `-180 <= lon <= 180`).
- **R04**: Magnitud en rango admisible (`-2.0 <= mag <= 10.0` o nulo).
- **R05**: Tipo de evento conocido (`earthquake`, `quarry blast`, `explosion`, etc.).

**Aislamiento y no pérdida de datos:**
Los eventos que violan cualquiera de estas reglas **nunca** avanzan a `staging` ni a `curated.earthquakes`. Se aíslan en `curated.quarantine` preservando su payload `JSONB` original y consolidando los motivos de rechazo para auditoría y reentrenamiento.

### 4.4 Idempotencia en Staging (Upsert)
USGS puede actualizar metadatos de un terremoto minutos u horas después (por ejemplo, recalculando magnitud o profundidad).
Para no generar registros duplicados ni perder la actualización más precisa:
```sql
INSERT INTO staging.earthquakes (...) VALUES (...)
ON CONFLICT (id) DO UPDATE SET
    mag = EXCLUDED.mag,
    updated_utc = EXCLUDED.updated_utc,
    last_seen_at = now()
WHERE EXCLUDED.updated_utc >= staging.earthquakes.updated_utc;
```

### 4.5 Comprobación rápida
1. ¿Por qué los registros rechazados no se descartan simplemente con un `drop()` silencioso?
2. ¿Por qué si un registro falla en latitud y en magnitud debe registrarse una sola vez en cuarentena?
3. ¿Cómo garantiza `transform_silver.py` que una reejecución del mismo `run_id` no duplique métricas en `curated.quality_runs`?

---

## Módulo 5. Capa Gold, Compuertas de Calidad y Detección de Anomalías

### 5.1 Quality Gate: el firewall entre Staging y Gold
Staging (`staging.earthquakes`) contiene la totalidad de los datos estructurados limpios procesados por Spark. Sin embargo, los consumidores de la plataforma (FastAPI, React, reportes IA) **nunca** leen de Staging.
- **Principio de Quality Gate**: Antes de que un lote se publique en `curated.earthquakes`, el job valida que:
  1. `status == 'passed'` en `curated.quality_runs`.
  2. El puntaje ponderado de calidad sea mayor o igual al umbral requerido (`score >= 80.0`).
  3. El número de eventos aprobados sea mayor a cero (`approved > 0`).
- Si un lote falla en calidad, la compuerta se cierra: el lote queda en Staging/Cuarentena y **no se publica a Gold**.

### 5.2 Detección de anomalías por Z-Score ($z = \frac{x - \mu}{\sigma}$)
Para alertar sobre enjambres sísmicos o actividad anormal sin requerir modelos opacos:
- Se agrupan los eventos en ventanas de 1 hora por región (`date_trunc('hour', time_utc)`).
- Se calcula la media regional ($\mu$) y la desviación estándar muestral ($\sigma$).
- **Casos de borde estadísticos:**
  - Si una región sólo tiene una ventana o eventos idénticos ($\sigma = 0$), la división arrojaría error matemático o infinito; por diseño se neutraliza a $z = 0.0$ (`is_anomaly = false`).
  - Si el conteo es menor al promedio ($x \le \mu$), no representa una anomalía de alta actividad sísmica.
- **Alerta explicable:** Cuando $z \ge 2.5$, se marca `is_anomaly = true` y se genera una explicación en lenguaje natural con las cifras exactas para el operador del dashboard.

### 5.3 Comprobación rápida
1. ¿Por qué la API lee exclusivamente del schema `curated` y no de `staging`?
2. ¿Qué ocurriría si una región tiene un solo terremoto histórico al calcular la desviación estándar?
3. ¿Cómo evita `publish_gold.py` que dos ejecuciones concurrentes o repetidas generen duplicados en `curated.anomalies`?

---

## Módulo 6. Backfill Histórico, API FDSNWS e Idempotencia Estricta

### 6.1 Feed en tiempo real vs Backfill histórico
- **Feed (`summary/all_hour.geojson`)**: Diseñado para polling frecuente (cada 5 min). Contiene únicamente los eventos de la última hora en un tamaño muy liviano (~3–5 KB).
- **FDSNWS Query API (`fdsnws/event/1/query`)**: Diseñado para extracciones analíticas por rango de fechas (`starttime`, `endtime`), magnitud (`minmagnitude`) y volumen (`limit=20000`). Devuelve un GeoJSON de hasta decenas de megabytes.

### 6.2 Idempotencia estricta ($f(f(x)) = f(x)$)
Un pipeline de datos profesional debe ser capaz de reejecutar cualquier lote histórico sin corromper el estado:
1. **En MinIO Bronze**: El payload se archiva inmutablemente bajo una clave estructurada por fecha y `run_id` (`backfill/dt=YYYY-MM-DD/<run_id>.json`).
2. **En PostgreSQL Staging y Gold**: Cláusula `ON CONFLICT (id) DO UPDATE ... WHERE EXCLUDED.updated_utc >= ...`.
   - Si un evento ya existía, se actualiza sólo si la versión entrante es más reciente.
   - Si la versión entrante es idéntica o más antigua, la fila se ignora.
   - El conteo total de claves primarias únicas permanece invariable.
3. **En Curated Anomalies**: Clave primaria compuesta `(window_start, region, method)`. El recálculo estadístico sobre la misma ventana horaria actualiza los baselines sin generar registros duplicados.

### 6.3 Trazabilidad y XCom en Airflow
En el DAG `backfill_history`:
- La tarea `ingest_backfill` genera un `run_id` propio y lo retorna a XCom.
- Las tareas subordinadas `transform_backfill` y `publish_backfill` leen `--run-id '{{ ti.xcom_pull(task_ids="ingest_backfill") }}'`.
- Esto garantiza que Spark procese el lote histórico recién extraído, sin riesgo de colisión con el lote en vivo que el feed de 5 minutos genera en paralelo.

### 6.4 Comprobación rápida
1. ¿Cómo se evita que un backfill histórico sobreescriba un terremoto que ya fue actualizado hoy por USGS?
2. ¿Qué advertencia técnica se genera si la API FDSNWS alcanza el tope de 20.000 eventos?
3. ¿Por qué es vital que `transform_silver` reciba el `run_id` exacto del backfill vía XCom?
