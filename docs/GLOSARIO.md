# Glosario — SismoWatch

Glosario acumulativo. Cada término se añade cuando aparece por primera vez en el proyecto, con un
ejemplo real. No se espera al final.

| Término | Definición sencilla | Explicación técnica | Ejemplo en SismoWatch | Herramienta | Por qué importa |
|---|---|---|---|---|---|
| **Contenedor** | Caja aislada que ejecuta un programa con sus dependencias. | Proceso Linux con namespaces y cgroups; comparte el kernel del host. | El servicio `api` corre en su propio contenedor. | Docker | Reproducibilidad: mismo entorno en cualquier máquina. |
| **Imagen** | Plantilla de solo lectura para crear contenedores. | Sistema de capas inmutables identificadas por digest. | `apache/airflow:2.10.5-python3.11`. | Docker | Fija versiones y evita “en mi máquina funciona”. |
| **Volumen** | Almacenamiento que sobrevive al contenedor. | Directorio gestionado por Docker montado en el contenedor. | `sismowatch_pgdata` guarda la base. | Docker | Sin volumen, `down` borraría los datos. |
| **Red (bridge)** | Red virtual privada entre contenedores. | Bridge Linux con DNS interno por nombre de servicio. | `api` resuelve `postgres` por nombre, no por IP. | Docker | Explica por qué `localhost` dentro de un contenedor no es el host. |
| **Variable de entorno** | Valor de configuración inyectado al proceso. | Par clave-valor leída por el proceso; puede venir de `.env`. | `POSTGRES_HOST_PORT=5433`. | Docker Compose | Separa configuración de código y evita secretos hardcodeados. |
| **Pipeline** | Secuencia de pasos que mueve y transforma datos. | Grafo dirigido de tareas con dependencias. | USGS → Bronze → Silver → Gold → API. | Airflow | Define el orden y qué se puede reprocesar. |
| **DAG** | Grafo dirigido sin ciclos de tareas. | Objeto de Airflow con tareas y dependencias, sin bucles. | `ingest_feed` → emite Dataset → `transform_publish`. | Airflow | Es la unidad de orquestación y de programación. |
| **Dataset / Asset** | Marca de que un dato fue actualizado. | En Airflow 2.10 es *Dataset*; en Airflow 3 se llama *Asset*. Dispara DAGs suscritos. | `sismowatch://bronze/feed`. | Airflow | Coordina por dato, no por hora; desacopla DAGs. |
| **XCom** | Mensajería pequeña entre tareas. | Se serializa en el metadata DB (límite práctico KB). | Se pasa `run_id` y `object_key`, no el GeoJSON. | Airflow | Evita reventar la base de metadatos con payloads. |
| **arquitectura Medallion** | Organización de datos en calidades crecientes. | Capas Bronze (crudo) → Silver (limpio) → Gold (listo para consumo). | Bronze = GeoJSON; Silver = tablas tipadas; Gold = serving. | Conjunto | Da trazabilidad y permite reprocesar sin volver a la fuente. |
| **Bronze / Silver / Gold** | Nombres de las capas Medallion. | Corresponden a `raw`, `staging`, `curated` en el enunciado. | Ver matriz en `docs/ARQUITECTURA.md`. | — | Evita confundir nombres lógicos con nombres físicos. |
| **Idempotencia** | Repetir la misma operación no cambia el resultado. | Operación *f* tal que `f(f(x)) = f(x)`, aquí vía upsert por clave natural. | Ejecutar dos veces el mismo backfill no duplica `id`. | SQL, Airflow | Permite reintentos y reprocesos sin ensuciar datos. |
| **Upsert** | Insertar si no existe, actualizar si existe. | `INSERT ... ON CONFLICT (id) DO UPDATE`. | `staging.earthquakes` actualiza `mag` si USGS revisó el evento. | PostgreSQL | Resuelve eventos que cambian después de publicarse. |
| **Clave primaria** | Identificador único de una fila. | Restricción `PRIMARY KEY` (única y no nula). | `staging.earthquakes.id` (id del evento USGS). | PostgreSQL | Base de la deduplicación y del upsert. |
| **Esquema (DB)** | Espacio de nombres que agrupa tablas. | Objeto de Postgres con permisos propios. | `raw`, `staging`, `curated`, `app`. | PostgreSQL | Permite separar capas y aplicar mínimo privilegio. |
| **Cuarentena** | Zona donde se guardan datos rechazados. | Tabla con el payload original y el motivo del rechazo. | `curated.quarantine` con `reason_code='lat_out_of_range'`. | PostgreSQL | No se pierde evidencia y se puede auditar la regla. |
| **Linaje** | Historial del origen y transformaciones de un dato. | Aristas entre activos (tabla/archivo) que documentan dependencias. | `raw.ingestions` → `staging.earthquakes` → `curated.earthquakes`. | OpenMetadata | Permite explicar de dónde sale cada cifra. |
| **Catálogo de datos** | Inventario buscable de activos y sus metadatos. | Servicio con descripciones, esquemas, dueños y linaje. | Tablas y buckets registrados en OpenMetadata. | OpenMetadata | Gobierno: quién usa qué y con qué significado. |
| **Bucket** | Contenedor lógico de objetos. | Namespace de S3/MinIO; los objetos se identifican por clave. | `sismowatch-bronze`. | MinIO | Separa capas con permisos distintos. |
| **Objeto** | Archivo inmutable dentro de un bucket. | Bytes + clave + metadatos; se reemplaza completo o se versiona. | `bronze/feed/dt=2026-10-27/hour=07/<run_id>.json`. | MinIO | Preservar el crudo exige inmutabilidad. |
| **Parquet** | Formato columnar comprimido para analítica. | Columnas tipadas, estadísticas por bloque, compresión. | `silver/earthquakes/dt=2026-10-27/*.parquet`. | Spark | Lee solo columnas necesarias y pesa menos que JSON. |
| **Driver / Executor** | Procesos que coordinan y ejecutan Spark. | Driver planifica; executors ejecutan tareas en particiones. | En `local[*]` ambos viven en el contenedor de Airflow. | Spark | Explica dónde corre el cómputo y dónde mirar errores. |
| **Transformación / acción** | Planificar vs ejecutar en Spark. | Las transformaciones son perezosas; las acciones disparan el job. | `df.filter(...)` no ejecuta; `.write.jdbc(...)` sí. | Spark | Evita creer que un job corrió cuando solo se definió. |
| **Partición** | División de datos para procesar en paralelo. | Unidad de trabajo; en escritura, subdirectorio `dt=...`. | Silver particionado por fecha del evento. | Spark | Afecta rendimiento y reproceso selectivo. |
| **JWT** | Token firmado con la identidad del usuario. | JSON firmado (HS256), con `sub`, `role`, `exp`. | `Authorization: Bearer <token>`. | FastAPI | Autenticación sin sesión en servidor. |
| **SSE** | Canal unidireccional servidor → navegador. | HTTP con `Content-Type: text/event-stream` y eventos `data:`. | `/api/stream` empuja eventos nuevos. | FastAPI/React | Tiempo real simple y con reconexión automática. |
| **Umbral / score de calidad** | Medida de cuánto dato pasó las reglas. | `score = aprobados / evaluados * 100`, con fórmula documentada. | Si baja del umbral, no se publica Gold. | Spark | Convierte la calidad en un número accionable. |
| **Z-score** | Cuántas desviaciones está un valor de la media. | `z = (x − μ) / σ` sobre una ventana histórica. | Pico de eventos por hora en una región. | Spark/NumPy | Detecta anomalías de forma reproducible y explicable. |
| **Prompt** | Instrucción textual al modelo. | Contexto + datos + formato esperado; se versiona. | `prompts/daily_report_v1.md`. | Ollama | Cambiarlo cambia la salida: debe ser auditable. |
| **Alucinación** | Dato inventado por el modelo. | Texto plausible no sustentado en el contexto. | Reportar una magnitud que no existe en Gold. | LLM | Se mitiga validando números contra el cálculo. |
