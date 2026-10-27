# SismoWatch

Plataforma de ingeniería de datos sísmicos en tiempo casi real construida sobre **Apache Airflow, Apache Spark, arquitectura Medallion (Bronze/Silver/Gold), MinIO, PostgreSQL, OpenMetadata, FastAPI, Vue 3 + TypeScript + Tailwind CSS y un modelo de lenguaje local (Ollama)**.

Fuente de datos: [USGS Earthquake Catalog](https://earthquake.usgs.gov/fdsnws/event/1/) (API pública y gratuita, sin clave requerida).

---

## 1. Qué resuelve

Convierte un feed sismológico público en un producto de datos confiable, analizable y navegable:

1. **Ingesta continua** cada 5 minutos desde el feed USGS hacia MinIO Bronze (GeoJSON inmutable) y registro de manifiestos de auditoría en Postgres.
2. **Backfill histórico** parametrizado de hasta 30 días, idempotente mediante deduplicación por `id` natural de evento.
3. **Transformación Medallion** con Spark `local[*]`:
   - **Silver:** eventos limpios, tipados, con región normalizada y fechas UTC en Postgres `staging` y Apache Parquet particionado por fecha en MinIO.
   - **Gold:** eventos aprobados por compuertas de calidad en Postgres `curated`, con agregados y detección de anomalías estadísticas ($z \ge 2.0$).
4. **Calidad declarativa bloqueante (R01–R05):** aislamiento de anomalías en cuarentena (`curated.quarantine`) conservando el payload original íntegro y el motivo de rechazo.
5. **Síntesis ejecutiva con IA:** generación de resúmenes diarios (Ollama Gemma:2b / Llama3) con validación activa contra alucinaciones frente a cifras deterministas de Gold y fallback transparente si Ollama no está disponible.
6. **Agente analítico de solo lectura:** clasificación de intenciones, distinción metodológica entre región sísmica y país político, y rechazo de preguntas fuera de dominio.
7. **API REST segura (FastAPI):** autenticación JWT, RBAC estricto (`viewer` / `admin`), streaming de eventos en vivo por Server-Sent Events (SSE) y exportaciones en CSV y GeoJSON.
8. **Frontend moderno (Vue 3 + TypeScript + Tailwind CSS + Leaflet):** tres vistas funcionales (Monitoreo sísmico, Reportes e IA, Operaciones y calidad).
9. **Gobernanza y linaje en OpenMetadata 2.0 (perfil `governance`):** catálogo completo de 10 tablas, 2 buckets y grafo de linaje de 8 aristas.

---

## 2. Documentación del Proyecto

| Documento | Descripción |
|---|---|
| [`PROYECTO.md`](PROYECTO.md) | Documento maestro: arquitectura, alcance obligatorio, estado de fases y criterios de aceptación. |
| [`BITACORA.md`](BITACORA.md) | Registro cronológico de actividades, problemas reales encontrados, causa raíz y soluciones aplicadas. |
| [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md) | Diagrama de componentes, matriz de almacenamiento y trade-offs técnicos. |
| [`docs/GUIA_ESTUDIO.md`](docs/GUIA_ESTUDIO.md) | Guía de preparación y estudio para la sustentación técnica del proyecto. |
| [`docs/GLOSARIO.md`](docs/GLOSARIO.md) | Glosario acumulativo de conceptos con ejemplos reales del proyecto. |
| [`docs/decisiones/`](docs/decisiones/) | Registros de Decisión Arquitectónica (ADR-001 a ADR-007). |

---

## 3. Requisitos del Sistema

- **Sistema Operativo:** Linux (probado en Ubuntu 22.04 LTS / 24.04 LTS), macOS o Windows con WSL2.
- **Herramientas requeridas en el host:**
  - `Docker Engine` 24.0+ y `Docker Compose` v2.20+.
  - `make` (GNU Make).
  - `curl` (para verificaciones de salud y endpoints).
  - `python3` (versión 3.10+, únicamente requerida para ejecutar los scripts de auditoría de gobernanza en el host).
- **Recursos de Hardware mínimos recomendados:**
  - CPU: 4 núcleos.
  - Memoria RAM libre:
    - **8 GB RAM** para el stack principal (Postgres, MinIO, Airflow, Spark, API, Web).
    - **16 GB RAM** si se arranca simultáneamente el perfil opcional de OpenMetadata (`--profile governance`).
  - Espacio libre en disco: 10 GB.
- **Puertos de Host utilizados:**
  - `3000`: Frontend Web (Vue 3 / Nginx).
  - `8000`: Backend API (FastAPI / Uvicorn).
  - `8082`: Airflow Webserver UI (remapeado para evitar colisiones con el puerto 8080 local).
  - *Nota de seguridad:* PostgreSQL (`5432`) y MinIO (`9000`/`9001`) residen exclusivamente en la red interna de Docker (`sismowatch-net`) y no exponen puertos al host.

---

## 4. Instalación en Otro Computador (Paso a Paso)

Sigue exactamente estos pasos para clonar e iniciar el proyecto en un computador limpio:

### Paso 1: Clonar el repositorio
```bash
git clone <URL_DEL_REPOSITORIO>
cd Prueba-Avanzada
```

### Paso 2: Configurar las variables de entorno
```bash
make setup
```
*Este comando crea automáticamente el archivo `.env` a partir de `.env.example` si no existe.*

> [!NOTE]
> Las variables de `.env.example` están preconfiguradas con parámetros seguros y funcionales para demostración local. En un entorno de producción real, modifica las contraseñas de PostgreSQL, Airflow y la clave `JWT_SECRET`.

### Paso 3: Construir e iniciar los servicios centrales
```bash
make up
```
*Este comando descarga las imágenes base, compila las imágenes personalizadas (`api`, `web`, `airflow`), inicializa la base de datos y arranca los contenedores en segundo plano dentro de la red `sismowatch-net`:*
- `sismowatch-postgres` (PostgreSQL 16)
- `sismowatch-minio` + `sismowatch-minio-init` (MinIO S3)
- `sismowatch-airflow-init` (migración de esquema y usuario admin)
- `sismowatch-airflow-webserver` (UI de Airflow en :8082)
- `sismowatch-airflow-scheduler` (orquestador y ejecutor de Spark)
- `sismowatch-api` (backend FastAPI en :8000)
- `sismowatch-web` (frontend Vue 3 en :3000)

### Paso 4: Verificar la salud de los servicios
```bash
make ps
```
Espera aproximadamente 30 a 45 segundos hasta que todos los contenedores reporten estado `Up` y `healthy`.

### Paso 5: Carga inicial de datos USGS y ejecución del pipeline
Al iniciar una instalación nueva, la base de datos está vacía. Puedes esperar 5 minutos a que el DAG continuo de Airflow ingiera los sismos recientes, o bien poblar datos históricos de inmediato mediante la ejecución manual de una ventana de 7 días:

```bash
# 1. Ingesta de backfill de los últimos 7 días desde USGS a MinIO Bronze
make backfill DAYS=7

# 2. Transformación Bronze -> Silver con Spark (limpieza, deduplicación y reglas de calidad)
make transform

# 3. Publicación Silver -> Gold (cálculo de anomalías estadísticas y vistas materializadas)
make gold

# 4. Generación del primer reporte diario de síntesis
make report
```

---

## 5. Acceso a la Plataforma y Credenciales

Una vez levantado el entorno, los servicios están disponibles en las siguientes direcciones:

| Servicio | URL | Descripción |
|---|---|---|
| **Frontend Web** (Vue 3) | [http://localhost:3000](http://localhost:3000) | Dashboard interactivo con mapa Leaflet, KPIs, reportes y operaciones. |
| **Backend API** (Swagger UI) | [http://localhost:8000/docs](http://localhost:8000/docs) | Documentación interactiva OpenAPI/Swagger de los endpoints. |
| **Apache Airflow UI** | [http://localhost:8082](http://localhost:8082) | Monitoreo visual de DAGs, logs de tareas y estado del scheduler. |
| **OpenMetadata UI** *(opcional)* | [http://localhost:8585](http://localhost:8585) | Catálogo de datos, especificaciones de tablas y linaje Medallion. |

### Credenciales de la Aplicación Web

La aplicación incluye dos roles claramente diferenciados por permisos RBAC en el servidor:

| Rol | Email | Contraseña | Permisos |
|---|---|---|---|
| **Viewer** *(Solo lectura)* | `viewer@sismowatch.local` | `viewer123` | Monitoreo sísmico, KPIs, mapa, tabla interactiva, filtros, exportaciones CSV/GeoJSON, consulta de reportes IA y chat con el agente analítico. **Acceso bloqueado (403 Forbidden)** a registros en cuarentena y disparo de backfills. |
| **Admin** *(Operador total)* | `admin@sismowatch.local` | `admin123` | Acceso irrestricto: todo lo anterior más auditoría forense de cuarentena con motivo de rechazo y disparo manual de pipelines de ingesta histórica. |

> [!TIP]
> En la pantalla de login ([http://localhost:3000/login](http://localhost:3000/login)) existen dos botones de acceso rápido (**"Ingresar como Viewer"** y **"Ingresar como Admin"**) que autocompletan las credenciales para demostración inmediata.

### Cómo se inicializan las cuentas y cómo cambiar contraseñas

1. **Creación inicial:** En cada arranque de la API (`lifespan`), se ejecuta `_seed_initial_users()` en [`api/app/main.py`](api/app/main.py). Si las cuentas no existen, se crean con hash seguro `bcrypt` (12 rondas de salt). La instrucción utiliza `ON CONFLICT (email) DO NOTHING`, garantizando que **reiniciar los contenedores nunca sobreescribirá contraseñas que hayan sido modificadas**.
2. **Listar usuarios existentes:**
   ```bash
   make users-list
   ```
3. **Restablecer contraseña o recuperar acceso administrativo:**
   ```bash
   make user-reset EMAIL=admin@sismowatch.local PASSWORD=mi_nueva_contrasena_segura
   ```

### Credenciales de Servicios de Infraestructura

- **Airflow Webserver:** usuario `admin`, contraseña definida en la variable `AIRFLOW_ADMIN_PASSWORD` del archivo `.env` (`admin123` por defecto).
- **OpenMetadata:** usuario `admin@open-metadata.org`, contraseña `admin`.

---

## 6. Servicios Obligatorios vs. Opcionales

| Servicio | Estado | Comportamiento si no está disponible |
|---|---|---|
| **PostgreSQL 16** | **Obligatorio** | Motor relacional central. Almacena esquemas `raw`, `staging`, `curated` y `app`. |
| **MinIO (S3)** | **Obligatorio** | Data Lake inmutable. Almacena GeoJSON crudo (Bronze) y Parquet particionado (Silver). |
| **Airflow + Spark** | **Obligatorio** | Orquesta la ingesta continua y ejecuta las transformaciones con calidad bloqueante. |
| **FastAPI Backend** | **Obligatorio** | Provee autenticación JWT, RBAC, endpoints de negocio, SSE y agente de consulta. |
| **Vue 3 Frontend** | **Obligatorio** | Interfaz de usuario SPA servida por Nginx con enrutamiento y proxy inverso. |
| **Ollama (IA Local)** | **Opcional** | Si el servicio Ollama no está instalado o activo en `host.docker.internal:11434`, el generador de reportes activa automáticamente el **modo fallback determinista** (ADR-004). El reporte se genera con datos cuantitativos 100% precisos de Gold sin interrumpir el pipeline ni la API. |
| **OpenMetadata** | **Opcional** | Aislado bajo el perfil Docker Compose `governance` (ADR-006). Solo se arranca bajo demanda para ahorrar memoria RAM y CPU en estaciones de trabajo locales. |

---

## 7. Ejecución de Pruebas Automatizadas

La plataforma cuenta con 97 pruebas automatizadas que cubren el ciclo completo:

```bash
# 1. Ejecutar TODAS las pruebas (61 de Airflow/Spark + 36 de FastAPI/RBAC)
make test-all

# 2. Ejecutar solo las pruebas del pipeline de datos en Airflow
make test

# 3. Ejecutar solo las pruebas de la API y seguridad RBAC
make test-api

# 4. Ejecutar la auditoría de seguridad y escaneo estricto de secretos en Git
make security-scan

# 5. Ejecutar la verificación interactiva de permisos RBAC con curl (401, 403, 200)
make rbac-check

# 6. Ejecutar la comprobación en vivo de endpoints de la API (KPIs, exports, agente)
make api-check
```

---

## 8. Gobernanza y Linaje (OpenMetadata)

Para activar la capa de gobernanza de datos:

```bash
# 1. Levantar OpenMetadata (MySQL 8 + OpenSearch 2.11 + Server 2.0.6)
make governance-up

# 2. Verificar el catálogo Medallion y linaje de 8 aristas de forma determinista
make governance-catalog

# 3. Registrar los activos y dependencias en vivo en la instancia de OpenMetadata
make governance-live

# 4. Detener los servicios de gobernanza al finalizar para liberar memoria
make governance-down
```

---

## 9. Diagnóstico, Logs y Solución de Problemas

### Monitoreo de logs
```bash
# Ver logs de todos los servicios
make logs

# Ver logs específicos del backend FastAPI
make api-logs

# Ver logs específicos del frontend Nginx
make web-logs
```

### Problemas frecuentes y soluciones

1. **Error: "Puerto 3000 / 8000 / 8082 ya está en uso":**
   Verifica si tienes otro proceso local corriendo en esos puertos (`sudo lsof -i :3000` o `sudo ss -tulpn | grep 3000`) o ajusta las variables `WEB_HOST_PORT`, `API_HOST_PORT` o `AIRFLOW_HOST_PORT` en el archivo `.env`.

2. **Error al conectar con la base de datos:**
   PostgreSQL no publica su puerto al host. Para abrir una consola interactiva, utiliza:
   ```bash
   make psql
   ```

3. **Reinicio de servicios individuales tras cambios de configuración:**
   ```bash
   docker compose restart api
   docker compose restart web
   ```

---

## 10. Parada Segura del Entorno

Para detener la plataforma sin perder datos:

```bash
make down
```

> [!CAUTION]
> `make down` detiene y remueve los contenedores preservando intactos los volúmenes de datos (`pgdata`, `minio_data`, `airflow_logs`). **NUNCA utilices `docker compose down -v`**, ya que dicho flag elimina los volúmenes y destruiría la base de datos histórica y los archivos de MinIO.
