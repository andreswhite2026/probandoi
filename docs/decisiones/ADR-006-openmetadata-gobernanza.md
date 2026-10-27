# ADR-006: Catálogo de Activos, Linaje y Gobernanza con OpenMetadata en perfil `governance`

## Estado
Aprobada (Fase 9)

## Contexto
El producto de datos Medallion de SismoWatch requiere gobernanza demostrable:
1. Catalogar los activos de almacenamiento y procesamiento a lo largo de las capas Bronze, Silver y Gold.
2. Registrar el linaje de extremo a extremo (desde el feed público de USGS y buckets MinIO, pasando por las transformaciones Spark y reglas de calidad, hasta los reportes IA y serving).
3. Prevenir problemas de contención de memoria RAM en la máquina anfitriona (la suite completa de OpenMetadata con servidor Java y motor de búsqueda puede consumir > 2.5 GB si no se aísla).

## Decisión
1. **Aislamiento mediante Docker Compose `profiles: ["governance"]`**:
   - Los servicios de gobernanza (`openmetadata-server`, `openmetadata-mysql`, `openmetadata-search`) se definen bajo el perfil `governance`.
   - `docker compose up -d` o `make up` levantan exclusivamente los servicios centrales del pipeline (Postgres, MinIO, Airflow, API, Web), preservando memoria.
   - El catálogo y gobernanza se activan bajo demanda mediante `docker compose --profile governance up -d` o el target `make governance-up`.

2. **Ajuste de memoria JVM (Heap limits)**:
   - Servidor OpenMetadata limitado a `-Xms512M -Xmx1G`.
   - Motor de indexación limitado a `-Xms512m -Xmx512m`.
   - Esto reduce el consumo total de OpenMetadata a ~1.8-2.2 GB, encajando holgadamente en los 5.6 GB de RAM disponibles en la máquina.

3. **Estrategia de linaje declarativo vía API REST (`/api/v1/lineage`)**:
   - En lugar de depender de conectores automáticos de OpenLineage que introducen dependencias pesadas en Spark `local[*]` y suelen omitir operaciones JDBC internas, se adopta un **motor declarativo de registro de catálogo y linaje** (`scripts/governance/register_catalog.py`).
   - El script registra formalmente los servicios (`sismowatch_postgres`, `sismowatch_minio`, `sismowatch_airflow`), bases de datos, esquemas (`raw`, `staging`, `curated`), tablas con sus columnas y descripciones, y construye el grafo de dependencias canónico:
     ```text
     sismowatch-bronze (MinIO)
            │
            ▼
       raw.ingestions
            │
            ├─────────────────────────────────────────┐
            ▼                                         ▼
     staging.earthquakes                     sismowatch-silver (MinIO)
            │
            ├──────────────────────┬──────────────────────┐
            ▼                      ▼                      ▼
     curated.earthquakes   curated.quarantine    curated.quality_runs
            │
            ▼
     curated.ai_reports
            │
            ▼
      curated.ai_runs
     ```
   - Cada arista de linaje especifica el pipeline responsable (`ingest_usgs`, `process_silver_gold`, `daily_ai_report`) y la descripción del proceso de transformación.

4. **Verificación de seguridad y auditoría de secretos**:
   - Fase 9 incorpora un escaneo automatizado de seguridad (`scripts/governance/security_scan.py`) que audita todo el historial de commits y los archivos rastreados, asegurando cero credenciales expuestas en Git y validando que solo `.env.example` esté versionado.

## Consecuencias
- Catálogo y linaje navegables en la UI de OpenMetadata (`http://localhost:8585`) y auditables programáticamente.
- Máxima robustez: el linaje no se pierde entre reinicios ni depende de eventos asíncronos frágiles.
- La ejecución del entorno base no se degrada por consumo de memoria.
