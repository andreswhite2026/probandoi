"""
SismoWatch — Definición Canónica del Catálogo Medallion y Linaje para OpenMetadata
"""

from typing import Dict, Any, List

CATALOG_SPEC = {
    "version": "1.0.0",
    "services": {
        "database": {
            "name": "sismowatch_postgres",
            "serviceType": "Postgres",
            "description": "PostgreSQL 16 canónico de SismoWatch para capas Medallion y serving analítico.",
            "database": "sismowatch",
            "schemas": [
                {
                    "name": "raw",
                    "description": "Capa Bronze: manifiestos y auditoría de descargas de GeoJSON desde USGS.",
                    "tables": [
                        {
                            "name": "ingestions",
                            "description": "Manifiesto de cada extracción de USGS. El GeoJSON vive inmutable en MinIO.",
                            "columns": [
                                {"name": "run_id", "dataType": "UUID", "description": "Identificador único de ejecución de ingesta", "constraint": "PRIMARY_KEY"},
                                {"name": "source", "dataType": "VARCHAR", "description": "Origen: feed (5 min) o backfill"},
                                {"name": "source_url", "dataType": "VARCHAR", "description": "URL completa invocada al API de USGS"},
                                {"name": "window_from", "dataType": "TIMESTAMP", "description": "Límite inferior temporal (solo backfill)"},
                                {"name": "window_to", "dataType": "TIMESTAMP", "description": "Límite superior temporal (solo backfill)"},
                                {"name": "fetched_at", "dataType": "TIMESTAMP", "description": "Timestamp UTC de descarga del feed"},
                                {"name": "http_status", "dataType": "INT", "description": "Código HTTP retornado por USGS (200 OK)"},
                                {"name": "object_key", "dataType": "VARCHAR", "description": "Clave única del objeto almacenado en MinIO Bronze", "constraint": "UNIQUE"},
                                {"name": "bytes", "dataType": "INT", "description": "Tamaño del archivo descargado en bytes"},
                                {"name": "event_count", "dataType": "INT", "description": "Cantidad de sismos presentes en el GeoJSON"},
                                {"name": "sha256", "dataType": "CHAR", "description": "Hash SHA-256 para validación de integridad inmutable"},
                                {"name": "status", "dataType": "VARCHAR", "description": "Estado de la ingesta (ok, empty, error)"},
                                {"name": "error_detail", "dataType": "VARCHAR", "description": "Detalle técnico en caso de fallo"}
                            ]
                        }
                    ]
                },
                {
                    "name": "staging",
                    "description": "Capa Silver: eventos sísmicos normalizados, tipados y deduplicados por Spark.",
                    "tables": [
                        {
                            "name": "earthquakes",
                            "description": "Eventos sísmicos limpios y normalizados. Clave primaria id natural de USGS.",
                            "columns": [
                                {"name": "id", "dataType": "VARCHAR", "description": "Identificador canónico del evento USGS (ej: us7000m8v5)", "constraint": "PRIMARY_KEY"},
                                {"name": "mag", "dataType": "NUMERIC", "description": "Magnitud sismológica del evento (-2.0 a 10.0)"},
                                {"name": "mag_type", "dataType": "VARCHAR", "description": "Tipo de escala sismológica (ml, md, mw, mww, etc.)"},
                                {"name": "place", "dataType": "VARCHAR", "description": "Descripción geográfica original de USGS"},
                                {"name": "region", "dataType": "VARCHAR", "description": "Región estandarizada calculada por el pipeline"},
                                {"name": "time_utc", "dataType": "TIMESTAMP", "description": "Fecha y hora UTC del epicentro"},
                                {"name": "updated_utc", "dataType": "TIMESTAMP", "description": "Fecha y hora UTC de última actualización de USGS"},
                                {"name": "tsunami", "dataType": "BOOLEAN", "description": "Indicador de alerta o potencial de tsunami"},
                                {"name": "sig", "dataType": "INT", "description": "Índice de significancia del sismo (0 a 1000)"},
                                {"name": "event_status", "dataType": "VARCHAR", "description": "Estado del evento (reviewed, automatic)"},
                                {"name": "event_type", "dataType": "VARCHAR", "description": "Tipo de evento (earthquake, quarry blast)"},
                                {"name": "url", "dataType": "VARCHAR", "description": "Enlace al evento en el sitio web de USGS"},
                                {"name": "longitude", "dataType": "NUMERIC", "description": "Longitud geográfica en WGS84 (-180.0 a +180.0)"},
                                {"name": "latitude", "dataType": "NUMERIC", "description": "Latitud geográfica en WGS84 (-90.0 a +90.0)"},
                                {"name": "depth_km", "dataType": "NUMERIC", "description": "Profundidad focal en kilómetros"},
                                {"name": "ingest_run_id", "dataType": "UUID", "description": "Trazabilidad hacia el manifiesto Bronze"},
                                {"name": "first_seen_at", "dataType": "TIMESTAMP", "description": "Primera detección del evento en el pipeline"},
                                {"name": "last_seen_at", "dataType": "TIMESTAMP", "description": "Última sincronización del evento"}
                            ]
                        }
                    ]
                },
                {
                    "name": "curated",
                    "description": "Capa Gold: datos aprobados por calidad, métricas operativas, anomalías y reportes analíticos.",
                    "tables": [
                        {
                            "name": "earthquakes",
                            "description": "Eventos sísmicos aprobados por las reglas de calidad R01-R05. Servidos a la API.",
                            "columns": [
                                {"name": "id", "dataType": "VARCHAR", "description": "Identificador verificado de USGS", "constraint": "PRIMARY_KEY"},
                                {"name": "mag", "dataType": "NUMERIC", "description": "Magnitud validada"},
                                {"name": "mag_type", "dataType": "VARCHAR", "description": "Escala sismológica"},
                                {"name": "place", "dataType": "VARCHAR", "description": "Ubicación geográfica"},
                                {"name": "region", "dataType": "VARCHAR", "description": "Región analítica"},
                                {"name": "time_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC del sismo"},
                                {"name": "updated_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC de actualización"},
                                {"name": "tsunami", "dataType": "BOOLEAN", "description": "Alerta de tsunami"},
                                {"name": "sig", "dataType": "INT", "description": "Significancia"},
                                {"name": "event_status", "dataType": "VARCHAR", "description": "Estado del evento"},
                                {"name": "event_type", "dataType": "VARCHAR", "description": "Tipo de evento"},
                                {"name": "url", "dataType": "VARCHAR", "description": "URL oficial USGS"},
                                {"name": "longitude", "dataType": "NUMERIC", "description": "Longitud validada (-180 a +180)"},
                                {"name": "latitude", "dataType": "NUMERIC", "description": "Latitud validada (-90 a +90)"},
                                {"name": "depth_km", "dataType": "NUMERIC", "description": "Profundidad en km validada"},
                                {"name": "ingest_run_id", "dataType": "UUID", "description": "Trazabilidad a ingesta"},
                                {"name": "published_at", "dataType": "TIMESTAMP", "description": "Fecha y hora de publicación en Gold"}
                            ]
                        },
                        {
                            "name": "quarantine",
                            "description": "Zona de aislamiento para registros que fallaron reglas de calidad. Solo accesible por administradores.",
                            "columns": [
                                {"name": "id", "dataType": "BIGINT", "description": "Identificador secuencial", "constraint": "PRIMARY_KEY"},
                                {"name": "run_id", "dataType": "UUID", "description": "ID de ejecución de calidad"},
                                {"name": "event_id", "dataType": "VARCHAR", "description": "ID del sismo rechazado"},
                                {"name": "raw_object_key", "dataType": "VARCHAR", "description": "Objeto Bronze origen"},
                                {"name": "payload", "dataType": "JSON", "description": "Payload original completo no modificado"},
                                {"name": "reason_code", "dataType": "VARCHAR", "description": "Código de regla violada (lat_out_of_range, etc.)"},
                                {"name": "reason_detail", "dataType": "VARCHAR", "description": "Explicación técnica del motivo de rechazo"},
                                {"name": "created_at", "dataType": "TIMESTAMP", "description": "Timestamp de envío a cuarentena"}
                            ]
                        },
                        {
                            "name": "quality_runs",
                            "description": "Métricas agregadas por ejecución del pipeline de calidad (evaluados, aprobados, score).",
                            "columns": [
                                {"name": "run_id", "dataType": "UUID", "description": "Identificador de corrida de calidad", "constraint": "PRIMARY_KEY"},
                                {"name": "dag_run_id", "dataType": "VARCHAR", "description": "ID de ejecución en Airflow"},
                                {"name": "layer", "dataType": "VARCHAR", "description": "Capa evaluada (staging->curated)"},
                                {"name": "evaluated", "dataType": "INT", "description": "Total de registros procesados"},
                                {"name": "approved", "dataType": "INT", "description": "Registros conformes publicados a Gold"},
                                {"name": "rejected", "dataType": "INT", "description": "Registros enviados a cuarentena"},
                                {"name": "duplicates", "dataType": "INT", "description": "Registros deduplicados"},
                                {"name": "score", "dataType": "NUMERIC", "description": "Porcentaje de calidad (0.00% a 100.00%)"},
                                {"name": "status", "dataType": "VARCHAR", "description": "Resultado (passed, failed)"},
                                {"name": "started_at", "dataType": "TIMESTAMP", "description": "Inicio de validación"},
                                {"name": "finished_at", "dataType": "TIMESTAMP", "description": "Fin de validación"}
                            ]
                        },
                        {
                            "name": "quality_rules",
                            "description": "Detalle analítico por regla declarativa (R01 a R05) de cada corrida de calidad.",
                            "columns": [
                                {"name": "id", "dataType": "BIGINT", "description": "ID de auditoría de regla", "constraint": "PRIMARY_KEY"},
                                {"name": "run_id", "dataType": "UUID", "description": "ID de ejecución de calidad"},
                                {"name": "rule_code", "dataType": "VARCHAR", "description": "Código de la regla (R01_coords, etc.)"},
                                {"name": "description", "dataType": "VARCHAR", "description": "Descripción formal de la regla"},
                                {"name": "severity", "dataType": "VARCHAR", "description": "Severidad (blocking, warning)"},
                                {"name": "evaluated", "dataType": "INT", "description": "Total evaluados para esta regla"},
                                {"name": "failed", "dataType": "INT", "description": "Total fallos para esta regla"},
                                {"name": "passed", "dataType": "BOOLEAN", "description": "Si la regla fue satisfecha globalmente"}
                            ]
                        },
                        {
                            "name": "anomalies",
                            "description": "Detección estadística de anomalías de frecuencia sísmica mediante z-score.",
                            "columns": [
                                {"name": "window_start", "dataType": "TIMESTAMP", "description": "Inicio de ventana horaria analizada", "constraint": "PRIMARY_KEY"},
                                {"name": "window_hours", "dataType": "INT", "description": "Amplitud de la ventana en horas"},
                                {"name": "region", "dataType": "VARCHAR", "description": "Región geográfica evaluada", "constraint": "PRIMARY_KEY"},
                                {"name": "method", "dataType": "VARCHAR", "description": "Método estadístico (zscore)", "constraint": "PRIMARY_KEY"},
                                {"name": "metric", "dataType": "VARCHAR", "description": "Métrica observada (event_count)"},
                                {"name": "value", "dataType": "NUMERIC", "description": "Valor observado en la ventana"},
                                {"name": "baseline_mean", "dataType": "NUMERIC", "description": "Media de la línea base histórica"},
                                {"name": "baseline_std", "dataType": "NUMERIC", "description": "Desviación estándar de la línea base"},
                                {"name": "zscore", "dataType": "NUMERIC", "description": "Puntaje z estandarizado"},
                                {"name": "is_anomaly", "dataType": "BOOLEAN", "description": "Booleano que señala si supera umbral (z >= 2.0)"},
                                {"name": "explanation", "dataType": "VARCHAR", "description": "Explicación contextual de la anomalía"},
                                {"name": "detected_at", "dataType": "TIMESTAMP", "description": "Timestamp de detección"}
                            ]
                        },
                        {
                            "name": "ai_reports",
                            "description": "Reportes diarios ejecutivos generados por LLM con cifras numéricas deterministas auditadas.",
                            "columns": [
                                {"name": "report_id", "dataType": "UUID", "description": "ID único del reporte diario", "constraint": "PRIMARY_KEY"},
                                {"name": "reference_date", "dataType": "DATE", "description": "Día calendario analizado"},
                                {"name": "generated_at", "dataType": "TIMESTAMP", "description": "Timestamp de generación"},
                                {"name": "summary_md", "dataType": "VARCHAR", "description": "Contenido narrativo en Markdown"},
                                {"name": "indicators", "dataType": "JSON", "description": "Indicadores Gold calculados por código (total, max, avg)"},
                                {"name": "anomalies", "dataType": "JSON", "description": "Listado de anomalías adjuntas"},
                                {"name": "provider", "dataType": "VARCHAR", "description": "Proveedor de IA (ollama o deterministic)"},
                                {"name": "model", "dataType": "VARCHAR", "description": "Modelo utilizado (gemma:2b, llama3)"},
                                {"name": "prompt_version", "dataType": "VARCHAR", "description": "Versión del prompt utilizado"},
                                {"name": "input_tokens", "dataType": "INT", "description": "Tokens de entrada"},
                                {"name": "output_tokens", "dataType": "INT", "description": "Tokens de salida"},
                                {"name": "latency_ms", "dataType": "INT", "description": "Latencia de inferencia en milisegundos"},
                                {"name": "cost_usd", "dataType": "NUMERIC", "description": "Costo estimado en USD"},
                                {"name": "status", "dataType": "VARCHAR", "description": "Estado (ok, fallback, failed)"},
                                {"name": "source_run_id", "dataType": "UUID", "description": "ID de ejecución origen"},
                                {"name": "error_detail", "dataType": "VARCHAR", "description": "Detalle técnico si operó en fallback"}
                            ]
                        },
                        {
                            "name": "ai_runs",
                            "description": "Trazabilidad y auditoría de cada interacción con modelos de IA (reportes y agente).",
                            "columns": [
                                {"name": "run_id", "dataType": "UUID", "description": "Identificador de corrida IA", "constraint": "PRIMARY_KEY"},
                                {"name": "report_id", "dataType": "UUID", "description": "Reporte asociado si aplica"},
                                {"name": "operation", "dataType": "VARCHAR", "description": "Operación (daily_report, agent_query)"},
                                {"name": "provider", "dataType": "VARCHAR", "description": "Proveedor invocado"},
                                {"name": "model", "dataType": "VARCHAR", "description": "Modelo invocado"},
                                {"name": "prompt_version", "dataType": "VARCHAR", "description": "Versión del prompt"},
                                {"name": "input_tokens", "dataType": "INT", "description": "Tokens evaluados"},
                                {"name": "output_tokens", "dataType": "INT", "description": "Tokens generados"},
                                {"name": "latency_ms", "dataType": "INT", "description": "Tiempo de respuesta en milisegundos"},
                                {"name": "cost_usd", "dataType": "NUMERIC", "description": "Costo financiero"},
                                {"name": "status", "dataType": "VARCHAR", "description": "Estado de la ejecución"},
                                {"name": "error_detail", "dataType": "VARCHAR", "description": "Detalle de error"},
                                {"name": "created_at", "dataType": "TIMESTAMP", "description": "Timestamp de invocación"}
                            ]
                        },
                        {
                            "name": "pipeline_runs",
                            "description": "Resumen consolidado del estado de ejecuciones de Airflow para consumo de la vista Operaciones.",
                            "columns": [
                                {"name": "dag_id", "dataType": "VARCHAR", "description": "Identificador del DAG en Airflow", "constraint": "PRIMARY_KEY"},
                                {"name": "run_id", "dataType": "VARCHAR", "description": "ID de corrida en Airflow", "constraint": "PRIMARY_KEY"},
                                {"name": "state", "dataType": "VARCHAR", "description": "Estado de la ejecución (running, success, failed)"},
                                {"name": "started_at", "dataType": "TIMESTAMP", "description": "Inicio de corrida"},
                                {"name": "finished_at", "dataType": "TIMESTAMP", "description": "Finalización de corrida"},
                                {"name": "updated_at", "dataType": "TIMESTAMP", "description": "Última sincronización"},
                                {"name": "note", "dataType": "VARCHAR", "description": "Nota operativa"}
                            ]
                        }
                    ]
                }
            ]
        },
        "storage": {
            "name": "sismowatch_minio",
            "serviceType": "S3",
            "description": "MinIO S3 canonical data lake para payloads crudos (Bronze) y columnar analítico (Silver).",
            "containers": [
                {
                    "name": "sismowatch-bronze",
                    "description": "Capa Bronze: almacenamiento inmutable de archivos GeoJSON descargados de USGS (particionado por fecha y hora).",
                    "format": "JSON"
                },
                {
                    "name": "sismowatch-silver",
                    "description": "Capa Silver: almacenamiento columnar Apache Parquet particionado por fecha (dt=YYYY-MM-DD).",
                    "format": "Parquet"
                }
            ]
        },
        "pipeline": {
            "name": "sismowatch_airflow",
            "serviceType": "Airflow",
            "description": "Apache Airflow 2.10.5 orquestador del pipeline de datos Medallion.",
            "pipelines": [
                {
                    "name": "ingest_usgs",
                    "description": "Ingesta incremental cada 5 minutos desde el feed público de USGS hacia MinIO Bronze y Postgres raw.ingestions."
                },
                {
                    "name": "process_silver_gold",
                    "description": "Transformación Spark Bronze -> Silver -> Gold con validación de calidad R01-R05, cuarentena y anomalías z-score."
                },
                {
                    "name": "daily_ai_report",
                    "description": "Generación diaria de indicadores deterministas Gold, validación anti-alucinaciones y narrativa con LLM local Ollama."
                },
                {
                    "name": "backfill_history",
                    "description": "Backfill parametrizado e idempotente de hasta 30 días de historia de USGS."
                }
            ]
        }
    },
    "lineage": [
        {
            "fromEntity": {"type": "container", "fqn": "sismowatch_minio.sismowatch-bronze"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.raw.ingestions"},
            "pipeline": "sismowatch_airflow.ingest_usgs",
            "description": "La tarea de ingesta descarga el GeoJSON a MinIO Bronze y registra su manifiesto, tamaño y hash en raw.ingestions."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.raw.ingestions"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.staging.earthquakes"},
            "pipeline": "sismowatch_airflow.process_silver_gold",
            "description": "Spark lee los objetos de MinIO indicados en raw.ingestions, limpia, tipa y hace upsert en staging.earthquakes."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.staging.earthquakes"},
            "toEntity": {"type": "container", "fqn": "sismowatch_minio.sismowatch-silver"},
            "pipeline": "sismowatch_airflow.process_silver_gold",
            "description": "Spark persiste los eventos normalizados de Silver en formato Parquet particionado en MinIO sismowatch-silver."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.staging.earthquakes"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.earthquakes"},
            "pipeline": "sismowatch_airflow.process_silver_gold",
            "description": "Filtro bloqueante de calidad: solo los registros conformes con reglas R01 a R05 se publican en curated.earthquakes."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.staging.earthquakes"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.quarantine"},
            "pipeline": "sismowatch_airflow.process_silver_gold",
            "description": "Registros no conformes son desviados a cuarentena con payload original íntegro y motivo de rechazo."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.staging.earthquakes"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.quality_runs"},
            "pipeline": "sismowatch_airflow.process_silver_gold",
            "description": "Auditoría de métricas de calidad (aprobados, rechazados, score) calculada por cada ejecución del pipeline."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.earthquakes"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.ai_reports"},
            "pipeline": "sismowatch_airflow.daily_ai_report",
            "description": "Indicadores agregados en Gold alimentan el generador de reportes diarios con validación determinista anti-alucinaciones."
        },
        {
            "fromEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.ai_reports"},
            "toEntity": {"type": "table", "fqn": "sismowatch_postgres.sismowatch.curated.ai_runs"},
            "pipeline": "sismowatch_airflow.daily_ai_report",
            "description": "Cada invocación de inferencia (modelo, tokens, latencia) se registra en curated.ai_runs para auditoría."
        }
    ]
}


def verify_catalog_consistency(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Verifica que la especificación de catálogo y linaje sean coherentes."""
    errors = []
    registered_entities = set()

    db_service = spec["services"]["database"]
    db_name = db_service["database"]
    for schema in db_service["schemas"]:
        schema_name = schema["name"]
        for table in schema["tables"]:
            table_name = table["name"]
            fqn = f"{db_service['name']}.{db_name}.{schema_name}.{table_name}"
            registered_entities.add(("table", fqn))
            
            if not table.get("columns"):
                errors.append(f"Tabla {fqn} no tiene columnas definidas.")
            has_pk = any(col.get("constraint") == "PRIMARY_KEY" for col in table.get("columns", []))
            if not has_pk:
                errors.append(f"Tabla {fqn} no tiene clave primaria (PRIMARY_KEY) definida.")

    storage_service = spec["services"]["storage"]
    for container in storage_service["containers"]:
        fqn = f"{storage_service['name']}.{container['name']}"
        registered_entities.add(("container", fqn))

    pipeline_service = spec["services"]["pipeline"]
    registered_pipelines = set()
    for pipe in pipeline_service["pipelines"]:
        fqn = f"{pipeline_service['name']}.{pipe['name']}"
        registered_pipelines.add(fqn)

    for i, edge in enumerate(spec["lineage"]):
        from_ent = (edge["fromEntity"]["type"], edge["fromEntity"]["fqn"])
        to_ent = (edge["toEntity"]["type"], edge["toEntity"]["fqn"])
        pipeline_fqn = edge.get("pipeline")

        if from_ent not in registered_entities:
            errors.append(f"Arista linaje #{i+1}: Origen no registrado {from_ent}")
        if to_ent not in registered_entities:
            errors.append(f"Arista linaje #{i+1}: Destino no registrado {to_ent}")
        if pipeline_fqn and pipeline_fqn not in registered_pipelines:
            errors.append(f"Arista linaje #{i+1}: Pipeline no registrado {pipeline_fqn}")

    table_count = sum(len(s["tables"]) for s in db_service["schemas"])
    container_count = len(storage_service["containers"])
    pipeline_count = len(pipeline_service["pipelines"])
    edge_count = len(spec["lineage"])

    return {
        "valid": len(errors) == 0,
        "table_count": table_count,
        "container_count": container_count,
        "pipeline_count": pipeline_count,
        "edge_count": edge_count,
        "errors": errors
    }
