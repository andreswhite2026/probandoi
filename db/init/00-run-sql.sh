#!/bin/bash
# ===========================================================================
# Inicialización de PostgreSQL (se ejecuta UNA vez, con el volumen vacío).
# Se monta en /docker-entrypoint-initdb.d/ y delega en los .sql de /sql.
# ---------------------------------------------------------------------------
# Por qué un script y no los .sql directamente:
#   los .sql usan variables de psql (:'usuario', :'password') que provienen de
#   variables de entorno. Así no se escriben credenciales en el repositorio.
# ===========================================================================
set -euo pipefail

REQUIRED=(
  POSTGRES_USER POSTGRES_DB
  AIRFLOW_DB_NAME AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD
  SISMO_INGEST_USER SISMO_INGEST_PASSWORD
  SISMO_TRANSFORM_USER SISMO_TRANSFORM_PASSWORD
  SISMO_API_USER SISMO_API_PASSWORD
)
for var in "${REQUIRED[@]}"; do
  if [ -z "${!var:-}" ]; then
    echo "[db-init] ERROR: falta la variable de entorno ${var}" >&2
    exit 1
  fi
done

PSQL=(psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}")

VARS=(
  -v "airflow_db=${AIRFLOW_DB_NAME}"
  -v "airflow_user=${AIRFLOW_DB_USER}"
  -v "airflow_password=${AIRFLOW_DB_PASSWORD}"
  -v "ingest_user=${SISMO_INGEST_USER}"
  -v "ingest_password=${SISMO_INGEST_PASSWORD}"
  -v "transform_user=${SISMO_TRANSFORM_USER}"
  -v "transform_password=${SISMO_TRANSFORM_PASSWORD}"
  -v "api_user=${SISMO_API_USER}"
  -v "api_password=${SISMO_API_PASSWORD}"
  -v "db_name=${POSTGRES_DB}"
)

echo "[db-init] 1/4 roles de base de datos y metadatos de Airflow"
"${PSQL[@]}" --dbname postgres "${VARS[@]}" -f /sql/01_roles_databases.sql

echo "[db-init] 2/4 esquemas de SismoWatch"
"${PSQL[@]}" --dbname "${POSTGRES_DB}" "${VARS[@]}" -f /sql/02_schemas.sql

echo "[db-init] 3/4 tablas"
"${PSQL[@]}" --dbname "${POSTGRES_DB}" "${VARS[@]}" -f /sql/03_tables.sql

echo "[db-init] 4/4 permisos de mínimo privilegio"
"${PSQL[@]}" --dbname "${POSTGRES_DB}" "${VARS[@]}" -f /sql/04_grants.sql

echo "[db-init] listo"
