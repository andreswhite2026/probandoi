#!/bin/bash
# ===========================================================================
# Inicialización de MinIO: buckets + usuarios de servicio con permisos acotados.
# Se ejecuta una sola vez (o al recrear el contenedor) y termina.
# ---------------------------------------------------------------------------
# Usa "mc" (cliente oficial) porque la API de administración de MinIO (crear
# usuarios y políticas) no está disponible en boto3. El contenedor de mc solo
# se usa aquí: no queda corriendo.
# ===========================================================================
set -euo pipefail

: "${MINIO_ENDPOINT:?}"
: "${MINIO_ROOT_USER:?}"
: "${MINIO_ROOT_PASSWORD:?}"
: "${SISMO_BRONZE_BUCKET:?}"
: "${SISMO_SILVER_BUCKET:?}"
: "${MINIO_INGEST_USER:?}"
: "${MINIO_INGEST_PASSWORD:?}"
: "${MINIO_SPARK_USER:?}"
: "${MINIO_SPARK_PASSWORD:?}"

ALIAS=sismowatch

echo "[minio-init] conectando a ${MINIO_ENDPOINT}"
until mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  echo "[minio-init] MinIO aún no responde, reintento en 2s"
  sleep 2
done

echo "[minio-init] creando buckets (idempotente)"
mc mb --ignore-existing "${ALIAS}/${SISMO_BRONZE_BUCKET}"
mc mb --ignore-existing "${ALIAS}/${SISMO_SILVER_BUCKET}"

# Política de ingesta: escribir y leer SOLO en Bronze.
cat > /tmp/ingest-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${SISMO_BRONZE_BUCKET}"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": ["arn:aws:s3:::${SISMO_BRONZE_BUCKET}/*"]
    }
  ]
}
JSON

# Política de Spark: leer Bronze y leer/escribir Silver.
cat > /tmp/spark-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${SISMO_BRONZE_BUCKET}", "arn:aws:s3:::${SISMO_SILVER_BUCKET}"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::${SISMO_BRONZE_BUCKET}/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::${SISMO_SILVER_BUCKET}/*"]
    }
  ]
}
JSON

# mc no sobrescribe políticas existentes: se comprueba antes (idempotente).
for policy in ingest-bronze spark-silver; do
  if ! mc admin policy info "${ALIAS}" "${policy}" >/dev/null 2>&1; then
    echo "[minio-init] creando política ${policy}"
    case "${policy}" in
      ingest-bronze) mc admin policy create "${ALIAS}" "${policy}" /tmp/ingest-policy.json ;;
      spark-silver)  mc admin policy create "${ALIAS}" "${policy}" /tmp/spark-policy.json ;;
    esac
  fi
done

echo "[minio-init] usuarios de servicio (recrear actualiza la contraseña)"
mc admin user add "${ALIAS}" "${MINIO_INGEST_USER}" "${MINIO_INGEST_PASSWORD}" >/dev/null
mc admin policy attach "${ALIAS}" ingest-bronze --user "${MINIO_INGEST_USER}"
mc admin user add "${ALIAS}" "${MINIO_SPARK_USER}" "${MINIO_SPARK_PASSWORD}" >/dev/null
mc admin policy attach "${ALIAS}" spark-silver --user "${MINIO_SPARK_USER}"

echo "[minio-init] estado final:"
mc ls "${ALIAS}"
mc admin user list "${ALIAS}"
