# Imagen de Airflow para SismoWatch
# ---------------------------------------------------------------------------
# Por qué una imagen propia:
#   - Airflow no trae Java ni Spark: se necesitan para ejecutar spark-submit.
#   - Spark 3.5.1 se instala como cliente/ejecutor local (ADR-003).
#   - Los JAR de S3A y el driver JDBC se empaquetan en build-time para que el
#     contenedor no dependa de Internet en tiempo de ejecución.
# Base fijada: Python 3.11 (compatible con PySpark 3.5.1) y Airflow 2.10.5.
# ---------------------------------------------------------------------------
FROM apache/airflow:2.10.5-python3.11

USER root

# Java 17: requerido por Spark 3.5.x. procps/curl: diagnóstico y healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

USER airflow

# Dependencias de la aplicación.
#   pyspark 3.5.1  -> mismo motor que el resto del proyecto (ADR-003)
#   boto3          -> cliente S3 para MinIO (ingesta en Bronze)
#   psycopg2-binary-> acceso a PostgreSQL (ingesta, verificación, API futura)
#   requests       -> llamadas HTTP a USGS con timeout
RUN pip install --no-cache-dir \
        "pyspark==3.5.1" \
        "boto3==1.34.162" \
        "psycopg2-binary==2.9.9" \
        "requests==2.32.3" \
        "pytest==8.3.3"

# Los wheels de PySpark no incluyen el conector S3A ni el driver de PostgreSQL.
# Versiones alineadas con el Hadoop client que empaqueta Spark 3.5.1 (3.3.4).
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark \
    PATH="/home/airflow/.local/bin:/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin:${PATH}"

RUN set -eux; \
    curl -fsSL -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    ls -l "${SPARK_HOME}/jars/" | grep -E "hadoop-aws|aws-java-sdk-bundle|postgresql"

# Verificación temprana: si el entorno no está completo, la imagen no se construye.
RUN spark-submit --version 2>&1 | grep -q "version 3.5.1" \
    && java -version 2>&1 | grep -q "17\." \
    && echo "OK: Spark 3.5.1 + Java 17 disponibles"
