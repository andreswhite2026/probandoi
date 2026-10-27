# ADR-003 — Spark en modo `local[*]` dentro del contenedor de Airflow

- **Fecha:** 2026-10-27
- **Estado:** Aprobada
- **Decide:** topología de ejecución de Spark

## Contexto

Spark es obligatorio para las transformaciones. El volumen real es pequeño (11 eventos por hora;
1.969 eventos en 30 días con `minmagnitude=2.5`). El equipo tiene 15 GiB de RAM compartidos con otros
procesos.

## Opciones evaluadas

1. **Clúster standalone** (`spark-master` + `spark-worker`): patrón clásico, pero añade ~600 MiB,
   dos servicios más, una red de clúster y un punto de fallo adicional, además de serialización entre
   driver y ejecutores.
2. **`SparkSubmitOperator` a un master remoto**: requiere proveedor, conexión y coordinación de jars
   en dos lugares.
3. **`local[*]`**: el driver y los ejecutores viven en el mismo proceso, dentro del contenedor de
   Airflow; se invoca con `spark-submit --master local[*]` desde un `BashOperator`.

## Decisión

Opción 3. El job se ejecuta con `spark-submit --master local[*] --driver-memory 1g` desde el
contenedor de Airflow, con `pyspark==3.5.1` instalado en la imagen y los jars de `hadoop-aws` y del
driver JDBC de PostgreSQL empaquetados en build-time.

## Consecuencias

- Un proceso menos que operar y menos RAM.
- El driver corre en el contenedor de Airflow: los logs del job aparecen en la tarea de Airflow, lo
  que facilita el diagnóstico.
- **Limitación honesta:** no demuestra escalado distribuido. En producción se cambiaría
  `--master` a `spark://`, `k8s://` o el servicio gestionado, sin tocar la lógica del job.
- El job debe ser agnóstico del master: no hardcodear `local[*]` en el código Python, solo en el
  comando de Airflow.

## Verificación

- [ ] Smoke test en Fase 1: leer un JSON de MinIO con s3a, escribir un Parquet y una fila en Postgres.
- [ ] La tarea de transformación termina en estado `success` y deja filas en `staging.earthquakes`.
- [ ] `spark-submit --version` dentro del contenedor reporta 3.5.1.
