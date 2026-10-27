# Código reutilizable del pipeline

Módulos que **no** son DAGs: cliente HTTP de USGS, almacenamiento en MinIO,
transformaciones auxiliares, cliente de IA y acceso a PostgreSQL.

Se montan en `/opt/airflow/include` y se importan desde los DAGs y desde las
pruebas. Regla: si una función se usa en más de un DAG o se quiere testear sin
Airflow, vive aquí.
