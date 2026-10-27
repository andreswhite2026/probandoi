# Airflow DAGs

Aquí viven **solo las definiciones** de los DAGs (Fase 2 en adelante). La lógica
reutilizable (cliente USGS, validaciones, acceso a datos) va en `airflow/include/`
para que los DAGs se lean de un vistazo y se puedan probar sin ejecutar Airflow.
