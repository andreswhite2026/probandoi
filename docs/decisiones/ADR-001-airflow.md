# ADR-001 — Versión de Airflow y coordinación por Dataset

- **Fecha:** 2026-10-27
- **Estado:** Aprobada
- **Decide:** versión de Airflow y mecanismo de encadenamiento entre DAGs

## Contexto

El enunciado exige Airflow, coordinación ingesta → transformación y backfill idempotente. En la
máquina hay `apache/airflow:3.3.2` cacheada, pero el procesamiento se hace con **pyspark 3.5.1**, cuya
compatibilidad oficial de Python llega hasta 3.11; las imágenes Airflow 3.x corren Python 3.12.

## Opciones evaluadas

1. **Airflow 3.3.2 + Python 3.12**: moderna, pero exige pyspark 4.x (cambio de motor y de API) o
   convivir con incompatibilidades; además cambia el modelo de autorización/UI y consume tiempo de
   adaptación.
2. **Airflow 2.10.5 + Python 3.11**: mantiene *Dataset scheduling* (requisito), es estable,
   documentación abundante y compatible con pyspark 3.5.1.
3. Airflow 2.8.2 (usada en otro proyecto local): funcional, pero sin las mejoras de 2.10.

## Decisión

`apache/airflow:2.10.5-python3.11` con **LocalExecutor** y PostgreSQL como metadata DB.
Coordinación por **Dataset** (`outlets=[BRONZE_DATASET]` en ingesta, `schedule=[BRONZE_DATASET]` en
transformación).

## Consecuencias

- Un solo contenedor de Airflow (api-server + scheduler + dag-processor implícitos en 2.10:
  `webserver`, `scheduler`), menos RAM que Celery.
- La transformación no depende de una hora: si la ingesta se retrasa, se dispara igual al emitir el
  Dataset.
- Se documenta que en Airflow 3 el concepto se llama *Asset*, para responder en la sustentación.

## Verificación

- [ ] El tag `2.10.5-python3.11` existe (comprobado con `docker manifest inspect`).
- [ ] `airflow dags list` muestra los 4 DAGs sin errores (Fase 2-3).
- [ ] Al terminar la ingesta, la transformación se dispara sola (Fase 3).
