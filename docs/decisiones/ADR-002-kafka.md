# ADR-002 — Kafka fuera del alcance

- **Fecha:** 2026-10-27
- **Estado:** Aprobada
- **Decide:** si se incorpora Apache Kafka al diseño

## Contexto

Kafka aparece como opcional en la indicación del TL. El caso de uso es un feed consultado por polling
cada 5 minutos, un backfill histórico y un reporte diario. El tiempo real del dashboard se resuelve
con SSE leyendo Gold.

## Análisis

Kafka aportaría valor si existieran: (a) varios consumidores independientes del mismo flujo,
(b) necesidad de replay dentro de una retención, (c) procesamiento continuo con latencia de segundos,
o (d) desacople entre productores de alta frecuencia y consumidores lentos.

En SismoWatch:

- El replay ya lo da **Bronze inmutable** en MinIO + backfill parametrizado.
- El desacople ya lo da **Airflow + Dataset**.
- La latencia de 5 minutos es el propio intervalo del feed; Kafka no la mejora.
- Añadiría ~600 MiB de RAM y una segunda semántica de idempotencia (offsets + upsert),
  compitiendo con la que ya existe en base de datos.

## Decisión

**No se usa Kafka.** El pipeline es batch/micro-batch y el tiempo real se resuelve con SSE.

## Consecuencias

- Menos servicios que mantener y menos RAM, que es el recurso escaso del equipo.
- Se debe poder explicar la diferencia batch/streaming y el concepto de offset sin haberlo
  implementado: queda en `docs/GUIA_ESTUDIO.md` (Módulo 8) y en el glosario.
- Si en el futuro se requiriera streaming real, el punto de inserción sería el paso de ingesta:
  publicar el GeoJSON en un topic y consumirlo con Spark Structured Streaming.

## Verificación

- No aplica ejecución. Se verifica que no exista ningún servicio, topic o dependencia de Kafka en el
  repositorio: `grep -ri kafka --include='*.py' --include='*.yml'` solo debe devolver documentación.
