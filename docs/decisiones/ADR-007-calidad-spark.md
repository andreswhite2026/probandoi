# ADR-007: Reglas Declarativas de Calidad de Datos en Spark con Cuarentena y Auditoría

## Estado
Aprobada (Fase 3, formalizada en Fase 9)

## Contexto
Los datos del catálogo de terremotos de USGS provienen de estaciones sismológicas distribuidas y pueden contener
errores de medición, coordenadas fuera de límites matemáticos, magnitudes inconsistentes o registros incompletos.
Para que las capas analíticas (Gold, reportes IA y API) sean confiables, el procesamiento debe aplicar un filtro de
calidad estricto, auditable y bloqueante.

## Decisión
1. **Reglas declarativas ejecutadas por Spark**:
   - En lugar de validaciones dispersas en SQL o scripts ad-hoc, las reglas de calidad se centralizan en la transformación Silver $\to$ Gold de Spark.
   - Cada regla tiene un identificador único, código, descripción y severidad:
     - **R01 (`valid_coordinates`)**: Latitud entre -90.0 y +90.0, longitud entre -180.0 y +180.0. Severidad: `blocking`.
     - **R02 (`valid_magnitude`)**: Magnitud entre -2.0 y 10.0 (o nula para micro-sismos en revisión). Severidad: `blocking`.
     - **R03 (`valid_time`)**: Timestamp `time_utc` no nulo y dentro del rango razonable (entre año 2000 y el presente + margen de reloj de 1 hora). Severidad: `blocking`.
     - **R04 (`positive_depth`)**: Profundidad `depth_km` $\ge$ -10.0 km. Severidad: `blocking`.
     - **R05 (`non_empty_identifier`)**: Identificador de evento `id` de USGS presente y no vacío. Severidad: `blocking`.

2. **Aislamiento en Cuarentena (`curated.quarantine`)**:
   - Todo registro que falla al menos una regla bloqueante es rechazado inmediatamente de la publicación en Gold (`curated.earthquakes`).
   - El payload original completo del evento se preserva como `jsonb` junto con el código del motivo (`reason_code`), detalle explicativo y el `run_id` de calidad correspondiente.
   - La tabla de cuarentena es de acceso exclusivo para el rol `admin` de la API, bloqueando el acceso al rol `viewer`.

3. **Métricas y Auditoría de Calidad (`curated.quality_runs` y `curated.quality_rules`)**:
   - Cada ejecución de calidad genera una fila en `curated.quality_runs` con el conteo de registros evaluados, aprobados, rechazados, duplicados y un puntaje de calidad `score = (aprobados / evaluados) * 100`.
   - El desglose por regla se persiste en `curated.quality_rules` para auditoría histórica y visualización en la vista de Operaciones del frontend.

## Consecuencias
- Cero eventos inválidos llegan a `curated.earthquakes` ni son consumidos por el modelo de IA o la API pública.
- Máxima trazabilidad: cualquier rechazo puede inspeccionarse forensemente en cuarentena sin pérdida de datos crudos.
- Métricas consultables en tiempo real desde la vista "Operaciones y Calidad" del frontend.
