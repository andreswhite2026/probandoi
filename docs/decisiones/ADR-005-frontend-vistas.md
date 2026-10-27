# ADR-005: Estructura de 3 vistas en Frontend (Vue 3 + TypeScript + Tailwind CSS + Leaflet)

## Estado
Aprobada (Fase 8)

## Contexto
El enunciado del proyecto exige una aplicación web con exactamente 3 vistas funcionales de datos,
diseñada para dos perfiles de usuario (`viewer` y `admin`), con actualización en tiempo real y visualización
geográfica interactiva. El stack seleccionado por el equipo es Vue 3 + TypeScript + Vite + Tailwind CSS + Leaflet.

## Decisión
1. **Reparto funcional de las 3 vistas de datos**:
   - **Vista 1 — Monitoreo sísmico**:
     - 6 KPIs de actividad sísmica (últimas 24h y 7d, magnitud máxima y promedio, tsunamis, anomalías activas).
     - Mapa interactivo con Leaflet (teselas oscuras sin marcas de agua de ESRI Canvas, marcadores con escala de color y radio por magnitud).
     - Tabla de terremotos con filtros (búsqueda por lugar/región, magnitud mínima, tipo de evento), paginación y ordenamiento.
     - Ticker de eventos en tiempo real vía Server-Sent Events (SSE) con reconexión automática y estado de enlace visual.
     - Exportación de datos en formatos CSV y GeoJSON con preservación de filtros.
   - **Vista 2 — Reportes e IA**:
     - Visualizador del reporte narrativo diario en Markdown validado contra alucinaciones.
     - Insignias de indicadores deterministas calculados por código (total eventos, magnitud máxima, promedio, anomalías).
     - Metadatos de auditoría del reporte (fecha de referencia, proveedor, modelo, latencia, versión de prompt).
     - Selector histórico de reportes pasados.
     - Interfaz de chat interactiva con el Agente Analítico de solo lectura (`/api/v1/agent/query`).
   - **Vista 3 — Operaciones y calidad**:
     - Estado y métricas de ejecución de los DAGs del pipeline (`ingest_usgs`, `process_silver_gold`, `daily_ai_report`).
     - Métricas de calidad agregadas (evaluados, aprobados, rechazados, score de calidad) y detalle de reglas R01 a R05.
     - Vista de cuarentena: restringida a administradores (el rol `viewer` observa bloqueo de acceso; `admin` visualiza payloads originales y códigos de rechazo).
     - Disparo de backfill histórico: restringido a administradores con formulario parametrizado de días.

2. **Autenticación (Login) fuera del conteo de vistas de datos**:
   - El formulario de inicio de sesión JWT con selección de rol (`viewer` / `admin`) se implementa como modal/overlay de autenticación y no como una vista de análisis de datos Medallion, garantizando el cumplimiento estricto del requisito de "exactamente 3 vistas".

3. **Mecanismo de tiempo real**:
   - SSE (Server-Sent Events) sobre `/api/v1/events/stream` en lugar de WebSockets. El flujo es estrictamente unidireccional (servidor a cliente), incluye reconexión nativa en el navegador y es compatible con el proxy inverso Nginx (`proxy_buffering off; chunked_transfer_encoding off;`).

## Consecuencias
- Cumplimiento estricto del alcance funcional de la prueba con evidencia visual y trazabilidad RBAC.
- Separación limpia entre vista de datos operativos, analíticos y de monitoreo geográfico.
- Consumo mínimo de recursos en cliente mediante Vite y Leaflet ligero.
