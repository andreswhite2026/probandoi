-- ===========================================================================
-- 02. Esquemas de SismoWatch
-- ---------------------------------------------------------------------------
-- Correspondencia con Medallion (ADR-004):
--   raw      = Bronze  (manifiesto del crudo que vive en MinIO)
--   staging  = Silver  (evento limpio, tipado, estado actual)
--   curated  = Gold    (serving, calidad, cuarentena, reportes IA)
--   app      = autenticación de la plataforma (no es dato sísmico)
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS app;

COMMENT ON SCHEMA raw     IS 'Bronze: manifiesto de los objetos GeoJSON originales guardados en MinIO';
COMMENT ON SCHEMA staging IS 'Silver: eventos sísmicos limpios, tipados y normalizados';
COMMENT ON SCHEMA curated IS 'Gold: datos aprobados, métricas de calidad, cuarentena y reportes de IA';
COMMENT ON SCHEMA app     IS 'Autenticación y autorización de la plataforma (usuarios y roles)';
