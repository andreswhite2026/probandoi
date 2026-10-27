-- ===========================================================================
-- 04. Permisos de mínimo privilegio
-- ---------------------------------------------------------------------------
--   ingesta    -> escribe el manifiesto en raw
--   transform  -> lee raw, escribe staging y curated
--   api        -> SOLO lee curated (más app.users para autenticar)
-- Ningún rol de aplicación es superusuario ni dueño de las tablas.
-- ===========================================================================

-- Nadie se conecta por defecto: solo los roles explícitos.
REVOKE CONNECT ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db_name" TO :"ingest_user", :"transform_user", :"api_user";

-- ---------------------------------------------------------------------------
-- Ingesta: solo puede insertar manifiestos
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA raw TO :"ingest_user";
GRANT INSERT ON raw.ingestions TO :"ingest_user";
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM :"ingest_user";
GRANT INSERT ON raw.ingestions TO :"ingest_user";

-- ---------------------------------------------------------------------------
-- Transformación (Spark): lee el manifiesto, escribe Silver y Gold
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA raw, staging, curated TO :"transform_user";
GRANT SELECT ON raw.ingestions TO :"transform_user";

GRANT SELECT, INSERT, UPDATE, DELETE ON staging.earthquakes TO :"transform_user";

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA curated TO :"transform_user";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curated TO :"transform_user";

-- ---------------------------------------------------------------------------
-- API: solo lectura de Gold + autenticación
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA curated, app TO :"api_user";
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO :"api_user";
GRANT SELECT, INSERT, UPDATE ON app.users TO :"api_user";

-- ---------------------------------------------------------------------------
-- Permisos por defecto para tablas futuras (creadas por el superusuario)
-- ---------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES IN SCHEMA curated
    GRANT SELECT ON TABLES TO :"api_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA curated
    GRANT SELECT, INSERT, UPDATE ON TABLES TO :"transform_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA curated
    GRANT USAGE, SELECT ON SEQUENCES TO :"transform_user";

-- La API no debe poder tocar Bronze ni Silver, ni siquiera si alguien
-- concediera permisos por error sobre un objeto nuevo.
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM :"api_user";
REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM :"api_user";
