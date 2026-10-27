-- ===========================================================================
-- 01. Roles de base de datos y base de metadatos de Airflow
-- ---------------------------------------------------------------------------
-- Se ejecuta contra la base "postgres" (los roles son de todo el clúster).
-- Las contraseñas llegan como variables de psql desde variables de entorno;
-- nunca están escritas en este archivo.
-- ===========================================================================

-- Un rol por responsabilidad (mínimo privilegio, ver 04_grants.sql)
CREATE ROLE :"airflow_user"  LOGIN PASSWORD :'airflow_password';
CREATE ROLE :"ingest_user"   LOGIN PASSWORD :'ingest_password';
CREATE ROLE :"transform_user" LOGIN PASSWORD :'transform_password';
CREATE ROLE :"api_user"      LOGIN PASSWORD :'api_password';

-- Airflow guarda su propio estado (DAGs, runs, tareas) en una base separada.
-- No se mezcla con los datos de negocio.
CREATE DATABASE :"airflow_db" OWNER :"airflow_user" ENCODING 'UTF8';
