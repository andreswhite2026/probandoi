-- ===========================================================================
-- 03. Modelo de datos
-- ---------------------------------------------------------------------------
-- Criterios:
--   * ids estables del origen (USGS) como clave natural -> idempotencia
--   * sin reglas de rango duplicadas: la calidad se decide en un único lugar
--     (Spark, Fase 3). Aquí solo integridad estructural (PK/FK/UNIQUE).
--   * timestamptz siempre en UTC
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- BRONZE / raw
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingestions (
    run_id        uuid PRIMARY KEY,
    source        text NOT NULL,                 -- 'feed' | 'backfill'
    source_url    text NOT NULL,
    window_from   timestamptz,                   -- solo backfill
    window_to     timestamptz,                   -- solo backfill
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    http_status   integer,
    object_key    text NOT NULL UNIQUE,          -- clave del objeto en MinIO
    bytes         integer,
    event_count   integer,
    sha256        char(64),                      -- integridad del payload
    status        text NOT NULL,                 -- 'ok' | 'empty' | 'invalid' | 'error'
    error_detail  text
);

CREATE INDEX IF NOT EXISTS ix_raw_ingestions_fetched_at
    ON raw.ingestions (fetched_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_ingestions_status
    ON raw.ingestions (status);

COMMENT ON TABLE raw.ingestions IS
    'Bronze: manifiesto de cada extracción de USGS. El GeoJSON vive en MinIO (object_key).';

-- ---------------------------------------------------------------------------
-- SILVER / staging
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.earthquakes (
    id             text PRIMARY KEY,             -- properties.code/ids de USGS (id del evento)
    mag            numeric(4,2),
    mag_type       text,
    place          text,
    region         text,                         -- derivada de place (heurística documentada)
    time_utc       timestamptz NOT NULL,
    updated_utc    timestamptz,
    tsunami        boolean NOT NULL DEFAULT false,
    sig            integer,
    event_status   text,
    event_type     text,
    url            text,
    longitude      numeric(9,5) NOT NULL,
    latitude       numeric(8,5) NOT NULL,
    depth_km       numeric(7,2),
    ingest_run_id  uuid NOT NULL,                -- trazabilidad hacia raw.ingestions
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_staging_eq_time
    ON staging.earthquakes (time_utc DESC);
CREATE INDEX IF NOT EXISTS ix_staging_eq_mag
    ON staging.earthquakes (mag DESC);
CREATE INDEX IF NOT EXISTS ix_staging_eq_region
    ON staging.earthquakes (region);

COMMENT ON TABLE staging.earthquakes IS
    'Silver: evento sísmico limpio y tipado. Una fila por id de USGS (upsert).';

-- ---------------------------------------------------------------------------
-- GOLD / curated — serving
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.earthquakes (
    id             text PRIMARY KEY,
    mag            numeric(4,2),
    mag_type       text,
    place          text,
    region         text,
    time_utc       timestamptz NOT NULL,
    updated_utc    timestamptz,
    tsunami        boolean NOT NULL DEFAULT false,
    sig            integer,
    event_status   text,
    event_type     text,
    url            text,
    longitude      numeric(9,5) NOT NULL,
    latitude       numeric(8,5) NOT NULL,
    depth_km       numeric(7,2),
    ingest_run_id  uuid NOT NULL,
    published_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_curated_eq_time
    ON curated.earthquakes (time_utc DESC);
CREATE INDEX IF NOT EXISTS ix_curated_eq_mag
    ON curated.earthquakes (mag DESC);
CREATE INDEX IF NOT EXISTS ix_curated_eq_region
    ON curated.earthquakes (region);

COMMENT ON TABLE curated.earthquakes IS
    'Gold: eventos aprobados por calidad. Es la única tabla de eventos que lee la API.';

-- ---------------------------------------------------------------------------
-- GOLD / curated — calidad
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.quality_runs (
    run_id       uuid PRIMARY KEY,
    dag_run_id   text,
    layer        text NOT NULL DEFAULT 'staging->curated',
    evaluated    integer NOT NULL DEFAULT 0,
    approved     integer NOT NULL DEFAULT 0,
    rejected     integer NOT NULL DEFAULT 0,
    duplicates   integer NOT NULL DEFAULT 0,
    score        numeric(5,2),
    status       text NOT NULL,                  -- 'passed' | 'failed'
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

CREATE INDEX IF NOT EXISTS ix_quality_runs_started
    ON curated.quality_runs (started_at DESC);

COMMENT ON TABLE curated.quality_runs IS
    'Gold: resultado agregado de calidad por ejecución (evaluados, aprobados, rechazados, score).';

CREATE TABLE IF NOT EXISTS curated.quality_rules (
    id           bigserial PRIMARY KEY,
    run_id       uuid NOT NULL REFERENCES curated.quality_runs (run_id) ON DELETE CASCADE,
    rule_code    text NOT NULL,
    description  text NOT NULL,
    severity     text NOT NULL DEFAULT 'blocking',  -- 'blocking' | 'warning'
    evaluated    integer NOT NULL DEFAULT 0,
    failed       integer NOT NULL DEFAULT 0,
    passed       boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_quality_rules_run
    ON curated.quality_rules (run_id);

COMMENT ON TABLE curated.quality_rules IS
    'Gold: detalle por regla de cada ejecución de calidad.';

CREATE TABLE IF NOT EXISTS curated.quarantine (
    id             bigserial PRIMARY KEY,
    run_id         uuid NOT NULL REFERENCES curated.quality_runs (run_id),
    event_id       text,
    raw_object_key text,                         -- objeto de MinIO del que salió
    payload        jsonb NOT NULL,               -- registro original, no se pierde
    reason_code    text NOT NULL,                -- p. ej. lat_out_of_range
    reason_detail  text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_quarantine_created
    ON curated.quarantine (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_quarantine_reason
    ON curated.quarantine (reason_code);

COMMENT ON TABLE curated.quarantine IS
    'Gold: registros rechazados con su motivo. Solo accesible para rol admin en la API.';

-- ---------------------------------------------------------------------------
-- GOLD / curated — anomalías
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.anomalies (
    window_start    timestamptz NOT NULL,
    window_hours    smallint NOT NULL DEFAULT 1,
    region          text NOT NULL,
    method          text NOT NULL DEFAULT 'zscore',
    metric          text NOT NULL DEFAULT 'event_count',
    value           numeric(12,2) NOT NULL,
    baseline_mean   numeric(12,2),
    baseline_std    numeric(12,2),
    zscore          numeric(8,3),
    is_anomaly      boolean NOT NULL DEFAULT false,
    explanation     text,
    detected_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, region, method)
);

CREATE INDEX IF NOT EXISTS ix_anomalies_detected
    ON curated.anomalies (detected_at DESC);

COMMENT ON TABLE curated.anomalies IS
    'Gold: detección de actividad inusual (z-score sobre conteo por hora y región).';

-- ---------------------------------------------------------------------------
-- GOLD / curated — IA
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.ai_reports (
    report_id       uuid PRIMARY KEY,
    reference_date  date NOT NULL,               -- día de los datos reportados
    generated_at    timestamptz NOT NULL DEFAULT now(),
    summary_md      text NOT NULL,
    indicators      jsonb NOT NULL,              -- cifras calculadas por código
    anomalies       jsonb NOT NULL DEFAULT '[]'::jsonb,
    provider        text NOT NULL,
    model           text NOT NULL,
    prompt_version  text NOT NULL,
    input_tokens    integer,
    output_tokens   integer,
    latency_ms      integer,
    cost_usd        numeric(10,6),
    status          text NOT NULL,               -- 'ok' | 'fallback' | 'failed'
    source_run_id   uuid,                        -- ejecución de calidad que originó los datos
    error_detail    text
);

CREATE INDEX IF NOT EXISTS ix_ai_reports_date
    ON curated.ai_reports (reference_date DESC, generated_at DESC);

COMMENT ON TABLE curated.ai_reports IS
    'Gold: reportes narrativos generados con IA a partir de cifras calculadas por código.';

CREATE TABLE IF NOT EXISTS curated.ai_runs (
    run_id          uuid PRIMARY KEY,
    report_id       uuid,
    operation       text NOT NULL,               -- 'daily_report' | 'anomaly_explanation' | 'agent_query'
    provider        text NOT NULL,
    model           text NOT NULL,
    prompt_version  text NOT NULL,
    input_tokens    integer,
    output_tokens   integer,
    latency_ms      integer,
    cost_usd        numeric(10,6),
    status          text NOT NULL,               -- 'ok' | 'fallback' | 'failed'
    error_detail    text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ai_runs_created
    ON curated.ai_runs (created_at DESC);

COMMENT ON TABLE curated.ai_runs IS
    'Gold: auditoría de cada invocación al proveedor de IA (modelo, tokens, latencia, estado).';

-- ---------------------------------------------------------------------------
-- GOLD / curated — estado del pipeline (lo que consume la vista Operaciones)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.pipeline_runs (
    dag_id       text NOT NULL,
    run_id       text NOT NULL,
    state        text NOT NULL,                  -- 'running' | 'success' | 'failed'
    started_at   timestamptz,
    finished_at  timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    note         text,
    PRIMARY KEY (dag_id, run_id)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_updated
    ON curated.pipeline_runs (updated_at DESC);

COMMENT ON TABLE curated.pipeline_runs IS
    'Gold: resumen del estado de ejecuciones para la API, sin exponer la base de Airflow.';

-- ---------------------------------------------------------------------------
-- app — autenticación de la plataforma
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.users (
    id            uuid PRIMARY KEY,
    email         text NOT NULL UNIQUE,          -- la API normaliza a minúsculas
    password_hash text NOT NULL,
    role          text NOT NULL CHECK (role IN ('viewer', 'admin')),
    is_active     boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    last_login_at timestamptz
);

COMMENT ON TABLE app.users IS
    'Usuarios de la plataforma. El hash lo genera la API (bcrypt); nunca se guarda la contraseña.';
