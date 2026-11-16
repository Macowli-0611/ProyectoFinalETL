-- ============================================================================
-- 02_create_streaming_store.sql
-- Streaming Storage Schema (PostgreSQL 16)
-- Supports real-time metric updates, event buffering & resilience logging (DLQ)
-- ============================================================================

DROP TABLE IF EXISTS streaming_metrics_store CASCADE;
DROP TABLE IF EXISTS streaming_events_log CASCADE;
DROP TABLE IF EXISTS streaming_invalid_events_log CASCADE;
DROP TABLE IF EXISTS streaming_municipal_metrics CASCADE;

-- 1. streaming_metrics_store: Singleton state table containing live calculated KPIs
CREATE TABLE streaming_metrics_store (
    id                                INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- Metric 1: Operational volume in real-time
    total_eventos_procesados          BIGINT NOT NULL DEFAULT 0,
    total_victimas_procesadas         BIGINT NOT NULL DEFAULT 0,
    total_victimas_mujeres            BIGINT NOT NULL DEFAULT 0,
    total_victimas_menores            BIGINT NOT NULL DEFAULT 0,
    -- Metric 2: Lethality and critical severity rates
    total_casos_arma_letal            BIGINT NOT NULL DEFAULT 0,
    tasa_letalidad_pct                NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    tasa_menores_pct                  NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    tasa_mujeres_pct                  NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    -- Metric 3: Institutional deficit saturation alerts (Ley 2126)
    total_alertas_deficit_ley2126     BIGINT NOT NULL DEFAULT 0,
    indice_presion_operativa          VARCHAR(50) NOT NULL DEFAULT 'NORMAL',
    -- Operational trace metadata
    ultimo_evento_id                  VARCHAR(64),
    ultimo_evento_timestamp           TIMESTAMP WITH TIME ZONE,
    ultimo_municipio_reportado        VARCHAR(150),
    ultimo_departamento_reportado     VARCHAR(100),
    ultima_actualizacion              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial row
INSERT INTO streaming_metrics_store (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- 2. streaming_events_log: Audit trail of validated incoming streaming events
CREATE TABLE streaming_events_log (
    id                       BIGSERIAL PRIMARY KEY,
    event_id                 VARCHAR(64) UNIQUE NOT NULL,
    event_key                VARCHAR(50) NOT NULL,
    event_timestamp          TIMESTAMP WITH TIME ZONE NOT NULL,
    codigo_dane_municipio    CHAR(5) NOT NULL,
    municipio                VARCHAR(150) NOT NULL,
    departamento             VARCHAR(100) NOT NULL,
    genero                   VARCHAR(50) NOT NULL,
    grupo_etario             VARCHAR(50) NOT NULL,
    es_menor_de_edad         BOOLEAN NOT NULL,
    medio_agresion           VARCHAR(100) NOT NULL,
    es_arma_letal            BOOLEAN NOT NULL,
    cantidad_victimas        INT NOT NULL CHECK (cantidad_victimas > 0),
    categoria_municipio      VARCHAR(50) NOT NULL,
    tiene_deficit_ley_2126   BOOLEAN NOT NULL,
    processed_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_streaming_log_time   ON streaming_events_log(event_timestamp DESC);
CREATE INDEX idx_streaming_log_dane   ON streaming_events_log(codigo_dane_municipio);
CREATE INDEX idx_streaming_log_letal  ON streaming_events_log(es_arma_letal);
CREATE INDEX idx_streaming_log_menor  ON streaming_events_log(es_menor_de_edad);

-- 3. streaming_invalid_events_log: Dead-Letter Queue (DLQ) for contract violations
CREATE TABLE streaming_invalid_events_log (
    id              BIGSERIAL PRIMARY KEY,
    event_key       VARCHAR(100),
    raw_payload     TEXT NOT NULL,
    error_reason    TEXT NOT NULL,
    received_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_invalid_events_time  ON streaming_invalid_events_log(received_at DESC);

-- 4. streaming_municipal_metrics: Real-time municipal ranking and saturation
CREATE TABLE streaming_municipal_metrics (
    codigo_dane_municipio    CHAR(5) PRIMARY KEY,
    municipio                VARCHAR(150) NOT NULL,
    departamento             VARCHAR(100) NOT NULL,
    categoria_municipio      VARCHAR(50) NOT NULL,
    eventos_recientes        INT NOT NULL DEFAULT 0,
    victimas_recientes       INT NOT NULL DEFAULT 0,
    casos_arma_letal         INT NOT NULL DEFAULT 0,
    victimas_menores         INT NOT NULL DEFAULT 0,
    tiene_deficit_ley_2126   BOOLEAN NOT NULL DEFAULT FALSE,
    ultima_alerta            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stream_muni_victimas ON streaming_municipal_metrics(victimas_recientes DESC);
CREATE INDEX idx_stream_muni_deficit  ON streaming_municipal_metrics(tiene_deficit_ley_2126);

