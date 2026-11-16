-- ============================================================================
-- 02_create_streaming_store.sql
-- Streaming store (PostgreSQL 16): live metrics, event log, DLQ, producer
-- checkpoint. Written by streaming/consumer.py and streaming/producer.py,
-- read by the Streamlit real-time dashboard.
-- ============================================================================

DROP VIEW  IF EXISTS vw_streaming_resumen_vivo CASCADE;
DROP TABLE IF EXISTS streaming_metrics_store CASCADE;
DROP TABLE IF EXISTS streaming_events_log CASCADE;
DROP TABLE IF EXISTS streaming_invalid_events_log CASCADE;
DROP TABLE IF EXISTS streaming_municipal_metrics CASCADE;
DROP TABLE IF EXISTS streaming_producer_checkpoint CASCADE;

-- 1. Singleton table with the live streaming metrics
CREATE TABLE streaming_metrics_store (
    id                              INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- Metric 1: operational volume
    total_eventos_procesados        BIGINT NOT NULL DEFAULT 0,
    total_victimas_procesadas       BIGINT NOT NULL DEFAULT 0,
    total_victimas_mujeres          BIGINT NOT NULL DEFAULT 0,
    total_victimas_menores          BIGINT NOT NULL DEFAULT 0,
    -- Metric 2: severity
    total_casos_arma_letal          BIGINT NOT NULL DEFAULT 0,
    tasa_letalidad_pct              NUMERIC(5,2) NOT NULL DEFAULT 0,
    tasa_menores_pct                NUMERIC(5,2) NOT NULL DEFAULT 0,
    tasa_mujeres_pct                NUMERIC(5,2) NOT NULL DEFAULT 0,
    -- Metric 3: institutional pressure (Ley 2126 deficit) over the last 50 events
    total_alertas_deficit_ley2126   BIGINT NOT NULL DEFAULT 0,
    pct_eventos_deficit_ventana     NUMERIC(5,2) NOT NULL DEFAULT 0,
    indice_presion_operativa        VARCHAR(20) NOT NULL DEFAULT 'NORMAL'
        CHECK (indice_presion_operativa IN ('NORMAL', 'ALTA', 'CRITICA')),
    -- Pipeline health
    total_eventos_invalidos         BIGINT NOT NULL DEFAULT 0,
    total_duplicados_descartados    BIGINT NOT NULL DEFAULT 0,
    latencia_promedio_ms            NUMERIC(12,1) NOT NULL DEFAULT 0,
    ultimo_evento_id                VARCHAR(64),
    ultimo_evento_timestamp         TIMESTAMPTZ,
    ultimo_municipio_reportado      VARCHAR(150),
    ultimo_departamento_reportado   VARCHAR(100),
    ultima_actualizacion            TIMESTAMPTZ DEFAULT now()
);
INSERT INTO streaming_metrics_store (id) VALUES (1);

-- 2. Audit log of every valid event (event_id UNIQUE => idempotent sink)
CREATE TABLE streaming_events_log (
    id                     BIGSERIAL PRIMARY KEY,
    event_id               VARCHAR(64) UNIQUE NOT NULL,
    event_key              CHAR(5) NOT NULL,
    event_timestamp        TIMESTAMPTZ NOT NULL,
    origen                 VARCHAR(30) NOT NULL,
    id_hecho_origen        BIGINT,
    fecha_hecho            DATE NOT NULL,
    codigo_dane_municipio  CHAR(5) NOT NULL,
    municipio              VARCHAR(150) NOT NULL,
    departamento           VARCHAR(100) NOT NULL,
    genero                 VARCHAR(50) NOT NULL,
    grupo_etario           VARCHAR(50) NOT NULL,
    es_menor_de_edad       BOOLEAN NOT NULL,
    medio_agresion         VARCHAR(100) NOT NULL,
    es_arma_letal          BOOLEAN NOT NULL,
    cantidad_victimas      INT NOT NULL CHECK (cantidad_victimas > 0),
    categoria_municipio    VARCHAR(50) NOT NULL,
    tiene_deficit_ley_2126 BOOLEAN NOT NULL,
    kafka_partition        INT,
    kafka_offset           BIGINT,
    latencia_ms            INT,
    processed_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_streaming_log_processed ON streaming_events_log(processed_at DESC);
CREATE INDEX idx_streaming_log_dane      ON streaming_events_log(codigo_dane_municipio);

-- 3. Dead-Letter Queue: messages that violate the event contract
CREATE TABLE streaming_invalid_events_log (
    id               BIGSERIAL PRIMARY KEY,
    event_key        VARCHAR(100),
    kafka_topic      VARCHAR(200),
    kafka_partition  INT,
    kafka_offset     BIGINT,
    raw_payload      TEXT NOT NULL,
    error_reason     TEXT NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_invalid_events_time ON streaming_invalid_events_log(received_at DESC);

-- 4. Live per-municipality ranking
CREATE TABLE streaming_municipal_metrics (
    codigo_dane_municipio  CHAR(5) PRIMARY KEY,
    municipio              VARCHAR(150) NOT NULL,
    departamento           VARCHAR(100) NOT NULL,
    categoria_municipio    VARCHAR(50) NOT NULL,
    eventos_recientes      INT NOT NULL DEFAULT 0,
    victimas_recientes     INT NOT NULL DEFAULT 0,
    casos_arma_letal       INT NOT NULL DEFAULT 0,
    victimas_menores       INT NOT NULL DEFAULT 0,
    tiene_deficit_ley_2126 BOOLEAN NOT NULL DEFAULT FALSE,
    ultima_alerta          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_stream_muni_victimas ON streaming_municipal_metrics(victimas_recientes DESC);

-- 5. Producer watermark: guarantees each DW fact row is published only once
CREATE TABLE streaming_producer_checkpoint (
    producer_id      VARCHAR(50) PRIMARY KEY,
    last_fecha       DATE NOT NULL,
    last_orden       BIGINT NOT NULL,
    last_id_hecho    BIGINT NOT NULL,
    published_total  BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
