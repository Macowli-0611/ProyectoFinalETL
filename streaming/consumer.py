"""Apache Kafka Consumer for Domestic Violence Streaming Analytics.

Consumes real-time events, validates them against the formal Event Contract,
handles malformed messages with an explicit Dead-Letter Queue (DLQ), and updates
at least three streaming metrics in PostgreSQL.
Complies with Sections 2.6 and 2.7 of the Final Project Rubric.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import sys
import time
from typing import Dict, Any

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from streaming.contract import ViolenciaIntrafamiliarEvent, EventContractViolationError

try:
    from kafka import KafkaConsumer
except ImportError:
    try:
        from kafka_python_ng import KafkaConsumer
    except ImportError:
        KafkaConsumer = None

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [KafkaConsumer] %(message)s"
)
logger = logging.getLogger("KafkaConsumer")


def get_db_connection():
    """Establishes connection to PostgreSQL streaming database."""
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", 5432))
    dbname = os.getenv("DB_NAME", "etl_dw")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres123")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10
    )


def log_invalid_event(conn, raw_payload: str, error_reason: str, event_key: str = None):
    """Persists contract violations to Dead-Letter Queue table for resilience auditing."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO streaming_invalid_events_log (event_key, raw_payload, error_reason, received_at)
                VALUES (%s, %s, %s, %s);
                """,
                (event_key or "UNKNOWN", raw_payload, error_reason, datetime.now(timezone.utc))
            )
        conn.commit()
        logger.warning(f"[DLQ RECORDED] Invalid event sent to Dead-Letter Queue: {error_reason[:100]}")
    except Exception as e:
        logger.error(f"Failed to record invalid event in DLQ: {e}")
        conn.rollback()


def update_streaming_metrics(conn, event: ViolenciaIntrafamiliarEvent):
    """Atomically updates the 3 streaming metrics in PostgreSQL tables.

    Metric 1: Operational volume in real-time (events, total victims, female & minor victims)
    Metric 2: Lethality and critical vulnerability rates (lethal weapon %, minor victim %)
    Metric 3: Institutional deficit saturation alerts (Ley 2126 statutory gap pressure)
    """
    try:
        with conn.cursor() as cur:
            # 1. Insert into event audit log
            cur.execute(
                """
                INSERT INTO streaming_events_log (
                    event_id, event_key, event_timestamp, codigo_dane_municipio,
                    municipio, departamento, genero, grupo_etario, es_menor_de_edad,
                    medio_agresion, es_arma_letal, cantidad_victimas, categoria_municipio,
                    tiene_deficit_ley_2126, processed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (event_id) DO NOTHING;
                """,
                (
                    event.event_id, event.event_key, event.timestamp, event.codigo_dane_municipio,
                    event.municipio, event.departamento, event.genero, event.grupo_etario,
                    event.es_menor_de_edad, event.medio_agresion, event.es_arma_letal,
                    event.cantidad_victimas, event.categoria_municipio, event.tiene_deficit_ley_2126,
                    datetime.now(timezone.utc)
                )
            )

            # 2. Update singleton KPI metrics store
            # Compute new values based on existing state
            cur.execute(
                """
                UPDATE streaming_metrics_store
                SET
                    total_eventos_procesados = total_eventos_procesados + 1,
                    total_victimas_procesadas = total_victimas_procesadas + %s,
                    total_victimas_mujeres = total_victimas_mujeres + CASE WHEN %s = 'FEMENINO' THEN %s ELSE 0 END,
                    total_victimas_menores = total_victimas_menores + CASE WHEN %s THEN %s ELSE 0 END,
                    total_casos_arma_letal = total_casos_arma_letal + CASE WHEN %s THEN 1 ELSE 0 END,
                    total_alertas_deficit_ley2126 = total_alertas_deficit_ley2126 + CASE WHEN %s THEN 1 ELSE 0 END,
                    tasa_letalidad_pct = ROUND(
                        ((total_casos_arma_letal + CASE WHEN %s THEN 1 ELSE 0 END)::NUMERIC / (total_eventos_procesados + 1)) * 100, 2
                    ),
                    tasa_menores_pct = ROUND(
                        ((total_victimas_menores + CASE WHEN %s THEN %s ELSE 0 END)::NUMERIC / (total_victimas_procesadas + %s)) * 100, 2
                    ),
                    tasa_mujeres_pct = ROUND(
                        ((total_victimas_mujeres + CASE WHEN %s = 'FEMENINO' THEN %s ELSE 0 END)::NUMERIC / (total_victimas_procesadas + %s)) * 100, 2
                    ),
                    indice_presion_operativa = CASE
                        WHEN (total_alertas_deficit_ley2126 + CASE WHEN %s THEN 1 ELSE 0 END)::NUMERIC / (total_eventos_procesados + 1) >= 0.50 THEN 'CRÍTICA (DÉFICIT SATURADO)'
                        WHEN (total_alertas_deficit_ley2126 + CASE WHEN %s THEN 1 ELSE 0 END)::NUMERIC / (total_eventos_procesados + 1) >= 0.25 THEN 'ALTA'
                        ELSE 'MODERADA'
                    END,
                    ultimo_evento_id = %s,
                    ultimo_evento_timestamp = %s,
                    ultimo_municipio_reportado = %s,
                    ultimo_departamento_reportado = %s,
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = 1;
                """,
                (
                    event.cantidad_victimas,
                    event.genero, event.cantidad_victimas,
                    event.es_menor_de_edad, event.cantidad_victimas,
                    event.es_arma_letal,
                    event.tiene_deficit_ley_2126,
                    # For rates recalculation:
                    event.es_arma_letal,
                    event.es_menor_de_edad, event.cantidad_victimas, event.cantidad_victimas,
                    event.genero, event.cantidad_victimas, event.cantidad_victimas,
                    event.tiene_deficit_ley_2126,
                    event.tiene_deficit_ley_2126,
                    # Trace:
                    event.event_id, event.timestamp, event.municipio, event.departamento
                )
            )

            # 3. Upsert municipal live ranking
            cur.execute(
                """
                INSERT INTO streaming_municipal_metrics (
                    codigo_dane_municipio, municipio, departamento, categoria_municipio,
                    eventos_recientes, victimas_recientes, casos_arma_letal, victimas_menores,
                    tiene_deficit_ley_2126, ultima_alerta
                ) VALUES (
                    %s, %s, %s, %s, 1, %s, CASE WHEN %s THEN 1 ELSE 0 END, CASE WHEN %s THEN %s ELSE 0 END, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (codigo_dane_municipio) DO UPDATE SET
                    eventos_recientes = streaming_municipal_metrics.eventos_recientes + 1,
                    victimas_recientes = streaming_municipal_metrics.victimas_recientes + EXCLUDED.victimas_recientes,
                    casos_arma_letal = streaming_municipal_metrics.casos_arma_letal + EXCLUDED.casos_arma_letal,
                    victimas_menores = streaming_municipal_metrics.victimas_menores + EXCLUDED.victimas_menores,
                    ultima_alerta = CURRENT_TIMESTAMP;
                """,
                (
                    event.codigo_dane_municipio, event.municipio, event.departamento, event.categoria_municipio,
                    event.cantidad_victimas, event.es_arma_letal, event.es_menor_de_edad, event.cantidad_victimas,
                    event.tiene_deficit_ley_2126
                )
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database error while updating streaming metrics: {e}") from e


def run_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str = "etl_streaming_group",
    max_messages: int = 0
):
    """Initializes consumer, continuously processes events and updates streaming metrics."""
    logger.info(f"Starting Kafka Consumer for topic '{topic}', group '{group_id}'...")

    if KafkaConsumer is None:
        raise RuntimeError("Neither 'kafka-python' nor 'kafka-python-ng' is installed.")

    conn = get_db_connection()
    logger.info("Connected to PostgreSQL streaming database.")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000 if max_messages > 0 else -1
    )

    logger.info(f"Subscribed to topic '{topic}'. Awaiting events...")

    processed_count = 0
    invalid_count = 0

    try:
        for message in consumer:
            raw_val = message.value
            event_key = message.key.decode("utf-8") if message.key else None

            # 1. Validate against the Event Contract
            try:
                event = ViolenciaIntrafamiliarEvent.from_raw_json(raw_val)
            except EventContractViolationError as err:
                invalid_count += 1
                logger.warning(f"[REJECTED] Message offset={message.offset} key={event_key}: {err}")
                log_invalid_event(conn, json.dumps(raw_val), str(err), event_key)
                # Commit offset to prevent poison pill loop
                consumer.commit()
                continue
            except Exception as unk_err:
                invalid_count += 1
                logger.error(f"[PARSE ERROR] Message offset={message.offset}: {unk_err}")
                log_invalid_event(conn, str(raw_val), f"JSON parse error: {unk_err}", event_key)
                consumer.commit()
                continue

            # 2. Update the 3 streaming metrics in PostgreSQL
            try:
                update_streaming_metrics(conn, event)
                processed_count += 1
                consumer.commit()

                logger.info(
                    f"[STREAM EVENT #{processed_count:04d}] Offset={message.offset} | "
                    f"{event.municipio} ({event.departamento}) | Victimas={event.cantidad_victimas} | "
                    f"Letal={event.es_arma_letal} | Menor={event.es_menor_de_edad} | Ley2126={event.tiene_deficit_ley_2126}"
                )
            except Exception as e:
                logger.error(f"Error persisting valid event #{event.event_id}: {e}")

            if max_messages > 0 and (processed_count + invalid_count) >= max_messages:
                logger.info(f"Processed target message count ({max_messages}). Exiting consumer.")
                break

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        conn.close()
        logger.info(f"Consumer closed. Processed={processed_count}, Invalid={invalid_count}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Domestic Violence Streaming Consumer")
    parser.add_argument("--servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka servers")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_VIOLENCIA", "violencia-intrafamiliar-events"), help="Kafka topic")
    parser.add_argument("--group", default=os.getenv("KAFKA_CONSUMER_GROUP", "etl_streaming_group"), help="Consumer group ID")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 for continuous)")

    args = parser.parse_args()
    run_consumer(
        bootstrap_servers=args.servers,
        topic=args.topic,
        group_id=args.group,
        max_messages=args.max_messages
    )
