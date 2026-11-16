"""Kafka consumer: contract validation + streaming metrics in PostgreSQL.

For every message of the topic the consumer
  1. validates it against the Event Contract (``streaming/contract.py``);
     violations go to the Dead-Letter Queue table ``streaming_invalid_events_log``
     and the offset is committed, so a "poison pill" never blocks the partition;
  2. inserts the event into ``streaming_events_log`` (``event_id`` is UNIQUE:
     a re-delivered event is detected and does NOT change the metrics again);
  3. updates, in the same DB transaction, the streaming metrics:
       * Metric 1 - operational volume: events, victims, women and minor victims
       * Metric 2 - severity: % of events with a lethal weapon and % of minor victims
       * Metric 3 - institutional pressure: share of the last 50 events that hit
                    municipalities with a Ley 2126 comisaría deficit
                    (NORMAL < 25 % <= ALTA < 50 % <= CRITICA)
       * per-municipality live ranking (``streaming_municipal_metrics``)
       * end-to-end latency (publication -> persisted) per event;
  4. commits the Kafka offset only AFTER the DB commit (at-least-once delivery +
     idempotent sink = effectively-once metrics). If the DB write fails, the
     consumer rolls back, reconnects and seeks back to the same offset.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from kafka import KafkaConsumer, TopicPartition  # kafka-python-ng

from streaming.contract import EventContractViolationError, ViolenciaIntrafamiliarEvent
from streaming.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [KafkaConsumer] %(message)s")
logger = logging.getLogger("KafkaConsumer")

PRESSURE_WINDOW = 50


def log_invalid_event(conn, topic, partition, offset, key, raw_payload: str, reason: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming_invalid_events_log
                (event_key, kafka_topic, kafka_partition, kafka_offset, raw_payload, error_reason, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, now());
            """,
            (key or "UNKNOWN", topic, partition, offset, raw_payload, reason),
        )
        cur.execute(
            "UPDATE streaming_metrics_store SET total_eventos_invalidos = total_eventos_invalidos + 1, "
            "ultima_actualizacion = now() WHERE id = 1;"
        )
    conn.commit()


def persist_event(conn, ev: ViolenciaIntrafamiliarEvent, partition: int, offset: int) -> bool:
    """Writes the event and updates all metrics atomically. Returns False for duplicates."""
    now = datetime.now(timezone.utc)
    latency_ms = max(0, int((now - ev.timestamp).total_seconds() * 1000))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming_events_log (
                event_id, event_key, event_timestamp, origen, id_hecho_origen, fecha_hecho,
                codigo_dane_municipio, municipio, departamento, genero, grupo_etario, es_menor_de_edad,
                medio_agresion, es_arma_letal, cantidad_victimas, categoria_municipio,
                tiene_deficit_ley_2126, kafka_partition, kafka_offset, latencia_ms, processed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id;
            """,
            (ev.event_id, ev.event_key, ev.timestamp, ev.origen, ev.id_hecho_origen, ev.fecha_hecho,
             ev.codigo_dane_municipio, ev.municipio, ev.departamento, ev.genero, ev.grupo_etario,
             ev.es_menor_de_edad, ev.medio_agresion, ev.es_arma_letal, ev.cantidad_victimas,
             ev.categoria_municipio, ev.tiene_deficit_ley_2126, partition, offset, latency_ms, now),
        )
        if cur.fetchone() is None:
            cur.execute(
                "UPDATE streaming_metrics_store SET total_duplicados_descartados = total_duplicados_descartados + 1 "
                "WHERE id = 1;"
            )
            conn.commit()
            return False

        v = ev.cantidad_victimas
        # Metric 1 + raw counters for metric 2
        cur.execute(
            """
            UPDATE streaming_metrics_store SET
                total_eventos_procesados  = total_eventos_procesados + 1,
                total_victimas_procesadas = total_victimas_procesadas + %s,
                total_victimas_mujeres    = total_victimas_mujeres + %s,
                total_victimas_menores    = total_victimas_menores + %s,
                total_casos_arma_letal    = total_casos_arma_letal + %s,
                total_alertas_deficit_ley2126 = total_alertas_deficit_ley2126 + %s,
                latencia_promedio_ms = ROUND(((latencia_promedio_ms * total_eventos_procesados) + %s)
                                             / (total_eventos_procesados + 1), 1),
                ultimo_evento_id = %s, ultimo_evento_timestamp = %s,
                ultimo_municipio_reportado = %s, ultimo_departamento_reportado = %s,
                ultima_actualizacion = now()
            WHERE id = 1;
            """,
            (v, v if ev.genero == "FEMENINO" else 0, v if ev.es_menor_de_edad else 0,
             1 if ev.es_arma_letal else 0, 1 if ev.tiene_deficit_ley_2126 else 0, latency_ms,
             ev.event_id, ev.timestamp, ev.municipio, ev.departamento),
        )
        # Metric 2 (rates) and Metric 3 (pressure over the last N events)
        cur.execute(
            """
            WITH ventana AS (
                SELECT tiene_deficit_ley_2126 FROM streaming_events_log ORDER BY id DESC LIMIT %s
            ), presion AS (
                SELECT ROUND(100.0 * AVG(CASE WHEN tiene_deficit_ley_2126 THEN 1 ELSE 0 END), 2) AS pct
                FROM ventana
            )
            UPDATE streaming_metrics_store s SET
                tasa_letalidad_pct = ROUND(100.0 * total_casos_arma_letal / NULLIF(total_eventos_procesados, 0), 2),
                tasa_menores_pct   = ROUND(100.0 * total_victimas_menores / NULLIF(total_victimas_procesadas, 0), 2),
                tasa_mujeres_pct   = ROUND(100.0 * total_victimas_mujeres / NULLIF(total_victimas_procesadas, 0), 2),
                pct_eventos_deficit_ventana = presion.pct,
                indice_presion_operativa = CASE
                    WHEN presion.pct >= 50 THEN 'CRITICA'
                    WHEN presion.pct >= 25 THEN 'ALTA'
                    ELSE 'NORMAL' END
            FROM presion
            WHERE s.id = 1;
            """,
            (PRESSURE_WINDOW,),
        )
        # Per-municipality live ranking
        cur.execute(
            """
            INSERT INTO streaming_municipal_metrics (
                codigo_dane_municipio, municipio, departamento, categoria_municipio, eventos_recientes,
                victimas_recientes, casos_arma_letal, victimas_menores, tiene_deficit_ley_2126, ultima_alerta)
            VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s, now())
            ON CONFLICT (codigo_dane_municipio) DO UPDATE SET
                eventos_recientes  = streaming_municipal_metrics.eventos_recientes + 1,
                victimas_recientes = streaming_municipal_metrics.victimas_recientes + EXCLUDED.victimas_recientes,
                casos_arma_letal   = streaming_municipal_metrics.casos_arma_letal + EXCLUDED.casos_arma_letal,
                victimas_menores   = streaming_municipal_metrics.victimas_menores + EXCLUDED.victimas_menores,
                ultima_alerta      = now();
            """,
            (ev.codigo_dane_municipio, ev.municipio, ev.departamento, ev.categoria_municipio, v,
             1 if ev.es_arma_letal else 0, v if ev.es_menor_de_edad else 0, ev.tiene_deficit_ley_2126),
        )
    conn.commit()
    return True


def run_consumer(servers, topic, group_id, max_messages=0, idle_timeout_ms=None):
    conn = get_db_connection()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8", errors="replace") if k else None,
        consumer_timeout_ms=idle_timeout_ms if idle_timeout_ms else float("inf"),
    )
    logger.info("Subscribed to '%s' as group '%s' (bootstrap %s). Waiting for events...", topic, group_id, servers)

    processed = invalid = duplicates = 0
    try:
        for msg in consumer:
            raw = msg.value  # bytes; decoded by the contract so malformed JSON is caught too
            try:
                ev = ViolenciaIntrafamiliarEvent.from_raw_json(raw)
            except EventContractViolationError as err:
                invalid += 1
                raw_txt = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
                log_invalid_event(conn, msg.topic, msg.partition, msg.offset, msg.key, raw_txt, str(err))
                consumer.commit()
                logger.warning("[DLQ] partition=%s offset=%s key=%s -> %s", msg.partition, msg.offset, msg.key,
                               str(err).splitlines()[0][:160])
                continue

            while True:  # retry the SAME message until the DB accepts it
                try:
                    is_new = persist_event(conn, ev, msg.partition, msg.offset)
                    break
                except Exception as db_err:  # noqa: BLE001
                    logger.error("DB error on %s (offset %s): %s - retrying in 5 s", ev.event_id, msg.offset, db_err)
                    try:
                        conn.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    time.sleep(5)
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                    conn = get_db_connection()
            consumer.commit()

            if is_new:
                processed += 1
                logger.info(
                    "[OK #%04d] p=%s off=%s %s | %s (%s) | victimas=%d%s%s",
                    processed, msg.partition, msg.offset, ev.event_id, ev.municipio, ev.departamento,
                    ev.cantidad_victimas, " LETAL" if ev.es_arma_letal else "",
                    " DEFICIT" if ev.tiene_deficit_ley_2126 else "",
                )
            else:
                duplicates += 1
                logger.info("[DUPLICATE] %s already processed - metrics unchanged", ev.event_id)

            if max_messages and (processed + invalid + duplicates) >= max_messages:
                break
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer closed. processed=%d invalid=%d duplicates=%d", processed, invalid, duplicates)
    return processed, invalid, duplicates


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Domestic violence streaming consumer")
    p.add_argument("--servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_VIOLENCIA", "violencia-intrafamiliar-events"))
    p.add_argument("--group", default=os.getenv("KAFKA_CONSUMER_GROUP", "etl_streaming_group"))
    p.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = run forever)")
    p.add_argument("--idle-timeout-ms", type=int, default=0, help="Stop after N ms without messages (0 = never)")
    a = p.parse_args()
    run_consumer(a.servers, a.topic, a.group, a.max_messages, a.idle_timeout_ms or None)
