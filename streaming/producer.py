"""Kafka producer: replays NEW fact rows of the Data Warehouse as events.

How events are generated (SIMULATION, clearly identified)
--------------------------------------------------------
The DIJIN source is published as a historical file, so there is no live feed of
police reports. To simulate their arrival, the producer reads
``fact_violencia_intrafamiliar`` (joined with its dimensions) in chronological
order (``fecha``; inside a day a deterministic pseudo-random order) and publishes one event per fact row - i.e. one
aggregated daily report of a municipality for a victim cohort and aggression
mechanism. Every event carries ``origen = "DW_REPLAY_SIMULATION"`` and the
original incident date in ``fecha_hecho``.

No repeated / unchanged publications
------------------------------------
A watermark (last ``fecha`` + replay order + ``id_hecho`` published) is persisted in
``streaming_producer_checkpoint`` after each delivered event. A new run only
publishes rows *after* the watermark, so the same fact is never re-published
unless ``--reset`` is given explicitly. ``event_id = dw-fact-<id_hecho>`` is
deterministic, so even a forced replay is de-duplicated by the consumer.

Publication rate: ``--interval`` seconds between events (default 1 s), which
emulates the operational cadence at which reports would reach a monitoring
center.

Usage
-----
    python streaming/producer.py --count 50 --interval 1
    python streaming/producer.py --start-date 2026-01-01 --reset
    python streaming/producer.py --inject-invalid      # every 10th message is malformed
"""

import argparse
from datetime import date, datetime, timezone
import json
import logging
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from kafka import KafkaProducer  # kafka-python-ng

from streaming.contract import ViolenciaIntrafamiliarEvent
from streaming.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [KafkaProducer] %(message)s")
logger = logging.getLogger("KafkaProducer")

PRODUCER_ID = "dw_fact_replay"
DEFAULT_START_DATE = "2026-01-01"  # most recent period loaded in the DW

FACT_QUERY = """
    SELECT f.id_hecho, t.fecha, g.codigo_dane_municipio, g.municipio, g.departamento,
           g.categoria_municipio, (g.brecha_comisarias > 0) AS tiene_deficit,
           v.genero, v.grupo_etario, v.es_menor_de_edad,
           m.medio_agresion, m.es_arma_letal, f.cantidad_victimas
    FROM fact_violencia_intrafamiliar f
    JOIN dim_tiempo    t ON t.id_tiempo    = f.id_tiempo
    JOIN dim_geografia g ON g.id_geografia = f.id_geografia
    JOIN dim_victima   v ON v.id_victima   = f.id_victima
    JOIN dim_medio     m ON m.id_medio     = f.id_medio
    WHERE (t.fecha, (f.id_hecho * 2654435761) %% 4294967296, f.id_hecho) > (%s, %s, %s)
    ORDER BY t.fecha, (f.id_hecho * 2654435761) %% 4294967296, f.id_hecho
    LIMIT %s;
"""


def read_watermark(conn, start_date: str, reset: bool):
    with conn.cursor() as cur:
        if reset:
            cur.execute("DELETE FROM streaming_producer_checkpoint WHERE producer_id = %s;", (PRODUCER_ID,))
            conn.commit()
        cur.execute(
            "SELECT last_fecha, last_orden, last_id_hecho FROM streaming_producer_checkpoint WHERE producer_id = %s;",
            (PRODUCER_ID,),
        )
        row = cur.fetchone()
    if row:
        return row[0], row[1], row[2]
    # Start just before the first row of start_date
    return date.fromisoformat(start_date), -1, 0


def replay_order(id_hecho: int) -> int:
    """Deterministic pseudo-random order inside a day (Knuth multiplicative hash),
    so the replay interleaves municipalities instead of following alphabetical ids."""
    return (id_hecho * 2654435761) % 4294967296


def save_watermark(conn, fecha, id_hecho):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming_producer_checkpoint
                (producer_id, last_fecha, last_orden, last_id_hecho, published_total, updated_at)
            VALUES (%s, %s, %s, %s, 1, now())
            ON CONFLICT (producer_id) DO UPDATE SET
                last_fecha = EXCLUDED.last_fecha,
                last_orden = EXCLUDED.last_orden,
                last_id_hecho = EXCLUDED.last_id_hecho,
                published_total = streaming_producer_checkpoint.published_total + 1,
                updated_at = now();
            """,
            (PRODUCER_ID, fecha, replay_order(id_hecho), id_hecho),
        )
    conn.commit()


def row_to_event(row) -> ViolenciaIntrafamiliarEvent:
    (id_hecho, fecha, dane, muni, depto, cat, deficit, genero, grupo, menor, medio, letal, victimas) = row
    return ViolenciaIntrafamiliarEvent(
        event_id=f"dw-fact-{id_hecho}",
        timestamp=datetime.now(timezone.utc),
        event_key=dane,
        origen="DW_REPLAY_SIMULATION",
        id_hecho_origen=id_hecho,
        fecha_hecho=fecha,
        codigo_dane_municipio=dane,
        municipio=muni,
        departamento=depto,
        genero=genero,
        grupo_etario=grupo,
        es_menor_de_edad=menor,
        medio_agresion=medio,
        es_arma_letal=letal,
        cantidad_victimas=victimas,
        categoria_municipio=cat,
        tiene_deficit_ley_2126=deficit,
    )


def malformed_payload(n: int) -> dict:
    """Deliberately broken message used to demonstrate DLQ handling."""
    return {
        "event_id": f"bad-{n}",          # too short
        "timestamp": "not-a-timestamp",  # wrong type
        "event_key": "999",              # not a DANE code
        "cantidad_victimas": -5,         # must be > 0
        "genero": "UNKNOWN_VALUE",
        "campo_extra": True,             # extra fields are forbidden
    }


def build_producer(servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )


def run_producer(servers, topic, interval, max_events, start_date, reset, inject_invalid):
    conn = get_db_connection()
    last_fecha, last_orden, last_id = read_watermark(conn, start_date, reset)
    logger.info("Watermark: fecha=%s orden=%s id_hecho=%s (source: fact_violencia_intrafamiliar)",
                last_fecha, last_orden, last_id)

    producer = build_producer(servers)
    logger.info("Connected to Kafka %s, topic '%s'. Mode: DW_REPLAY_SIMULATION", servers, topic)

    published = 0
    sent_messages = 0
    try:
        while max_events == 0 or published < max_events:
            batch = 500 if max_events == 0 else min(500, max_events - published)
            with conn.cursor() as cur:
                cur.execute(FACT_QUERY, (last_fecha, last_orden, last_id, batch))
                rows = cur.fetchall()
            if not rows:
                logger.info("No new fact rows after the watermark. Nothing else to publish.")
                break

            for row in rows:
                sent_messages += 1
                if inject_invalid and sent_messages % 10 == 0:
                    bad = malformed_payload(sent_messages)
                    producer.send(topic, key="00000", value=bad).get(timeout=30)
                    logger.warning("Injected malformed message #%d (expected to land in the DLQ)", sent_messages)
                    time.sleep(interval)

                event = row_to_event(row)
                producer.send(topic, key=event.event_key, value=event.to_kafka_dict()).get(timeout=30)
                save_watermark(conn, event.fecha_hecho, event.id_hecho_origen)
                last_fecha, last_id = event.fecha_hecho, event.id_hecho_origen
                last_orden = replay_order(last_id)
                published += 1
                logger.info(
                    "Published #%04d %s | key=%s | %s (%s) | fecha_hecho=%s | %s %s | %s%s | victimas=%d%s",
                    published, event.event_id, event.event_key, event.municipio, event.departamento,
                    event.fecha_hecho, event.genero, event.grupo_etario, event.medio_agresion,
                    " [LETAL]" if event.es_arma_letal else "", event.cantidad_victimas,
                    " [DEFICIT LEY 2126]" if event.tiene_deficit_ley_2126 else "",
                )
                if max_events and published >= max_events:
                    break
                time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        conn.close()
        logger.info("Producer closed. Published %d DW events (watermark now %s / %s).", published, last_fecha, last_id)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Replays new DW fact rows as Kafka events (simulation)")
    p.add_argument("--servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_VIOLENCIA", "violencia-intrafamiliar-events"))
    p.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    p.add_argument("--count", type=int, default=0, help="Events to publish (0 = until the DW is exhausted)")
    p.add_argument("--start-date", default=DEFAULT_START_DATE, help="Replay start when there is no checkpoint")
    p.add_argument("--reset", action="store_true", help="Discard the checkpoint and start from --start-date")
    p.add_argument("--inject-invalid", action="store_true", help="Every 10th message is malformed")
    a = p.parse_args()
    run_producer(a.servers, a.topic, a.interval, a.count, a.start_date, a.reset, a.inject_invalid)
