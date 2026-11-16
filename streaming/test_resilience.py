"""Streaming resilience experiments (real execution, with assertions).

Experiment 1 - invalid events
    Publishes a contract-violating JSON message and a non-JSON message, then one
    valid DW event. PASS if both bad messages land in the DLQ table, the consumer
    keeps running and the valid event right after them is processed.

Experiment 2 - consumer interruption
    Really stops the consumer (``docker stop etl_final_stream_consumer``),
    publishes N DW events, measures the consumer-group lag in Kafka while it is
    down, restarts it and waits for the backlog. PASS if the metrics do not move
    while it is stopped, the lag equals N, and after the restart exactly N new
    events are processed (no loss, no duplicates) and the lag returns to 0.

Experiment 3 - duplicate delivery
    Re-publishes an already processed event (same event_id). PASS if it is
    counted as a duplicate and the metrics stay unchanged (idempotent sink).

Each experiment writes its result to docs/evidence/streaming_experiment_<n>.json
and the script exits with code 1 if any assertion fails.

Usage (stack running with docker compose):
    python streaming/test_resilience.py               # all experiments
    python streaming/test_resilience.py --only 2 --events 10
"""

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition

from streaming.contract import ViolenciaIntrafamiliarEvent
from streaming.db import get_db_connection

EVIDENCE_DIR = os.path.join(PROJECT_ROOT, "docs", "evidence")
CONSUMER_CONTAINER = "etl_final_stream_consumer"


def banner(title):
    print("\n" + "=" * 80 + f"\n {title}\n" + "=" * 80, flush=True)


def snapshot():
    with closing(get_db_connection()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT total_eventos_procesados, total_victimas_procesadas, total_eventos_invalidos, "
            "total_duplicados_descartados FROM streaming_metrics_store WHERE id = 1;"
        )
        ev, vic, inv, dup = cur.fetchone()
        cur.execute("SELECT count(*) FROM streaming_invalid_events_log;")
        dlq = cur.fetchone()[0]
    return {"eventos": ev, "victimas": vic, "invalidos": inv, "duplicados": dup, "dlq_rows": dlq}


def consumer_lag(servers, topic, group):
    admin = KafkaAdminClient(bootstrap_servers=servers.split(","))
    committed = admin.list_consumer_group_offsets(group)
    admin.close()
    probe = KafkaConsumer(bootstrap_servers=servers.split(","))
    parts = [TopicPartition(topic, p) for p in (probe.partitions_for_topic(topic) or [])]
    end = probe.end_offsets(parts)
    probe.close()
    lag = 0
    for tp in parts:
        c = committed.get(tp)
        lag += end[tp] - (c.offset if c and c.offset >= 0 else 0)
    return lag


def container_running() -> bool:
    out = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", CONSUMER_CONTAINER],
                         capture_output=True, text=True)
    return out.stdout.strip() == "true"


def docker(action):
    subprocess.run(["docker", action, CONSUMER_CONTAINER], check=True, capture_output=True)
    print(f"    docker {action} {CONSUMER_CONTAINER} -> running={container_running()}", flush=True)


def run_producer(servers, topic, count, interval=0.2):
    subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "streaming", "producer.py"),
                    "--servers", servers, "--topic", topic, "--count", str(count),
                    "--interval", str(interval)], check=True)


def wait_until(predicate, timeout=60, step=1.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if predicate():
            return True
        time.sleep(step)
    return False


def save(n, payload):
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    path = os.path.join(EVIDENCE_DIR, f"streaming_experiment_{n}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
    print(f"    evidence -> {os.path.relpath(path, PROJECT_ROOT)}")


def experiment_1(servers, topic, group):
    banner("EXPERIMENT 1: INVALID EVENTS -> DEAD-LETTER QUEUE")
    before = snapshot()
    print(f">>> Before: {before}")
    prod = KafkaProducer(bootstrap_servers=servers.split(","))
    bad_contract = {"event_id": "bad-1", "timestamp": "yesterday", "event_key": "999",
                    "cantidad_victimas": -5, "genero": "UNKNOWN", "campo_extra": 1}
    prod.send(topic, key=b"00000", value=json.dumps(bad_contract).encode()).get(timeout=30)
    prod.send(topic, key=b"00000", value=b"<<this is not json>>").get(timeout=30)
    prod.close()
    print(">>> Published 1 contract-violating JSON + 1 non-JSON message")
    run_producer(servers, topic, 1)
    print(">>> Published 1 valid DW event right after them")

    ok = wait_until(lambda: (lambda s: s["dlq_rows"] >= before["dlq_rows"] + 2
                             and s["eventos"] >= before["eventos"] + 1)(snapshot()), timeout=45)
    after = snapshot()
    with closing(get_db_connection()) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, event_key, kafka_partition, kafka_offset, left(error_reason, 300) "
                    "FROM streaming_invalid_events_log ORDER BY id DESC LIMIT 2;")
        dlq = [dict(zip(["id", "event_key", "partition", "offset", "error_reason"], r)) for r in cur.fetchall()]
    alive = container_running()
    passed = bool(ok and alive and after["dlq_rows"] - before["dlq_rows"] == 2
                  and after["eventos"] - before["eventos"] == 1)
    for d in dlq:
        print(f"    DLQ #{d['id']} p={d['partition']} off={d['offset']}: {d['error_reason'][:120]}")
    print(f">>> After: {after} | consumer alive={alive}")
    print(f">>> RESULT: {'PASS' if passed else 'FAIL'}")
    save(1, {"experiment": "invalid events", "executed_at": datetime.now(timezone.utc),
             "before": before, "after": after, "dlq_entries": dlq, "consumer_alive": alive,
             "lag_after": consumer_lag(servers, topic, group), "result": "PASS" if passed else "FAIL"})
    return passed


def experiment_2(servers, topic, group, n):
    banner("EXPERIMENT 2: CONSUMER INTERRUPTION AND BACKLOG RECOVERY")
    if not container_running():
        docker("start")
        time.sleep(5)
    wait_until(lambda: consumer_lag(servers, topic, group) == 0, timeout=30)
    before = snapshot()
    print(f">>> Step 0 - baseline: {before}, lag={consumer_lag(servers, topic, group)}")

    print(">>> Step 1 - stopping the consumer")
    docker("stop")

    print(f">>> Step 2 - publishing {n} DW events while the consumer is down")
    run_producer(servers, topic, n)
    time.sleep(5)
    during = snapshot()
    lag_down = consumer_lag(servers, topic, group)
    print(f">>> Step 3 - while down: metrics={during} | consumer-group lag={lag_down}")

    print(">>> Step 4 - restarting the consumer")
    t_restart = time.time()
    docker("start")
    ok = wait_until(lambda: snapshot()["eventos"] >= before["eventos"] + n, timeout=90)
    recovery_s = round(time.time() - t_restart, 1)
    after = snapshot()
    lag_after = consumer_lag(servers, topic, group)
    print(f">>> Step 5 - after restart ({recovery_s}s): metrics={after} | lag={lag_after}")

    passed = bool(ok and during["eventos"] == before["eventos"] and lag_down == n
                  and after["eventos"] - before["eventos"] == n
                  and after["duplicados"] == before["duplicados"] and lag_after == 0)
    print(f">>> RESULT: {'PASS' if passed else 'FAIL'}")
    save(2, {"experiment": "consumer interruption", "executed_at": datetime.now(timezone.utc),
             "events_published_while_down": n, "before": before, "while_down": during,
             "lag_while_down": lag_down, "after_restart": after, "lag_after": lag_after,
             "recovery_seconds": recovery_s, "result": "PASS" if passed else "FAIL"})
    return passed


def experiment_3(servers, topic, group):
    banner("EXPERIMENT 3: DUPLICATE DELIVERY (IDEMPOTENT SINK)")
    with closing(get_db_connection()) as conn, conn.cursor() as cur:
        cur.execute("""SELECT event_id, event_key, origen, id_hecho_origen, fecha_hecho, codigo_dane_municipio,
                              municipio, departamento, genero, grupo_etario, es_menor_de_edad, medio_agresion,
                              es_arma_letal, cantidad_victimas, categoria_municipio, tiene_deficit_ley_2126
                       FROM streaming_events_log ORDER BY id DESC LIMIT 1;""")
        row = cur.fetchone()
    if not row:
        print("No processed event to duplicate - run experiment 2 first.")
        return False
    cols = ["event_id", "event_key", "origen", "id_hecho_origen", "fecha_hecho", "codigo_dane_municipio",
            "municipio", "departamento", "genero", "grupo_etario", "es_menor_de_edad", "medio_agresion",
            "es_arma_letal", "cantidad_victimas", "categoria_municipio", "tiene_deficit_ley_2126"]
    ev = ViolenciaIntrafamiliarEvent(timestamp=datetime.now(timezone.utc), **dict(zip(cols, row)))
    before = snapshot()
    prod = KafkaProducer(bootstrap_servers=servers.split(","),
                         value_serializer=lambda v: json.dumps(v).encode())
    prod.send(topic, key=ev.event_key.encode(), value=ev.to_kafka_dict()).get(timeout=30)
    prod.close()
    print(f">>> Re-published already processed event {ev.event_id}")
    ok = wait_until(lambda: snapshot()["duplicados"] >= before["duplicados"] + 1, timeout=30)
    after = snapshot()
    passed = bool(ok and after["eventos"] == before["eventos"] and after["victimas"] == before["victimas"])
    print(f">>> Before: {before}\n>>> After:  {after}\n>>> RESULT: {'PASS' if passed else 'FAIL'}")
    save(3, {"experiment": "duplicate delivery", "executed_at": datetime.now(timezone.utc),
             "event_id": ev.event_id, "before": before, "after": after, "result": "PASS" if passed else "FAIL"})
    return passed


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_VIOLENCIA", "violencia-intrafamiliar-events"))
    p.add_argument("--group", default=os.getenv("KAFKA_CONSUMER_GROUP", "etl_streaming_group"))
    p.add_argument("--events", type=int, default=10, help="Events published while the consumer is down")
    p.add_argument("--only", type=int, choices=[1, 2, 3])
    a = p.parse_args()

    results = {}
    if a.only in (None, 1):
        results[1] = experiment_1(a.servers, a.topic, a.group)
    if a.only in (None, 2):
        results[2] = experiment_2(a.servers, a.topic, a.group, a.events)
    if a.only in (None, 3):
        results[3] = experiment_3(a.servers, a.topic, a.group)
    banner("SUMMARY: " + ", ".join(f"Exp {k}: {'PASS' if v else 'FAIL'}" for k, v in results.items()))
    sys.exit(0 if all(results.values()) else 1)
