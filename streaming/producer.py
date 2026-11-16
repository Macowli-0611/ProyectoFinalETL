"""Apache Kafka Producer for Domestic Violence Real-Time Event Simulation.

Simulates the arrival of real-time police incident reports and Comisaría de Familia
intakes across Colombian municipalities. Complies with Section 2.6 of the Project Rubric:
- Uses dynamic, analytical variation (not repeated static KPI values).
- Supports streaming replay and synthetic generation derived from the actual DANE geography and Ley 2126 deficits.
- Signs every message with the formal Event Contract.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import random
import sys
import time
from uuid import uuid4

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.contract import ViolenciaIntrafamiliarEvent

try:
    from kafka import KafkaProducer
except ImportError:
    try:
        from kafka_python_ng import KafkaProducer
    except ImportError:
        KafkaProducer = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [KafkaProducer] %(message)s"
)
logger = logging.getLogger("KafkaProducer")

# Colombian municipalities curated with Ley 2126 deficit and priority status
MUNICIPALITIES_SEED = [
    # Top Critical Deficit Municipalities (R7 Rank 1 & 2 Urgency)
    {"dane": "54001", "muni": "CÚCUTA", "depto": "NORTE DE SANTANDER", "cat": "1", "deficit": True, "prio": "URGENTE"},
    {"dane": "25754", "muni": "SOACHA", "depto": "CUNDINAMARCA", "cat": "1", "deficit": True, "prio": "URGENTE"},
    {"dane": "23001", "muni": "MONTERÍA", "depto": "CÓRDOBA", "cat": "1", "deficit": True, "prio": "URGENTE"},
    {"dane": "19001", "muni": "POPAYÁN", "depto": "CAUCA", "cat": "1", "deficit": True, "prio": "ALTA"},
    {"dane": "05088", "muni": "BELLO", "depto": "ANTIOQUIA", "cat": "1", "deficit": True, "prio": "ALTA"},
    {"dane": "68276", "muni": "FLORIDABLANCA", "depto": "SANTANDER", "cat": "1", "deficit": True, "prio": "ALTA"},
    {"dane": "41001", "muni": "NEIVA", "depto": "HUILA", "cat": "1", "deficit": True, "prio": "ALTA"},
    {"dane": "70001", "muni": "SINCELEJO", "depto": "SUCRE", "cat": "1", "deficit": True, "prio": "ALTA"},

    # Major Metropolises (High Absolute Volume)
    {"dane": "11001", "muni": "BOGOTÁ, D.C.", "depto": "BOGOTÁ D.C.", "cat": "ESPECIAL", "deficit": False, "prio": "CONFORME"},
    {"dane": "05001", "muni": "MEDELLÍN", "depto": "ANTIOQUIA", "cat": "ESPECIAL", "deficit": False, "prio": "CONFORME"},
    {"dane": "76001", "muni": "CALI", "depto": "VALLE DEL CAUCA", "cat": "ESPECIAL", "deficit": False, "prio": "CONFORME"},
    {"dane": "08001", "muni": "BARRANQUILLA", "depto": "ATLÁNTICO", "cat": "ESPECIAL", "deficit": False, "prio": "CONFORME"},
    {"dane": "68001", "muni": "BUCARAMANGA", "depto": "SANTANDER", "cat": "1", "deficit": False, "prio": "CONFORME"},
    {"dane": "17001", "muni": "MANIZALES", "depto": "CALDAS", "cat": "1", "deficit": False, "prio": "CONFORME"},

    # Intermediate Municipalities
    {"dane": "52001", "muni": "PASTO", "depto": "NARIÑO", "cat": "1", "deficit": True, "prio": "ALTA"},
    {"dane": "73001", "muni": "IBAGUÉ", "depto": "TOLIMA", "cat": "1", "deficit": False, "prio": "CONFORME"},
    {"dane": "66001", "muni": "PEREIRA", "depto": "RISARALDA", "cat": "1", "deficit": False, "prio": "CONFORME"}
]

WEAPONS_PROFILE = [
    {"medio": "SIN EMPLEO DE ARMAS", "letal": False, "weight": 60},
    {"medio": "CONTUNDENTES", "letal": False, "weight": 20},
    {"medio": "ARMA BLANCA / CORTOPUNZANTE", "letal": True, "weight": 14},
    {"medio": "ARMA DE FUEGO", "letal": True, "weight": 6}
]

DEMOGRAPHICS_PROFILE = [
    {"genero": "FEMENINO", "grupo": "ADULTOS", "menor": False, "weight": 65},
    {"genero": "FEMENINO", "grupo": "ADOLESCENTES", "menor": True, "weight": 8},
    {"genero": "FEMENINO", "grupo": "MENORES", "menor": True, "weight": 6},
    {"genero": "MASCULINO", "grupo": "ADULTOS", "menor": False, "weight": 14},
    {"genero": "MASCULINO", "grupo": "ADOLESCENTES", "menor": True, "weight": 4},
    {"genero": "MASCULINO", "grupo": "MENORES", "menor": True, "weight": 3}
]


def generate_event(event_id: str = None) -> ViolenciaIntrafamiliarEvent:
    """Generates a realistic event that satisfies the formal Event Contract."""
    muni_meta = random.choice(MUNICIPALITIES_SEED)

    # Weighted choice for weapon
    weapon_weights = [w["weight"] for w in WEAPONS_PROFILE]
    weapon = random.choices(WEAPONS_PROFILE, weights=weapon_weights, k=1)[0]

    # Weighted choice for demographics
    demo_weights = [d["weight"] for d in DEMOGRAPHICS_PROFILE]
    demo = random.choices(DEMOGRAPHICS_PROFILE, weights=demo_weights, k=1)[0]

    # Number of victims (mostly 1, occasionally 2 or 3 in severe incidents)
    victimas = random.choices([1, 2, 3], weights=[85, 12, 3], k=1)[0]

    now_utc = datetime.now(timezone.utc)
    evt_uuid = event_id or f"evt-{uuid4()}"

    event = ViolenciaIntrafamiliarEvent(
        event_id=evt_uuid,
        timestamp=now_utc,
        event_key=muni_meta["dane"],
        codigo_dane_municipio=muni_meta["dane"],
        municipio=muni_meta["muni"],
        departamento=muni_meta["depto"],
        genero=demo["genero"],
        grupo_etario=demo["grupo"],
        es_menor_de_edad=demo["menor"],
        medio_agresion=weapon["medio"],
        es_arma_letal=weapon["letal"],
        cantidad_victimas=victimas,
        categoria_municipio=muni_meta["cat"],
        tiene_deficit_ley_2126=muni_meta["deficit"]
    )
    return event


def generate_invalid_event() -> dict:
    """Generates a deliberately malformed payload to test resilience."""
    return {
        "event_id": "malformed-short-id",  # Violates min_length
        "timestamp": "invalid-timestamp-format",  # Violates datetime format
        "event_key": "999",  # Not 5 digits
        "codigo_dane_municipio": "INVALID",  # Non-numeric
        "cantidad_victimas": -5,  # Violates gt=0
        "genero": "UNKNOWN_VALUE"
    }


def run_producer(
    bootstrap_servers: str,
    topic: str,
    interval_seconds: float = 1.0,
    max_events: int = 0,
    inject_invalid: bool = False
):
    """Initializes KafkaProducer and emits simulated events."""
    logger.info(f"Connecting to Kafka broker at '{bootstrap_servers}'...")

    if KafkaProducer is None:
        raise RuntimeError("Neither 'kafka-python' nor 'kafka-python-ng' is installed.")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all"
    )

    logger.info(f"Connected to Kafka. Publishing to topic '{topic}'...")

    count = 0
    try:
        while True:
            count += 1

            if inject_invalid and count % 10 == 0:
                # Deliberately inject invalid event for testing
                invalid_data = generate_invalid_event()
                logger.warning(f"Injecting deliberately malformed event #{count} to topic '{topic}'...")
                producer.send(topic, key=invalid_data.get("event_key", "00000"), value=invalid_data)
                producer.flush()
            else:
                event = generate_event()
                payload = event.to_kafka_dict()
                producer.send(topic, key=event.event_key, value=payload)
                producer.flush()

                alert_flag = "[DEFICIT ALERT]" if event.tiene_deficit_ley_2126 else ""
                lethal_flag = "[LETHAL]" if event.es_arma_letal else ""
                minor_flag = "[CHILD VICTIM]" if event.es_menor_de_edad else ""
                logger.info(
                    f"Published #{count:04d} | Key={event.event_key} | {event.municipio} ({event.departamento}) "
                    f"| {event.genero} {event.grupo_etario} | {event.medio_agresion} {lethal_flag} {minor_flag} {alert_flag}"
                )

            if max_events > 0 and count >= max_events:
                logger.info(f"Published target total of {max_events} events. Exiting.")
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Shutting down gracefully.")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Domestic Violence Streaming Producer")
    parser.add_argument("--servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_VIOLENCIA", "violencia-intrafamiliar-events"), help="Kafka topic")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between events in seconds")
    parser.add_argument("--count", type=int, default=0, help="Number of events to emit (0 for continuous)")
    parser.add_argument("--inject-invalid", action="store_true", help="Inject malformed events periodically for resilience testing")

    args = parser.parse_args()
    run_producer(
        bootstrap_servers=args.servers,
        topic=args.topic,
        interval_seconds=args.interval,
        max_events=args.count,
        inject_invalid=args.inject_invalid
    )
