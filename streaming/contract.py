"""Event Contract (v2) for the domestic-violence streaming pipeline.

Every message published to / consumed from the Kafka topic must satisfy this
contract. The consumer rejects anything else into the Dead-Letter Queue table
(``streaming_invalid_events_log``) without stopping.

Contract summary
----------------
* Event identifier : ``event_id`` - unique, deterministic for DW replays
                     (``dw-fact-<id_hecho>``) so re-deliveries are idempotent.
* Timestamp        : ``timestamp`` - UTC ISO-8601 publication time.
* Event key        : ``event_key`` - 5-digit DANE municipality code; it is also
                     the Kafka message key, so all events of one municipality
                     land in the same partition (per-municipality ordering).
* Business fields  : ``fecha_hecho`` (date of the incident in the DW),
                     municipality / department, victim cohort, aggression
                     mechanism, lethality flag, number of victims and the
                     Ley 2126 deficit flag of the municipality.
* Provenance       : ``origen`` - ``DW_REPLAY_SIMULATION`` for historical fact
                     rows replayed from the Data Warehouse (clearly marked as a
                     simulation of event arrival) or ``TEST_INJECTION`` for
                     events produced by the resilience experiments.
"""

from datetime import date, datetime
import json
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CONTRACT_VERSION = "2.0"


class EventContractViolationError(ValueError):
    """Raised when an incoming Kafka message violates the formal event contract."""


class ViolenciaIntrafamiliarEvent(BaseModel):
    """Formal Event Contract for domestic-violence report events."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)

    # --- Identity, time and key -------------------------------------------
    contract_version: Literal["2.0"] = Field(CONTRACT_VERSION, description="Contract version")
    event_id: str = Field(..., min_length=10, max_length=64, description="Unique event identifier")
    timestamp: datetime = Field(..., description="UTC publication timestamp (ISO-8601)")
    event_key: str = Field(..., min_length=5, max_length=5, description="Kafka key = DANE municipality code")
    origen: Literal["DW_REPLAY_SIMULATION", "TEST_INJECTION"] = Field(..., description="Event provenance")
    id_hecho_origen: Optional[int] = Field(None, ge=1, description="Source fact row id (DW replays)")

    # --- Business payload --------------------------------------------------
    fecha_hecho: date = Field(..., description="Date of the incident as stored in dim_tiempo")
    codigo_dane_municipio: str = Field(..., min_length=5, max_length=5)
    municipio: str = Field(..., min_length=2, max_length=150)
    departamento: str = Field(..., min_length=2, max_length=100)
    genero: str = Field(..., min_length=3, max_length=50)
    grupo_etario: str = Field(..., min_length=3, max_length=50)
    es_menor_de_edad: bool
    medio_agresion: str = Field(..., min_length=2, max_length=100)
    es_arma_letal: bool
    cantidad_victimas: int = Field(..., gt=0, le=1000)
    categoria_municipio: str = Field(..., min_length=1, max_length=50)
    tiene_deficit_ley_2126: bool

    @field_validator("codigo_dane_municipio", "event_key")
    @classmethod
    def _dane_5_digits(cls, v: str) -> str:
        if not v.isdigit() or len(v) != 5:
            raise ValueError(f"must be a 5-digit DANE code, got '{v}'")
        return v

    @field_validator("genero")
    @classmethod
    def _genero_domain(cls, v: str) -> str:
        v = v.upper()
        if v not in {"FEMENINO", "MASCULINO", "NO REPORTA", "NO REPORTADO", "OTRO"}:
            raise ValueError(f"unexpected gender category '{v}'")
        return v

    @field_validator("timestamp")
    @classmethod
    def _timestamp_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include a UTC offset")
        return v

    @model_validator(mode="after")
    def _key_matches_municipality(self):
        if self.event_key != self.codigo_dane_municipio:
            raise ValueError("event_key must equal codigo_dane_municipio")
        if self.origen == "DW_REPLAY_SIMULATION" and self.id_hecho_origen is None:
            raise ValueError("DW replay events must carry id_hecho_origen")
        return self

    def to_kafka_dict(self) -> dict:
        """JSON-serialisable representation used as the Kafka message value."""
        return self.model_dump(mode="json")

    @classmethod
    def from_raw_json(cls, payload) -> "ViolenciaIntrafamiliarEvent":
        """Parses and strictly validates a raw Kafka payload (bytes, str or dict)."""
        try:
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            data = json.loads(payload) if isinstance(payload, str) else payload
            if not isinstance(data, dict):
                raise ValueError("payload must be a JSON object")
            return cls(**data)
        except Exception as e:  # noqa: BLE001 - every failure is a contract violation
            raise EventContractViolationError(f"Event contract validation failed: {e}") from e
