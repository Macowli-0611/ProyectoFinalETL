"""Event Contract for Domestic Violence & Institutional Deficit Streaming Pipeline.

Defines the formal schema, required fields, data types, and business constraints
for every message published to and consumed from Apache Kafka.
Complies with section 2.6 of the Final Project Rubric.
"""

from datetime import datetime, timezone
import json
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict


class EventContractViolationError(ValueError):
    """Raised when an incoming Kafka message violates the formal event contract."""
    pass


class ViolenciaIntrafamiliarEvent(BaseModel):
    """Formal Event Contract for real-time domestic violence reports.

    Required fields:
      - event_id: Unique message identifier (UUID v4)
      - timestamp: ISO-8601 UTC timestamp of occurrence/ingestion
      - event_key: Partitioning key matching 5-digit DANE code
      - codigo_dane_municipio: 5-digit municipal DANE identifier (DDMMM)
      - municipio: Municipality name
      - departamento: Department name
      - genero: Victim gender ('FEMENINO', 'MASCULINO', 'NO REPORTA')
      - grupo_etario: Life stage category ('ADULTOS', 'MENORES', 'ADOLESCENTES', etc.)
      - es_menor_de_edad: Boolean flag for children/adolescents under statutory age
      - medio_agresion: Specific aggression mechanism
      - es_arma_letal: Boolean indicating firearm, bladed weapon, or blunt trauma
      - cantidad_victimas: Number of registered victims (strictly >= 1)
      - categoria_municipio: Municipal legal category under Ley 617 ('ESPECIAL', '1', '2', etc.)
      - tiene_deficit_ley_2126: Boolean indicating statutory Comisaría deficit under Ley 2126
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True
    )

    event_id: str = Field(
        ...,
        description="Unique identifier for the event (UUID v4 recommended)",
        min_length=10,
        max_length=64
    )
    timestamp: datetime = Field(
        ...,
        description="Event generation timestamp in UTC (ISO 8601)"
    )
    event_key: str = Field(
        ...,
        description="Kafka message partition key, strictly matching the 5-digit DANE code",
        min_length=5,
        max_length=5
    )
    codigo_dane_municipio: str = Field(
        ...,
        description="5-digit municipal DANE code (DDMMM)",
        min_length=5,
        max_length=5
    )
    municipio: str = Field(
        ...,
        description="Official name of the municipality",
        min_length=2,
        max_length=150
    )
    departamento: str = Field(
        ...,
        description="Official department name",
        min_length=2,
        max_length=100
    )
    genero: str = Field(
        ...,
        description="Victim gender (FEMENINO, MASCULINO, NO REPORTA)",
        min_length=3,
        max_length=50
    )
    grupo_etario: str = Field(
        ...,
        description="Age cohort classification",
        min_length=3,
        max_length=50
    )
    es_menor_de_edad: bool = Field(
        ...,
        description="Flag indicating if victim is a minor"
    )
    medio_agresion: str = Field(
        ...,
        description="Harmonized weapon or aggression mechanism",
        min_length=2,
        max_length=100
    )
    es_arma_letal: bool = Field(
        ...,
        description="Flag indicating lethal weapon (firearm / cutting weapon)"
    )
    cantidad_victimas: int = Field(
        ...,
        description="Number of recorded victims (must be strictly greater than 0)",
        gt=0
    )
    categoria_municipio: str = Field(
        ...,
        description="Municipal legal category (Ley 617)",
        min_length=1,
        max_length=50
    )
    tiene_deficit_ley_2126: bool = Field(
        ...,
        description="Statutory Comisaría deficit indicator according to Ley 2126"
    )

    @field_validator("codigo_dane_municipio")
    @classmethod
    def validate_dane_digits(cls, v: str) -> str:
        if not v.isdigit() or len(v) != 5:
            raise ValueError(f"DANE code must be exactly 5 numeric digits, got '{v}'")
        return v

    @field_validator("event_key")
    @classmethod
    def validate_event_key(cls, v: str, info) -> str:
        if not v.isdigit() or len(v) != 5:
            raise ValueError(f"event_key must be a valid 5-digit DANE code, got '{v}'")
        return v

    @field_validator("genero")
    @classmethod
    def validate_genero_standard(cls, v: str) -> str:
        standard = v.upper()
        if standard not in {"FEMENINO", "MASCULINO", "NO REPORTA", "OTRO"}:
            raise ValueError(f"Unexpected gender category '{v}'")
        return standard

    def to_kafka_dict(self) -> dict:
        """Serializes the event into a clean JSON-serializable dictionary."""
        data = self.model_dump()
        data["timestamp"] = self.timestamp.isoformat()
        return data

    @classmethod
    def from_raw_json(cls, payload: str | bytes | dict) -> "ViolenciaIntrafamiliarEvent":
        """Parses and strictly validates raw payload from Kafka."""
        try:
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            if isinstance(payload, str):
                data = json.loads(payload)
            else:
                data = payload
            return cls(**data)
        except Exception as e:
            raise EventContractViolationError(f"Event contract validation failed: {str(e)}") from e

