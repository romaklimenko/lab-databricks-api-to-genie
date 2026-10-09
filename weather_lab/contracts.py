"""Typed specialist outputs and immutable source facts."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from weather_lab.source import DOCS, FIELDS, PARAMETERS, digest, rows


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IngestionSpec(StrictModel):
    dataset: Literal["weather_daily"] = "weather_daily"
    key: list[str] = Field(default_factory=lambda: ["city_id", "weather_date"])
    source_fields: dict[str, str]
    timezone: str
    model: str
    rationale: str = Field(max_length=1500)


class Column(StrictModel):
    source: str
    unit: str
    description: str = Field(min_length=15, max_length=600)
    aggregation: Literal["sum", "max"]
    evidence_url: str


class DatasetContract(StrictModel):
    version: Literal[1] = 1
    grain: Literal["city and local calendar date"] = "city and local calendar date"
    timezone: str
    source_kind: Literal["reanalysis"] = "reanalysis"
    null_policy: Literal["reject incomplete week"] = "reject incomplete week"
    columns: dict[str, Column]


class Review(StrictModel):
    passed: bool
    findings: list[str] = Field(max_length=10)


class Route(StrictModel):
    specialist: Literal["ingestion", "steward", "review", "publish", "stop"]
    reason: str = Field(max_length=700)


class Intent(StrictModel):
    action: Literal["onboard", "ask", "clarify"]
    question: str = ""
    reason: str = Field(max_length=700)


def reference_spec() -> IngestionSpec:
    return IngestionSpec(
        source_fields={target: source for target, (source, _) in FIELDS.items()},
        timezone=PARAMETERS["timezone"],
        model="era5",
        rationale="Use the API's daily series and explicit requested units.",
    )


def reference_contract() -> DatasetContract:
    descriptions = {
        "max_temperature_c": "Daily maximum air temperature at 2 meters above ground, in degrees Celsius. Reanalysis estimate.",
        "precipitation_mm": "Daily total precipitation in millimeters, including rain, showers, and snowfall. Reanalysis estimate.",
        "max_wind_ms": "Daily maximum wind speed at 10 meters above ground, in meters per second. Reanalysis estimate.",
    }
    return DatasetContract(
        timezone=PARAMETERS["timezone"],
        columns={
            name: Column(
                source=source,
                unit=unit,
                description=descriptions[name],
                aggregation="sum" if name == "precipitation_mm" else "max",
                evidence_url=DOCS,
            )
            for name, (source, unit) in FIELDS.items()
        },
    )


def validate(snapshot: dict, spec: IngestionSpec, contract: DatasetContract) -> list[str]:
    errors = []
    try:
        rows(snapshot)
    except (ValueError, KeyError, TypeError) as exc:
        errors.append(str(exc))
    reference = reference_spec()
    for field in ("dataset", "key", "source_fields", "timezone", "model"):
        if getattr(spec, field) != getattr(reference, field):
            errors.append(f"Ingestion {field} does not match source evidence")
    if contract.timezone != PARAMETERS["timezone"]:
        errors.append("Contract timezone differs from source evidence")
    if set(contract.columns) != set(FIELDS):
        errors.append("Contract must describe exactly the three supported metrics")
    for name, expected in reference_contract().columns.items():
        column = contract.columns.get(name)
        if column is None:
            continue
        for field in ("source", "unit", "aggregation", "evidence_url"):
            if getattr(column, field) != getattr(expected, field):
                errors.append(f"Contract {name}.{field} does not match source evidence")
    return errors


def accepted_hash(snapshot, spec, contract) -> str:
    return digest(
        {"snapshot": snapshot, "spec": spec.model_dump(), "contract": contract.model_dump()}
    )
