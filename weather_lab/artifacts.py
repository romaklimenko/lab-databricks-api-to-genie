"""Generate reviewable source artifacts from accepted structured output."""

import json
import uuid
from copy import deepcopy

from weather_lab.config import ROOT, Settings
from weather_lab.contracts import DatasetContract, IngestionSpec, accepted_hash, validate
from weather_lab.source import encoded


def stable_id(value: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, "weather-agent-lab:" + value).hex


def canonical_space(value: dict) -> dict:
    """Genie may split serialized text into newline-preserving chunks."""
    result = deepcopy(value)
    instructions = result.get("instructions", {})
    for item in instructions.get("text_instructions", []):
        item["content"] = ["".join(item.get("content", []))]
    for item in instructions.get("example_question_sqls", []):
        item["sql"] = ["".join(item.get("sql", []))]
    return result


def genie_config(settings: Settings, contract: DatasetContract) -> dict:
    table = settings.table
    columns = [
        {
            "column_name": "city",
            "description": ["Configured Danish city name."],
            "enable_format_assistance": True,
            "enable_entity_matching": True,
        },
        {
            "column_name": "weather_date",
            "description": [
                "Local calendar date in Europe/Copenhagen. Coverage: September 21-27, 2026."
            ],
        },
        {"column_name": "city_id", "exclude": True},
        {"column_name": "snapshot_sha256", "exclude": True},
        {
            "column_name": "source_model",
            "description": ["ERA5 historical reanalysis; values are model estimates."],
        },
        *[
            {"column_name": name, "description": [column.description], "synonyms": [column.source]}
            for name, column in contract.columns.items()
        ],
    ]
    # Examples teach daily filtering and grouping. The evaluation questions stay outside this export.
    examples = [
        (
            "Show daily temperatures for Odense.",
            f"SELECT w.weather_date, w.max_temperature_c FROM {table} AS w WHERE w.city = 'Odense' ORDER BY w.weather_date",
        ),
        (
            "Show precipitation by city on September 23, 2026.",
            f"SELECT w.city, w.precipitation_mm FROM {table} AS w WHERE w.weather_date = DATE '2026-09-23' ORDER BY w.city",
        ),
    ]
    instructions = (
        "## DISAMBIGUATION\n- Ask for criteria when 'best weather' is unspecified.\n\n"
        "## Instructions you must follow when providing summaries\n"
        "- State the units and period used in the answer.\n"
        "- Describe historical weather as reanalysis estimates.\n"
        "- If the requested period has no rows, report missing coverage rather than inventing values."
    )
    return {
        "version": 2,
        "data_sources": {
            "tables": [
                {
                    "identifier": table,
                    "description": [
                        "Daily ERA5 weather for Copenhagen, Aarhus, and Odense. One row per city and local calendar date. September 21-27, 2026."
                    ],
                    "column_configs": sorted(columns, key=lambda column: column["column_name"]),
                }
            ]
        },
        "instructions": {
            "text_instructions": [{"id": stable_id("instructions"), "content": [instructions]}],
            "example_question_sqls": sorted(
                [
                    {"id": stable_id(question), "question": [question], "sql": [sql]}
                    for question, sql in examples
                ],
                key=lambda item: item["id"],
            ),
            "sql_snippets": {
                "measures": [
                    {
                        "id": stable_id("total-precipitation"),
                        "alias": "total_precipitation_mm",
                        "display_name": "Total precipitation in millimeters",
                        "sql": ["SUM(weather_daily.precipitation_mm)"],
                    }
                ]
            },
        },
    }


def render(
    snapshot: dict, spec: IngestionSpec, contract: DatasetContract, settings: Settings
) -> dict[str, bytes]:
    errors = validate(snapshot, spec, contract)
    if errors:
        raise ValueError("Artifact validation failed: " + "; ".join(errors))
    files = {
        "snapshot.json": encoded(snapshot),
        "ingestion.json": encoded(spec.model_dump()),
        "contract.json": encoded(contract.model_dump()),
        "genie.json": encoded(genie_config(settings, contract)),
        "ingest.py": (ROOT / "weather_lab" / "job.py").read_bytes(),
        "manifest.json": encoded(
            {
                "artifact_hash": accepted_hash(snapshot, spec, contract),
                "table": settings.table,
                "expected_rows": 21,
            }
        ),
    }
    compile(files["ingest.py"], "ingest.py", "exec")
    return files


def read_artifacts(files):
    return (
        json.loads(files["snapshot.json"]),
        IngestionSpec.model_validate_json(files["ingestion.json"]),
        DatasetContract.model_validate_json(files["contract.json"]),
    )
