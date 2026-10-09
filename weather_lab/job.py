"""Standalone generated job entrypoint. Runs only reviewed artifact packages.

This file is copied verbatim into each accepted artifact package.
The source adapter is generated as ingestion.json and contract.json beside it.
"""

import argparse
import hashlib
import json
import math
import re
from datetime import date, timedelta
from pathlib import Path


def run(artifact_root: str, catalog: str, schema: str) -> dict:
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("Invalid catalog")
    if not re.fullmatch(r"weather_agents_lab[A-Za-z0-9_]*", schema):
        raise ValueError("Only lab schemas are supported")
    root = Path(artifact_root)
    expected_prefix = f"/Volumes/{catalog}/{schema}/artifacts/runs/"
    if not artifact_root.startswith(expected_prefix) or ".." in root.parts:
        raise ValueError("Artifact directory must be inside the lab volume")
    snapshot = json.loads((root / "snapshot.json").read_text())
    spec = json.loads((root / "ingestion.json").read_text())
    contract = json.loads((root / "contract.json").read_text())
    manifest = json.loads((root / "manifest.json").read_text())

    def encode(value):
        return (
            json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
        ).encode()

    actual_hash = hashlib.sha256(
        encode({"snapshot": snapshot, "spec": spec, "contract": contract})
    ).hexdigest()
    if actual_hash != manifest["artifact_hash"]:
        raise ValueError("Artifacts changed after review")
    fields = {
        "max_temperature_c": ("temperature_2m_max", "°C"),
        "precipitation_mm": ("precipitation_sum", "mm"),
        "max_wind_ms": ("wind_speed_10m_max", "m/s"),
    }
    expected_dates = [(date(2026, 9, 21) + timedelta(days=i)).isoformat() for i in range(7)]
    records, keys = [], set()
    snapshot_hash = hashlib.sha256(encode(snapshot)).hexdigest()
    for item in snapshot["locations"]:
        response, city = item["response"], item["location"]
        if (
            response["daily"]["time"] != expected_dates
            or response["timezone"] != "Europe/Copenhagen"
        ):
            raise ValueError("Invalid dates or timezone")
        if city["city_id"] not in {"copenhagen", "aarhus", "odense"}:
            raise ValueError("Unexpected city")
        for i, day in enumerate(expected_dates):
            key = (city["city_id"], day)
            if key in keys:
                raise ValueError("Duplicate source key")
            keys.add(key)
            row = [city["city_id"], city["city"], date.fromisoformat(day)]
            for field, (source, unit) in fields.items():
                if (
                    response["daily_units"][source] != unit
                    or contract["columns"][field]["unit"] != unit
                ):
                    raise ValueError("Unit mismatch")
                value = response["daily"][source][i]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    raise ValueError("Missing or invalid source value")
                row.append(float(value))
            records.append((*row, "era5", snapshot_hash))
    if len(records) != 21:
        raise ValueError("Expected 21 rows")
    spark = SparkSession.builder.getOrCreate()
    table = f"{catalog}.{schema}.weather_daily"
    frame = spark.createDataFrame(
        records,
        "city_id STRING, city STRING, weather_date DATE, max_temperature_c DOUBLE, precipitation_mm DOUBLE, max_wind_ms DOUBLE, source_model STRING, snapshot_sha256 STRING",
    )
    if not spark.catalog.tableExists(table):
        spark.sql(
            f"CREATE TABLE {table} (city_id STRING, city STRING, weather_date DATE, max_temperature_c DOUBLE, precipitation_mm DOUBLE, max_wind_ms DOUBLE, source_model STRING, snapshot_sha256 STRING) USING DELTA TBLPROPERTIES ('weather_lab.owner' = 'api-to-genie')"
        )
    if spark.catalog.tableExists(table):
        properties = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        if properties.get("weather_lab.owner") != "api-to-genie":
            raise ValueError("Existing table is not owned by this lab")
        DeltaTable.forName(spark, table).alias("target").merge(
            frame.alias("source"),
            "target.city_id = source.city_id AND target.weather_date = source.weather_date",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    actual = spark.table(table)
    if actual.count() != 21 or actual.select("city_id", "weather_date").distinct().count() != 21:
        raise ValueError("Loaded table failed row-count or key validation")
    expected_rows = sorted(tuple(r) for r in frame.collect())
    actual_rows = sorted(tuple(r) for r in actual.select(*frame.columns).collect())
    if actual_rows != expected_rows:
        raise ValueError("Loaded data differs from the reviewed snapshot")
    report = {
        "row_count": 21,
        "unique_keys": 21,
        "artifact_hash": actual_hash,
        "snapshot_sha256": snapshot_hash,
        "validated": True,
    }
    (root / "load-result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.artifact_root, args.catalog, args.schema)))
