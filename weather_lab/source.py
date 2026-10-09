"""Bounded Open-Meteo acquisition and strict fixture validation."""

import hashlib
import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

URL = "https://archive-api.open-meteo.com/v1/archive"
DOCS = "https://open-meteo.com/en/docs/historical-weather-api"
CITIES = [
    {"city_id": "copenhagen", "city": "Copenhagen", "latitude": 55.6761, "longitude": 12.5683},
    {"city_id": "aarhus", "city": "Aarhus", "latitude": 56.1629, "longitude": 10.2039},
    {"city_id": "odense", "city": "Odense", "latitude": 55.4038, "longitude": 10.4024},
]
PARAMETERS = {
    "start_date": "2026-09-21",
    "end_date": "2026-09-27",
    "daily": "temperature_2m_max,precipitation_sum,wind_speed_10m_max",
    "models": "era5",
    "timezone": "Europe/Copenhagen",
    "temperature_unit": "celsius",
    "wind_speed_unit": "ms",
    "precipitation_unit": "mm",
}
FIELDS = {
    "max_temperature_c": ("temperature_2m_max", "°C"),
    "precipitation_mm": ("precipitation_sum", "mm"),
    "max_wind_ms": ("wind_speed_10m_max", "m/s"),
}


def encoded(value) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode()


def digest(value) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_snapshot() -> dict:
    """Fetch exactly three documented public locations, with bounded retries."""
    locations = []
    transport = httpx.HTTPTransport(retries=2)
    with httpx.Client(timeout=30, transport=transport, follow_redirects=False) as client:
        for city in CITIES:
            params = {**PARAMETERS, "latitude": city["latitude"], "longitude": city["longitude"]}
            response = client.get(URL, params=params)
            response.raise_for_status()
            if len(response.content) > 1_000_000:
                raise ValueError("Weather response exceeds fixture size limit")
            locations.append({"location": city, "response": response.json()})
    snapshot = {
        "source_url": URL,
        "documentation_url": DOCS,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "parameters": PARAMETERS,
        "licence": "CC BY 4.0",
        "licence_url": "https://open-meteo.com/en/licence",
        "attribution": "Weather data by Open-Meteo, using ERA5 reanalysis from Copernicus/ECMWF.",
        "locations": locations,
    }
    rows(snapshot)
    return snapshot


def rows(snapshot: dict) -> list[dict]:
    """Validate coverage, response units, and finite values before producing rows."""
    if snapshot.get("parameters") != PARAMETERS or snapshot.get("source_url") != URL:
        raise ValueError("Snapshot is outside the fixed lab source and date scope")
    expected_days = [(date(2026, 9, 21) + timedelta(days=i)).isoformat() for i in range(7)]
    records, seen = [], set()
    locations = snapshot.get("locations", [])
    if len(locations) != len(CITIES):
        raise ValueError("Expected exactly three locations")
    source_hash = digest(snapshot)
    for item in locations:
        city, response = item["location"], item["response"]
        if city not in CITIES or city["city_id"] in seen:
            raise ValueError("Unexpected or duplicate city")
        seen.add(city["city_id"])
        if response.get("timezone") != PARAMETERS["timezone"]:
            raise ValueError("Response timezone does not match the contract")
        daily, units = response["daily"], response["daily_units"]
        if daily.get("time") != expected_days:
            raise ValueError("Incomplete or duplicate daily coverage")
        for source, unit in FIELDS.values():
            if units.get(source) != unit:
                raise ValueError(f"Unit mismatch for {source}: expected {unit}")
            if len(daily.get(source, [])) != 7:
                raise ValueError(f"Missing daily values for {source}")
        for i, day in enumerate(expected_days):
            row = {"city_id": city["city_id"], "city": city["city"], "weather_date": day}
            for target, (source, _) in FIELDS.items():
                value = daily[source][i]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    raise ValueError(f"Missing or non-finite value for {source}")
                if target != "max_temperature_c" and value < 0:
                    raise ValueError(f"Negative value for {source}")
                row[target] = float(value)
            row.update(source_model="era5", snapshot_sha256=source_hash)
            records.append(row)
    return sorted(records, key=lambda row: (row["city_id"], row["weather_date"]))
