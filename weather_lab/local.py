"""Offline replay uses SQLite and reference SQL, without pretending to be Genie."""

import sqlite3
from pathlib import Path

from weather_lab.source import rows

QUESTIONS = [
    (
        "precipitation",
        "Which city had the most total precipitation during September 21-27, 2026? Return all cities and their totals in millimeters, greatest first.",
        "SELECT w.city, ROUND(SUM(w.precipitation_mm), 3) AS total_precipitation_mm FROM {table} AS w GROUP BY w.city ORDER BY total_precipitation_mm DESC, w.city",
    ),
    (
        "temperature",
        "What was the highest daily maximum temperature for each city during the available week? Give degrees Celsius and sort by city.",
        "SELECT w.city, MAX(w.max_temperature_c) AS max_temperature_c FROM {table} AS w GROUP BY w.city ORDER BY w.city",
    ),
    (
        "wind",
        "What was the strongest daily maximum wind in each city during the available week? Give meters per second and sort by city.",
        "SELECT w.city, MAX(w.max_wind_ms) AS max_wind_ms FROM {table} AS w GROUP BY w.city ORDER BY w.city",
    ),
    (
        "coverage",
        "For each city, show the earliest date, latest date, and count of daily rows. Sort by city.",
        "SELECT w.city, MIN(w.weather_date) AS first_date, MAX(w.weather_date) AS last_date, COUNT(*) AS days FROM {table} AS w GROUP BY w.city ORDER BY w.city",
    ),
    (
        "missing_period",
        "Show daily precipitation for Copenhagen during January 2025. If no data covers that period, say so.",
        "SELECT w.weather_date, w.precipitation_mm FROM {table} AS w WHERE w.city = 'Copenhagen' AND w.weather_date >= '2025-01-01' AND w.weather_date < '2025-02-01' ORDER BY w.weather_date",
    ),
]


def replay(snapshot: dict, database: Path) -> dict:
    data = rows(snapshot)
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS weather_daily (city_id TEXT, city TEXT, weather_date TEXT, max_temperature_c REAL, precipitation_mm REAL, max_wind_ms REAL, source_model TEXT, snapshot_sha256 TEXT, PRIMARY KEY(city_id, weather_date))"
        )
        connection.executemany(
            "INSERT INTO weather_daily VALUES (:city_id, :city, :weather_date, :max_temperature_c, :precipitation_mm, :max_wind_ms, :source_model, :snapshot_sha256) ON CONFLICT(city_id, weather_date) DO UPDATE SET city=excluded.city, max_temperature_c=excluded.max_temperature_c, precipitation_mm=excluded.precipitation_mm, max_wind_ms=excluded.max_wind_ms, source_model=excluded.source_model, snapshot_sha256=excluded.snapshot_sha256",
            data,
        )
        count = connection.execute("SELECT COUNT(*) FROM weather_daily").fetchone()[0]
        if count != 21:
            raise ValueError("Replay database contains unexpected rows")
        results = {
            name: [
                list(row)
                for row in connection.execute(sql.format(table="weather_daily")).fetchall()
            ]
            for name, _, sql in QUESTIONS
        }
    return {
        "row_count": count,
        "validated": True,
        "reference_results": results,
        "mode": "offline replay; no LLM or Genie call",
    }
