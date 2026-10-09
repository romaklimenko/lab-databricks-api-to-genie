"""Configuration and identifier validation."""

import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")


def identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError("Identifiers must contain only letters, digits, and underscores")
    return value


@dataclass(frozen=True)
class Settings:
    target: str = "local"
    catalog: str = "lab"
    schema: str = "weather_agents_lab"
    warehouse_id: str = ""
    state_volume: str = ""
    github_repo: str = ""
    runs_dir: Path = Path(".runs")
    model: str = "system.ai.claude-sonnet-4-5"
    job_environment: str = "4"

    def __post_init__(self):
        identifier(self.catalog)
        identifier(self.schema)
        if not self.schema.startswith("weather_agents_lab"):
            raise ValueError("Use a dedicated schema beginning with weather_agents_lab")
        if self.target not in {"local", "cloud"}:
            raise ValueError("WEATHER_TARGET must be local or cloud")
        if self.github_repo and not re.fullmatch(r"[\w.-]+/[\w.-]+", self.github_repo):
            raise ValueError("WEATHER_GITHUB_REPO must be owner/repository")

    @property
    def table(self) -> str:
        return f"{self.catalog}.{self.schema}.weather_daily"

    @property
    def volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/artifacts"

    @classmethod
    def from_env(cls):
        return cls(
            target=os.getenv("WEATHER_TARGET", "local"),
            catalog=os.getenv("WEATHER_CATALOG", "lab"),
            schema=os.getenv("WEATHER_SCHEMA", "weather_agents_lab"),
            warehouse_id=os.getenv("WEATHER_WAREHOUSE_ID", ""),
            state_volume=os.getenv("WEATHER_STATE_VOLUME", ""),
            github_repo=os.getenv("WEATHER_GITHUB_REPO", ""),
            runs_dir=Path(os.getenv("WEATHER_RUNS_DIR", ".runs")),
            model=os.getenv("WEATHER_MODEL", "system.ai.claude-sonnet-4-5"),
            job_environment=os.getenv("WEATHER_JOB_ENVIRONMENT", "4"),
        )
