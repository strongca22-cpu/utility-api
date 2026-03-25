#!/usr/bin/env python3
"""
Utility API Configuration

Purpose:
    Pydantic Settings loader for database connection and pipeline config.
    Reads from .env file in project root.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - pydantic-settings
    - pyyaml

Usage:
    from utility_api.config import settings
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings


# Project root (two levels up from this file: src/utility_api/config.py)
PROJECT_ROOT = Path(__file__).parents[2]


# Load .env into os.environ so third-party SDKs (e.g. Anthropic) can find their keys.
# Pydantic Settings reads .env for its own fields but does NOT export to os.environ.
def _load_env_file() -> None:
    """Parse .env and export key=value pairs to os.environ (no overwrite)."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file()


class Settings(BaseSettings):
    """Application settings loaded from environment / .env file."""

    database_url: str = (
        "postgresql+psycopg://strong_strategic:changeme@localhost:5432/strong_strategic"
    )
    utility_schema: str = "utility"
    api_port: int = 8000
    aqueduct_gdb_path: str = str(
        Path("/data/datasets/strong-strategic/raw/aqueduct")
        / "Aqueduct40_waterrisk_download_Y2023M07D05/GDB/Aq40_Y2023D07M05.gdb"
    )

    model_config = {
        "env_file": str(PROJECT_ROOT / ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


def load_sources_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load data source configuration from YAML."""
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "sources.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


settings = Settings()
