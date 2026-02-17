from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    db_path: Path
    log_level: str
    google_maps_api_key: str | None
    openai_api_key: str | None

def get_settings() -> Settings:
    db_path = Path(os.getenv("DB_PATH", "territory.db")).resolve()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    google_key = os.getenv("GOOGLE_MAPS_API_KEY") or None
    openai_key = os.getenv("OPENAI_API_KEY") or None

    return Settings(
        db_path=db_path,
        log_level=log_level,
        google_maps_api_key=google_key,
        openai_api_key=openai_key,
    )