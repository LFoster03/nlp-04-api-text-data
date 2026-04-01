"""
config_foster.py

Purpose

  Store configuration values for the EVTL pipeline.

Analytical Questions

- What API endpoint should be used as the data source?
- What HTTP request headers are required?
- Where should raw and processed data be stored?

"""

from pathlib import Path

# ============================================================
# API CONFIGURATION
# ============================================================

API_URL: str = "https://pokeapi.co/api/v2/pokemon?limit=20"

HTTP_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": "pokemon-analysis-foster/1.0",
    "Accept": "application/json",
}

# ============================================================
# PATH CONFIGURATION
# ============================================================

ROOT_PATH: Path = Path.cwd()
DATA_PATH: Path = ROOT_PATH / "data"
RAW_PATH: Path = DATA_PATH / "raw"
PROCESSED_PATH: Path = DATA_PATH / "processed"

# Custom file names for your project
RAW_JSON_PATH: Path = RAW_PATH / "pokemon_raw.json"
PROCESSED_CSV_PATH: Path = PROCESSED_PATH / "pokemon_processed.csv"
