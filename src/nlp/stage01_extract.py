"""
stage01_extract.py - Stage 01: Extract
(NO EDITS REQUIRED IN THIS FILE)

Source: API
Sink: raw JSON object + raw JSON file

The Extract stage is responsible for retrieving data
from the source (in this case, an API)
and saving it to a file.
It also returns the extracted data as a Python object
for use in subsequent stages.

Notes

- This file should not require modification.
"""

import json
import logging
from pathlib import Path
from typing import Any

import requests

from nlp.config_foster import API_URL, HTTP_REQUEST_HEADERS, RAW_JSON_PATH


def run_extract(
    source_api_url: str = API_URL,
    http_request_headers: dict[str, str] = HTTP_REQUEST_HEADERS,
    raw_json_path: Path = RAW_JSON_PATH,
    LOG: logging.Logger | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Extract all Pokémon from API, including details, and wrap in a dictionary for validation."""

    if LOG:
        LOG.info("========================")
        LOG.info("STAGE 01: EXTRACT starting...")
        LOG.info("========================")

    all_pokemon = []
    url = source_api_url

    while url:
        response = requests.get(url, headers=http_request_headers)
        data = response.json()

        for p in data["results"]:
            details = requests.get(p["url"], headers=http_request_headers).json()
            pokemon_record = {
                "name": details["name"],
                "id": details["id"],
                "types": [t["type"]["name"] for t in details["types"]],
                "url": p["url"],
                "height": details["height"],
                "weight": details["weight"],
            }
            all_pokemon.append(pokemon_record)

        url = data["next"]

    # Wrap list in dictionary for validator
    payload = {"results": all_pokemon}

    # Ensure directory exists
    raw_json_path.parent.mkdir(parents=True, exist_ok=True)

    # Save raw JSON
    with raw_json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    if LOG:
        LOG.info(f"Extracted {len(all_pokemon)} Pokémon")
        LOG.info(f"Raw JSON saved to {raw_json_path}")

    return payload
