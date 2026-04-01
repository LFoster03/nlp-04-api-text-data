"""
stage02_validate_foster.py

Source: raw JSON object
Sink: validated JSON object

Purpose

  Inspect JSON structure and validate that the data is usable.

Analytical Questions

- What is the top-level structure of the JSON data?
- What keys are present in each record?
- What data types are associated with each field?
- Does the data meet expectations for transformation?

"""

import logging
from typing import Any


def run_validate(json_data: Any, LOG: logging.Logger) -> list[dict]:
    """Validate Pokémon JSON structure.

    Args:
        json_data (Any): Raw JSON from the Extract stage.
        LOG (logging.Logger): Logger instance.

    Returns:
        list[dict]: Validated Pokémon records (from 'results').
    """
    LOG.info("========================")
    LOG.info("STAGE 02: VALIDATE starting...")
    LOG.info("========================")

    LOG.info(f"Top-level type: {type(json_data).__name__}")

    if not isinstance(json_data, dict):
        raise ValueError("Expected top-level JSON to be a dictionary.")

    if "results" not in json_data:
        raise ValueError("Missing 'results' key in JSON data.")

    results = json_data["results"]

    if not isinstance(results, list):
        raise ValueError("Expected 'results' to be a list.")

    if len(results) == 0:
        raise ValueError("Expected at least one Pokémon record.")

    if not all(isinstance(record, dict) for record in results):
        raise ValueError("All Pokémon records must be dictionaries.")

    LOG.info("Validation passed. Number of Pokémon records: %d", len(results))
    LOG.info("Sink: validated JSON object (results list)")

    return results
