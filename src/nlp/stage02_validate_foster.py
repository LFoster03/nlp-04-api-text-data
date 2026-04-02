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


def run_validate(
    json_data: Any,
    LOG: logging.Logger | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Validate JSON data structure for the Pokémon pipeline.

    Args:
        json_data: The raw JSON object from the extract stage.
        LOG: Optional logger instance.

    Returns:
        Validated JSON dictionary.

    Raises:
        ValueError: If the JSON structure is invalid.
    """

    if LOG:
        LOG.info("========================")
        LOG.info("STAGE 02: VALIDATE starting...")
        LOG.info("========================")

    # Ensure top-level is a dictionary
    if not isinstance(json_data, dict):
        raise ValueError("Expected top-level JSON to be a dictionary.")

    # Ensure "results" key exists and is a list
    if "results" not in json_data or not isinstance(json_data["results"], list):
        raise ValueError(
            'Expected JSON dictionary to contain a "results" key with a list.'
        )

    if LOG:
        LOG.info(f'Top-level keys: {list(json_data.keys())}')
        LOG.info(f'Number of Pokémon records: {len(json_data["results"])}')

    # Optional: validate that each Pokémon has required fields
    required_fields = {"name", "id", "types", "url", "height", "weight"}
    for i, record in enumerate(json_data["results"]):
        missing = required_fields - record.keys()
        if missing:
            raise ValueError(f"Record {i} is missing fields: {missing}")

    if LOG:
        LOG.info("Validation complete. All records contain required fields.")

    return json_data
