"""
stage03_transform_foster.py

Source: validated JSON object
Sink: Polars DataFrame

Purpose

  Transform validated JSON data into a structured format.

Analytical Questions

- Which fields are needed from the JSON data?
- How can records be normalized into tabular form?
- What derived fields would support analysis?

"""

import logging
from typing import Any

import polars as pl


def run_transform(
    json_data: dict[str, list[dict[str, Any]]],
    LOG: logging.Logger | None = None,
) -> pl.DataFrame:
    """Transform validated Pokémon JSON into a structured DataFrame."""

    if LOG:
        LOG.info("========================")
        LOG.info("STAGE 03: TRANSFORM starting...")
        LOG.info("========================")

    records: list[dict[str, Any]] = []

    for record in json_data["results"]:
        records.append(
            {
                "name": record["name"],
                "id": record["id"],
                "url": record["url"],
                "types": ", ".join(record["types"]),  # combine types into a string
                "height": record["height"],
                "weight": record["weight"],
            }
        )

    df: pl.DataFrame = pl.DataFrame(records)

    # Derived field: length of Pokémon name
    df = df.with_columns([pl.col("name").str.len_chars().alias("name_length")])

    if LOG:
        LOG.info("Transformation complete.")
        LOG.info(f"DataFrame preview:\n{df.head()}")
        LOG.info("Sink: Polars DataFrame created")

    return df
