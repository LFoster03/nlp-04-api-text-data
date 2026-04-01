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


def run_transform(json_data: list[dict[str, Any]], LOG: logging.Logger) -> pl.DataFrame:
    """Transform Pokémon JSON into a structured DataFrame.

    Args:
        json_data (list[dict[str, Any]]): Validated Pokémon records.
        LOG (logging.Logger): Logger instance.

    Returns:
        pl.DataFrame: Transformed Pokémon dataset.
    """
    LOG.info("========================")
    LOG.info("STAGE 03: TRANSFORM starting...")
    LOG.info("========================")

    records = []
    for record in json_data:
        records.append(
            {
                "name": record.get("name"),
                "url": record.get("url"),
            }
        )

    df = pl.DataFrame(records)

    # Derived field: length of Pokémon name
    df = df.with_columns([pl.col("name").str.len_chars().alias("name_length")])

    LOG.info("Transformation complete.")
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info("Sink: Polars DataFrame created")

    return df
