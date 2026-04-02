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

from nlp.config_foster import SUMMARY_CSV_PATH, TYPE_COUNTS_CSV_PATH


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

    # ===============
    # NEW: ANALYSIS
    # ===============

    # Summary statistics
    summary = df.select(
        [
            pl.count().alias("total_pokemon"),
            pl.col("height").mean().alias("avg_height"),
            pl.col("weight").mean().alias("avg_weight"),
        ]
    )

    # Most common Pokémon types
    type_counts = (
        df.select(pl.col("types")).to_series().str.split(", ").explode().value_counts()
    )

    if LOG:
        LOG.info("Transformation complete.")
        LOG.info(f"DataFrame preview:\n{df.head()}")

        # Log summary results
        LOG.info("Summary Statistics:")
        LOG.info(f"\n{summary}")

        LOG.info("Most Common Pokémon Types:")
        LOG.info(f"\n{type_counts}")

        LOG.info("Sink: Polars DataFrame created")

    # ============================================================
    # SAVE ADDITIONAL OUTPUTS
    # ============================================================

    # Ensure directories exist
    SUMMARY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save summary
    summary.write_csv(SUMMARY_CSV_PATH)

    # Save type counts (already a DataFrame)
    type_counts.write_csv(TYPE_COUNTS_CSV_PATH)

    if LOG:
        LOG.info(f"Summary saved to {SUMMARY_CSV_PATH}")
        LOG.info(f"Type counts saved to {TYPE_COUNTS_CSV_PATH}")

    return df
