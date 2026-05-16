"""Three assets per manifest row:

    {table}__source   →   {table}__stage   →   {table}__landing

  - source:  external source — no compute, lineage only
  - stage:   generate random rows, write parquet, PUT to internal stage,
             return the stage path
  - landing: COPY INTO from the staged file, full replace
"""

import random
import tempfile
from pathlib import Path
from typing import Tuple

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    MetadataValue,
    Output,
    SourceAsset,
    asset,
)

from pipeline_simple.snowflake_resource import SnowflakeResource


def _random_rows(table: str, n: int = 50) -> list[dict]:
    rng = random.Random()
    return [
        {
            "id": rng.randint(1, 10**9),
            "name": f"{table}_{i:04d}",
            "value": round(rng.uniform(1, 1000), 2),
            "status": rng.choice(["active", "pending", "closed"]),
        }
        for i in range(n)
    ]


def build_assets_for_table(
    table: str,
    database: str = "sources",
) -> Tuple[SourceAsset, AssetsDefinition, AssetsDefinition]:
    """Return (source_asset, stage_asset, landing_asset) for one table."""

    source_key = AssetKey([f"{table}__source"])
    stage_key = AssetKey([f"{table}__stage"])

    source_asset = SourceAsset(
        key=source_key,
        group_name="sources",
        tags={"database": database},
        description=f"External source data for {table} in {database}. No compute — lineage only.",
    )

    @asset(
        name=f"{table}__stage",
        group_name="staging",
        compute_kind="snowflake_stage",
        deps=[source_key],
        tags={"database": database},
        description=f"Generate random rows for {table} and PUT to Snowflake internal stage.",
    )
    def stage_asset(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        rows = _random_rows(table)
        df = pd.DataFrame(rows)

        with tempfile.NamedTemporaryFile(
            suffix=f"_{table}.parquet", delete=False
        ) as tmp:
            df.to_parquet(tmp.name, index=False)
            local_path = tmp.name

        try:
            stage_path = snowflake.stage_file(
                table=table.upper(),
                local_parquet=local_path,
            )
        finally:
            Path(local_path).unlink(missing_ok=True)

        # Return both the stage path and the column list so landing doesn't
        # need to re-read the parquet. Dagster passes this dict as input
        # to the landing asset.
        result = {"stage_path": stage_path, "columns": list(df.columns)}

        context.log.info(f"Staged {len(df)} rows at {stage_path}")
        return Output(
            value=result,
            metadata={
                "row_count": len(df),
                "stage_path": MetadataValue.text(stage_path),
                "preview": MetadataValue.md(df.head().to_markdown(index=False)),
            },
        )

    @asset(
        name=f"{table}__landing",
        group_name="landing",
        compute_kind="snowflake_table",
        ins={"staged": AssetIn(stage_key)},
        tags={"database": database},
        description=f"Full-replace BRONZE.{table.upper()} from the staged parquet.",
    )
    def landing_asset(
        context: AssetExecutionContext,
        staged: dict,
        snowflake: SnowflakeResource,
    ) -> Output[int]:
        row_count = snowflake.load_from_stage(
            table=table.upper(),
            stage_path=staged["stage_path"],
            columns=staged["columns"],
        )
        context.log.info(f"Loaded {row_count} rows into BRONZE.{table.upper()}")
        return Output(
            value=row_count,
            metadata={
                "row_count": row_count,
                "source_stage": MetadataValue.text(staged["stage_path"]),
            },
        )

    return source_asset, stage_asset, landing_asset


def build_all_assets(manifest: list[tuple[str, str]]) -> list:
    """Flatten (source, stage, landing) triples into a single list."""
    out: list = []
    for table, database in manifest:
        source, stage, landing = build_assets_for_table(table, database)
        out.extend([source, stage, landing])
    return out
