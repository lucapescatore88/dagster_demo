"""Three assets + two checks per manifest row:

    {table}__source  →  {table}__stage  →  {table}__landing
                              ↓                    ↓
                     stage_file_exists    rowcount_and_max_id
                       (asset check)        (asset check)

  - source:  external source — no compute, lineage only
  - stage:   REMOVE old stage files, generate random rows, PUT parquet
  - landing: COPY INTO from staged parquet, full replace
"""

import random
import tempfile
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    MetadataValue,
    Output,
    SourceAsset,
    asset,
    asset_check,
)

from pipeline_simple.snowflake_resource import SnowflakeResource

_STAGE_MAX_AGE_SECONDS = 600  # file must be newer than 10 min


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
) -> Tuple[SourceAsset, AssetsDefinition, AssetsDefinition, list]:
    """Return (source_asset, stage_asset, landing_asset, [checks]) for one table."""

    source_key = AssetKey([f"{table}__source"])
    stage_key = AssetKey([f"{table}__stage"])
    landing_key = AssetKey([f"{table}__landing"])

    source_asset = SourceAsset(
        key=source_key,
        group_name=database,
        tags={"asset_type": "source"},
        description=f"External source data for {table} in {database}. No compute — lineage only.",
    )

    @asset(
        name=f"{table}__stage",
        group_name=database,
        compute_kind="snowflake_stage",
        deps=[source_key],
        tags={"asset_type": "staging"},
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
                database=database,
            )
        finally:
            Path(local_path).unlink(missing_ok=True)

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
        group_name=database,
        compute_kind="snowflake_table",
        ins={"staged": AssetIn(stage_key)},
        tags={"asset_type": "landing"},
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

    # ── Asset checks ────────────────────────────────────────────────────────

    @asset_check(
        asset=stage_key,
        name="stage_file_exists",
        description="Verify a file was uploaded to the stage within the last 10 minutes.",
    )
    def stage_file_check(snowflake: SnowflakeResource) -> AssetCheckResult:
        files = snowflake.list_stage_files(database=database, table=table.upper())
        if not files:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No file found in stage.",
            )
        try:
            last_modified = parsedate_to_datetime(files[0]["last_modified"])
            age = (datetime.now(timezone.utc) - last_modified).total_seconds()
        except Exception:
            age = float("inf")

        passed = age < _STAGE_MAX_AGE_SECONDS
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR,
            description=f"File age {age:.0f}s (limit {_STAGE_MAX_AGE_SECONDS}s).",
            metadata={
                "file": MetadataValue.text(files[0]["name"]),
                "age_seconds": MetadataValue.float(age),
            },
        )

    @asset_check(
        asset=landing_key,
        name="rowcount_and_max_id",
        description="Verify the landing table has rows and a valid numeric max ID.",
    )
    def landing_stats_check(snowflake: SnowflakeResource) -> AssetCheckResult:
        count, max_id = snowflake.get_landing_stats(table=table.upper())
        passed = count > 0 and max_id is not None
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR,
            description=f"{count} rows, max ID = {max_id}.",
            metadata={
                "row_count": MetadataValue.int(count),
                "max_id": MetadataValue.int(max_id or 0),
            },
        )

    return source_asset, stage_asset, landing_asset, [stage_file_check, landing_stats_check]


def build_all_assets(manifest: list[tuple[str, str]]) -> tuple[list, list]:
    """Return (assets, checks) for the full manifest."""
    assets: list = []
    checks: list = []
    for table, database in manifest:
        source, stage, landing, table_checks = build_assets_for_table(table, database)
        assets.extend([source, stage, landing])
        checks.extend(table_checks)
    return assets, checks
