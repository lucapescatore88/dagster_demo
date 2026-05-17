"""Three assets + two checks per manifest row:

    {table}__source  →  {table}__stage  →  {table}__landing
                              ↓                    ↓
                     stage_file_exists    rowcount_and_max_id

Load modes (configured per-table in config/tables.csv):

  full        — no partitions; CREATE OR REPLACE TABLE then COPY INTO each run.

  incremental — MonthlyPartitionsDefinition; one Dagster partition per calendar
                month.  Each partition:
                  • generates 50 rows seeded by (table, date) → idempotent
                  • adds a LOAD_DATE column so rows are attributable to a month
                  • stages to  @STAGE/{database}/{TABLE}/{date}/
                  • MERGEs into the landing table (clustered by LOAD_DATE)
                The Dagster UI shows a partition health grid and lets you
                backfill any historical month range.
"""

import random
import tempfile
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    MonthlyPartitionsDefinition,
    MetadataValue,
    Output,
    RetryPolicy,
    SourceAsset,
    asset,
    asset_check,
)

from pipeline_simple.manifest import TableConfig
from pipeline_simple.snowflake_resource import SnowflakeResource

# All incremental tables share the same partition definition so their assets
# are always in sync (same start date, same granularity).
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date="2025-05-01")

RETRY_POLICY = RetryPolicy(max_retries=2, delay=60)

_STAGE_MAX_AGE_SECONDS = 600  # stage file must be < 10 min old


# ── Data generators ─────────────────────────────────────────────────────────

def _random_rows(table: str, n: int = 50) -> list[dict]:
    """Unseeded — used for full-load tables (different data every run)."""
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


def _random_rows_for_date(table: str, date: str, n: int = 50) -> list[dict]:
    """Seeded by (table, date) — re-running the same partition gives the same
    rows, making incremental MERGEs idempotent."""
    rng = random.Random(f"{table}:{date}")
    return [
        {
            "id": rng.randint(1, 10**9),
            "load_date": date,
            "name": f"{table}_{i:04d}",
            "value": round(rng.uniform(1, 1000), 2),
            "status": rng.choice(["active", "pending", "closed"]),
        }
        for i in range(n)
    ]


# ── Asset factory ────────────────────────────────────────────────────────────

def build_assets_for_table(
    cfg: TableConfig,
) -> Tuple[SourceAsset, AssetsDefinition, AssetsDefinition, list]:
    """Return (source_asset, stage_asset, landing_asset, [checks])."""

    table     = cfg.table
    database  = cfg.database
    is_incremental = cfg.load_mode == "incremental"
    partitions = MONTHLY_PARTITIONS if is_incremental else None

    source_key  = AssetKey([f"{table}__source"])
    stage_key   = AssetKey([f"{table}__stage"])
    landing_key = AssetKey([f"{table}__landing"])

    source_asset = SourceAsset(
        key=source_key,
        group_name=database,
        tags={"asset_type": "source"},
        description=(
            f"External source for {table} ({database}). "
            f"No compute — lineage only."
        ),
    )

    # ── Stage asset ──────────────────────────────────────────────────────────

    @asset(
        name=f"{table}__stage",
        group_name=database,
        compute_kind="snowflake_stage",
        deps=[source_key],
        partitions_def=partitions,
        retry_policy=RETRY_POLICY,
        tags={
            "asset_type": "staging",
            "load_mode": cfg.load_mode,
        },
        description=(
            f"Stage parquet for {table} → "
            f"@STAGE/{database}/{table.upper()}"
            + ("/{date}/" if is_incremental else "/")
        ),
    )
    def stage_asset(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        if is_incremental:
            partition_date = context.partition_key          # "2025-05-16"
            rows = _random_rows_for_date(table, partition_date)
            df   = pd.DataFrame(rows)
        else:
            partition_date = ""
            rows = _random_rows(table)
            df   = pd.DataFrame(rows)

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
                date=partition_date,
            )
        finally:
            Path(local_path).unlink(missing_ok=True)

        context.log.info(f"Staged {len(df)} rows → {stage_path}")
        meta: dict = {
            "row_count": len(df),
            "stage_path": MetadataValue.text(stage_path),
            "preview": MetadataValue.md(df.head().to_markdown(index=False)),
        }
        if partition_date:
            meta["partition_date"] = MetadataValue.text(partition_date)

        return Output(
            value={"stage_path": stage_path, "columns": list(df.columns)},
            metadata=meta,
        )

    # ── Landing asset ────────────────────────────────────────────────────────

    @asset(
        name=f"{table}__landing",
        group_name=database,
        compute_kind="snowflake_table",
        ins={"staged": AssetIn(stage_key)},
        partitions_def=partitions,
        retry_policy=RETRY_POLICY,
        tags={
            "asset_type": "landing",
            "load_mode": cfg.load_mode,
        },
        description=(
            ("MERGE by ID into" if is_incremental else "Full-replace")
            + f" BRONZE.{table.upper()}"
            + (" (CLUSTER BY LOAD_DATE)" if is_incremental else "")
            + "."
        ),
    )
    def landing_asset(
        context: AssetExecutionContext,
        staged: dict,
        snowflake: SnowflakeResource,
    ) -> Output[int]:
        if is_incremental:
            partition_date = context.partition_key
            total_rows, rows_affected = snowflake.load_incremental(
                table=table.upper(),
                stage_path=staged["stage_path"],
                columns=staged["columns"],
            )
            context.log.info(
                f"[{partition_date}] merged {rows_affected} rows into "
                f"BRONZE.{table.upper()} (total: {total_rows})"
            )
            return Output(
                value=total_rows,
                metadata={
                    "total_rows": total_rows,
                    "rows_affected": rows_affected,
                    "partition_date": MetadataValue.text(partition_date),
                    "load_mode": MetadataValue.text("incremental"),
                    "source_stage": MetadataValue.text(staged["stage_path"]),
                },
            )
        else:
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
                    "load_mode": MetadataValue.text("full"),
                    "source_stage": MetadataValue.text(staged["stage_path"]),
                },
            )

    # ── Asset checks ─────────────────────────────────────────────────────────

    @asset_check(
        asset=stage_key,
        name="stage_file_exists",
        description="Verify a file was staged within the last 10 minutes.",
    )
    def stage_file_check(
        context: AssetCheckExecutionContext,
        snowflake: SnowflakeResource,
    ) -> AssetCheckResult:
        date  = context.partition_key if is_incremental else ""
        files = snowflake.list_stage_files(
            database=database, table=table.upper(), date=date
        )
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
        description=(
            "Verify the landing table has rows"
            + (" for this partition date" if is_incremental else "")
            + " and a valid max ID."
        ),
    )
    def landing_stats_check(
        context: AssetCheckExecutionContext,
        snowflake: SnowflakeResource,
    ) -> AssetCheckResult:
        load_date = context.partition_key if is_incremental else ""
        count, max_id = snowflake.get_landing_stats(
            table=table.upper(), load_date=load_date
        )
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


def build_all_assets(manifest: list[TableConfig]) -> tuple[list, list]:
    """Return (assets, checks) for the full manifest."""
    assets: list = []
    checks: list = []
    for cfg in manifest:
        source, stage, landing, table_checks = build_assets_for_table(cfg)
        assets.extend([source, stage, landing])
        checks.extend(table_checks)
    return assets, checks
