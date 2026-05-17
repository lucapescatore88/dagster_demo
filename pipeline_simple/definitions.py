"""Top-level Dagster definitions.

- Asset per table from config/tables.csv
- dbt assets (staging views + marts) that depend on the landing assets
- Schedule: every 2 minutes, runs all assets in dependency order
- Sensor: detects new tables in the CSV and triggers a one-shot run for them
"""
import os
from pathlib import Path

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from pipeline_simple.assets import build_all_assets
from pipeline_simple.dbt_assets import DBT_PROJECT_DIR, dbt_project_assets
from pipeline_simple.manifest import load_table_manifest, load_table_names  # noqa: F401
from pipeline_simple.sensor import make_new_table_sensor
from pipeline_simple.snowflake_resource import SnowflakeResource
from pipeline_simple.reload_sensor import make_reload_sensor

MANIFEST_PATH = Path(__file__).parent.parent / "config" / "tables.csv"

manifest = load_table_manifest(MANIFEST_PATH)
all_assets, all_checks = build_all_assets(manifest)

# Ensure the dbt manifest is up-to-date every time the code location loads.
# `dbt parse` is fast (no DB connection needed) and keeps the asset graph in sync.
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

# One job covering all assets in dependency order:
#   source (skipped) → stage → landing → dbt staging → dbt marts
load_job = define_asset_job(
    name="load_all_tables",
    selection=AssetSelection.all(),
    description="Full-replace every table in the manifest, then run dbt.",
)

every_two_minutes = ScheduleDefinition(
    name="every_two_minutes",
    job=load_job,
    cron_schedule="*/2 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Re-load every table and run dbt every 2 minutes.",
)

new_table_sensor = make_new_table_sensor(MANIFEST_PATH, load_job)
reload_sensor = make_reload_sensor(MANIFEST_PATH)

defs = Definitions(
    assets=[*all_assets, dbt_project_assets],
    asset_checks=all_checks,
    jobs=[load_job],
    schedules=[every_two_minutes],
    sensors=[new_table_sensor, reload_sensor],
    resources={
        "snowflake": SnowflakeResource(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            role=EnvVar("SNOWFLAKE_ROLE"),
            warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=EnvVar("SNOWFLAKE_DATABASE"),
            target_schema=os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
            stage=os.getenv("SNOWFLAKE_STAGE", "RAW_STAGE"),
        ),
        "dbt": dbt_resource,
    },
)
