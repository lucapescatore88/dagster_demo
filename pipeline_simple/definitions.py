"""Top-level Dagster definitions.

- Asset per table from config/tables.csv
- Schedule: every 2 minutes, runs all assets currently in the manifest
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

from pipeline_simple.assets import build_all_assets
from pipeline_simple.manifest import load_table_names
from pipeline_simple.sensor import make_new_table_sensor
from pipeline_simple.snowflake_resource import SnowflakeResource
from pipeline_simple.reload_sensor import make_reload_sensor

MANIFEST_PATH = Path(__file__).parent.parent / "config" / "tables.csv"

tables = load_table_names(MANIFEST_PATH)
all_assets = build_all_assets(tables)

# One job covering every asset currently in the manifest. The job's asset
# selection is computed at definitions-load time, so when you add a row to
# the CSV and reload the code location, the next scheduled tick includes it
# automatically.
load_job = define_asset_job(
    name="load_all_tables",
    selection=AssetSelection.all(),
    description="Full-replace every table in the manifest.",
)

every_two_minutes = ScheduleDefinition(
    name="every_two_minutes",
    job=load_job,
    cron_schedule="*/2 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Re-load every table every 2 minutes.",
)

new_table_sensor = make_new_table_sensor(MANIFEST_PATH, load_job)
reload_sensor = make_reload_sensor(MANIFEST_PATH)

defs = Definitions(
    assets=all_assets,
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
    },
)
