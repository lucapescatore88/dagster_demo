"""Dagster assets generated from the dbt project.

The custom translator maps every dbt source in the `bronze` source block to the
corresponding `{table}__landing` asset already defined in assets.py, so Dagster
draws the full lineage:

    {table}__landing  →  stg_{table}  →  orders_summary
"""
import os
from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# `@dbt_assets` reads manifest.json at import time, so we must generate it
# before the decorator runs. `dbt parse` is fast — no DB connection required.
DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
).cli(["parse"]).wait()


class _LandingSourceTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([f"{dbt_resource_props['name']}__landing"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: dict) -> str:
        fqn = dbt_resource_props.get("fqn", [])
        # fqn = [project, folder, model_name] — use folder as group
        return fqn[1] if len(fqn) >= 2 else "dbt"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=_LandingSourceTranslator(),
)
def dbt_project_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
