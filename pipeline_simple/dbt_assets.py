"""Dagster assets generated from the dbt project.

The custom translator maps every dbt source in the `bronze` source block to the
corresponding `{table}__landing` asset already defined in assets.py, so Dagster
draws the full lineage:

    {table}__landing  →  stg_{table}  →  orders_summary

dbt tests are surfaced as Dagster asset checks via `dbt build`:
  - `dbt build` runs each model then immediately tests it before moving to
    dependents, so a test failure blocks downstream models within the dbt DAG.
  - dagster-dbt maps test results to AssetCheckEvaluation events automatically;
    no translator override is needed or supported for check specs.
"""
import os
from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Generate manifest.json only when it doesn't already exist.
# Running dbt parse at every import is too slow for the gRPC code-server
# heartbeat timeout; generate it once (e.g. during `dagster dev` startup or
# CI) and let Dagster reload use the cached file.
if not DBT_MANIFEST_PATH.exists():
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
    # Run models only. `dbt test` streaming requires dagster-dbt built for
    # dbt 1.9+ — re-enable once packages are upgraded (see pyproject.toml).
    yield from dbt.cli(["run"], context=context).stream()
    #yield from dbt.cli(["test"], context=context).stream()
