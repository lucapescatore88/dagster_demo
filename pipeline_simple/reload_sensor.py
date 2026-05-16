"""Reload the code location when config/tables.csv changes.

Uses the GraphQL client to hit the running Dagster webserver's reload
endpoint. Once reload completes, the new asset definitions exist in the
graph, and new_table_sensor can safely fire for them.
"""
import os
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)


def make_reload_sensor(manifest_path: Path, code_location_name: str = "pipeline_simple"):
    @sensor(
        name="manifest_reload_sensor",
        minimum_interval_seconds=15,
        default_status=DefaultSensorStatus.STOPPED,
        description="Reloads the code location when tables.csv changes.",
    )
    def _sensor(context: SensorEvaluationContext) -> SensorResult:
        mtime = str(manifest_path.stat().st_mtime)
        if context.cursor == mtime:
            return SensorResult(skip_reason=SkipReason("No change to tables.csv."),
                                cursor=mtime)

        # Trigger a reload via the local webserver's GraphQL endpoint.
        try:
            from dagster_graphql import DagsterGraphQLClient
            host = os.getenv("DAGSTER_WEBSERVER_HOST", "localhost")
            port = int(os.getenv("DAGSTER_WEBSERVER_PORT", "3000"))
            client = DagsterGraphQLClient(host, port_number=port)
            info = client.reload_repository_location(code_location_name)
            context.log.info(f"Reload status: {info.status}")
        except Exception as e:
            # Don't crash the sensor — log and try again next tick.
            context.log.warning(f"Reload failed: {e}")
            return SensorResult(
                skip_reason=SkipReason(f"Reload attempt failed: {e}"),
                cursor=context.cursor,
            )

        return SensorResult(
            skip_reason=SkipReason(f"Reloaded code location after manifest change."),
            cursor=mtime,
        )

    return _sensor
