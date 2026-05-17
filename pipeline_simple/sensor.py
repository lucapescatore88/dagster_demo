"""Watch the manifest CSV. When a new table appears, run it once."""
import json
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from pipeline_simple.assets import build_assets_for_table
from pipeline_simple.manifest import load_table_manifest


def make_new_table_sensor(manifest_path: Path, job):
    @sensor(
        name="new_table_sensor",
        job=job,
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.STOPPED,
        description="Tick every 30s. New tables in the CSV get one immediate run.",
    )
    def _sensor(context: SensorEvaluationContext) -> SensorResult:
        current = {cfg.table: cfg for cfg in load_table_manifest(manifest_path)}
        seen = set(json.loads(context.cursor)) if context.cursor else set()
        new = set(current) - seen

        if not new:
            return SensorResult(
                skip_reason=SkipReason(f"No new tables. Tracking {len(seen)}."),
                cursor=context.cursor,
            )

        run_requests = []
        for t in sorted(new):
            _source_a, stage_a, landing_a, _checks = build_assets_for_table(current[t])
            run_requests.append(
                RunRequest(
                    run_key=f"new-table::{t}",
                    asset_selection=[stage_a.key, landing_a.key],
                    tags={"trigger": "new_table_sensor", "table": t},
                )
            )

        context.log.info(f"Detected {len(new)} new table(s): {sorted(new)}")
        return SensorResult(
            run_requests=run_requests,
            cursor=json.dumps(sorted(seen | new)),
        )

    return _sensor