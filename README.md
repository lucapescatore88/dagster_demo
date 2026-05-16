# Pipeline Simple

A stripped-down Dagster demo: CSV-driven, random-data, full-load only,
Snowflake via internal stage.

```
config/tables.csv → random rows → /tmp parquet → PUT → COPY INTO (full replace)
```

## How it works

- One asset per row in `config/tables.csv`.
- Random rows generated in-process — no real source DB.
- Full replace each run: `CREATE OR REPLACE TABLE` then `COPY INTO`.
- Schedule `every_two_minutes` runs all assets every 2 min.
- Sensor `new_table_sensor` ticks every 30s, fires a one-shot run for any new
  table added to the CSV.

## Project layout

```
pipeline_simple/
├── config/tables.csv               ← one table name per row
├── pipeline_simple/
│   ├── definitions.py              ← top-level wiring
│   ├── assets.py                   ← asset factory + random data generator
│   ├── snowflake_resource.py       ← PUT + COPY INTO with full replace
│   ├── sensor.py                   ← detects new tables
│   └── manifest.py                 ← CSV reader
└── pyproject.toml
```

## Snowflake setup (one-time, ~2 minutes)

```sql
CREATE WAREHOUSE LOAD_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
CREATE DATABASE ANALYTICS;
CREATE ROLE PIPELINE_LOADER_ROLE;
GRANT USAGE ON WAREHOUSE LOAD_WH TO ROLE PIPELINE_LOADER_ROLE;
GRANT ALL ON DATABASE ANALYTICS TO ROLE PIPELINE_LOADER_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE PIPELINE_LOADER_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE ANALYTICS TO ROLE PIPELINE_LOADER_ROLE;
CREATE USER PIPELINE_LOADER PASSWORD = '...' DEFAULT_ROLE = PIPELINE_LOADER_ROLE;
GRANT ROLE PIPELINE_LOADER_ROLE TO USER PIPELINE_LOADER;
```

`BRONZE` schema and `RAW_STAGE` internal stage are auto-created on first run.

## Run it

```bash
py -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env       # fill in real values
dagster dev
```

In the UI (`localhost:3000`):
1. Materialize one asset manually to verify the Snowflake path works.
2. Turn on the `every_two_minutes` schedule.
3. Turn on `new_table_sensor`.
4. Add a row to `config/tables.csv` (e.g. `inventory`) and reload the code
   location (top right of the UI, or it auto-reloads in `dagster dev`). Within
   30 seconds the sensor should pick it up and trigger a one-shot run for the
   new table. The scheduled job picks it up on the next 2-minute tick.

## Why the sensor and schedule both exist

The schedule runs *all* currently-known tables every 2 minutes. The sensor
exists to give new tables an **immediate** first run — without it, you'd add
a row to the CSV and wait up to 2 minutes for the first load. The sensor's
cursor remembers which tables it has already fired for, so it doesn't
re-trigger them.
