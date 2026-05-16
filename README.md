# Pipeline Simple

A Dagster demo: CSV-driven asset pipeline loading random data into Snowflake
via internal stage, with dbt transformations and persistent run history in
PostgreSQL.

```
source (lineage) → stage (parquet → Snowflake stage) → landing (COPY INTO) → dbt models
```

## How it works

- Three assets per row in `config/tables.csv`: source (lineage only), stage, landing.
- Assets are grouped by source database (`crm_db`, `erp_db`, `hr_db`) and tagged by type.
- Random rows generated in-process — no real source DB needed.
- Full replace each run: `CREATE OR REPLACE TABLE` then `COPY INTO`.
- dbt staging views and a mart model run automatically after all landing assets complete.
- Asset checks verify stage file freshness and landing row count / max ID after each run.
- Schedule `every_two_minutes` runs all assets every 2 min.
- Sensor `new_table_sensor` ticks every 30s, fires a one-shot run for any new
  table added to the CSV.
- Run history is persisted in PostgreSQL so it survives restarts.

## Project layout

```
dagster_demo/
├── config/tables.csv               ← table name + source database per row
├── dagster_home/
│   └── dagster.yaml                ← instance config (PostgreSQL storage)
├── dbt/                            ← dbt project (staging views + mart)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                ← views over BRONZE tables
│       └── marts/                  ← joined / aggregated tables
├── pipeline_simple/
│   ├── definitions.py              ← top-level wiring
│   ├── assets.py                   ← asset + check factory
│   ├── dbt_assets.py               ← dagster-dbt integration
│   ├── snowflake_resource.py       ← PUT + COPY INTO + stage helpers
│   ├── sensor.py                   ← detects new tables in CSV
│   ├── reload_sensor.py            ← reloads code location on CSV change
│   └── manifest.py                 ← CSV reader
└── pyproject.toml
```

## 1. PostgreSQL setup (persistent run history)

Dagster stores run history, event logs, and schedules in PostgreSQL.
Without this, all history is lost on restart.

### Install and start PostgreSQL (WSL / Ubuntu)

```bash
sudo apt install postgresql -y
sudo service postgresql start
```

### Create the Dagster database and user

```bash
sudo -u postgres psql <<'SQL'
  CREATE USER dagster WITH PASSWORD 'dagster';
  CREATE DATABASE dagster OWNER dagster;
SQL
```

### Configure the connection

`dagster.yaml` lives at the project root alongside `pyproject.toml` and reads
credentials from the single `.env` file in the same directory. Edit `.env` and
fill in the Postgres section (the file already contains the Snowflake vars):

```
DAGSTER_PG_HOST=localhost
DAGSTER_PG_USER=dagster
DAGSTER_PG_PASSWORD=dagster
DAGSTER_PG_DB=dagster
DAGSTER_PG_PORT=5432
```

`.env` is gitignored — credentials are never committed.

## 2. Snowflake setup (one-time, ~2 minutes)

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
dbt writes staging views to `SILVER` and mart tables to `GOLD`.

## 3. Run it

```bash
# Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -e .

# Point DAGSTER_HOME at the project root (where dagster.yaml lives)
# and load all credentials from the single .env
export DAGSTER_HOME=$(pwd)
set -a && source .env && set +a

# Start
dagster dev
```

To avoid repeating the exports, add them to `~/.bashrc`:

```bash
export DAGSTER_HOME=/home/<you>/Code/dagster_demo
set -a && source $DAGSTER_HOME/.env && set +a
```

In the UI (`localhost:3000`):
1. Materialize one asset manually to verify the Snowflake path works.
2. Turn on the `every_two_minutes` schedule.
3. Turn on `new_table_sensor`.
4. Add a row to `config/tables.csv` (e.g. `inventory,erp_db`) and reload the
   code location (top right of the UI, or it auto-reloads in `dagster dev`).
   Within 30 seconds the sensor picks it up and fires a one-shot run.

## Why the sensor and schedule both exist

The schedule runs *all* currently-known tables every 2 minutes. The sensor
exists to give new tables an **immediate** first run — without it, you'd add
a row to the CSV and wait up to 2 minutes for the first load. The sensor's
cursor remembers which tables it has already fired for, so it doesn't
re-trigger them.
