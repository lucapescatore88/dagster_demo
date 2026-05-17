"""Snowflake resource — internal stage, full replace, and incremental MERGE."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import snowflake.connector
from dagster import ConfigurableResource


class SnowflakeResource(ConfigurableResource):
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    target_schema: str = "BRONZE"
    stage: str = "RAW_STAGE"

    @contextmanager
    def _conn(self):
        c = snowflake.connector.connect(
            account=self.account, user=self.user, password=self.password,
            role=self.role, warehouse=self.warehouse,
            database=self.database, schema=self.target_schema,
        )
        try:
            yield c
        finally:
            c.close()

    def execute(self, sql: str):
        with self._conn() as c:
            cur = c.cursor()
            try:
                cur.execute(sql)
                try:
                    return cur.fetchall()
                except snowflake.connector.errors.NotSupportedError:
                    return []
            finally:
                cur.close()

    def ensure_stage(self):
        """Idempotent: create schema + internal stage on demand."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.target_schema}")
        self.execute(
            f"CREATE STAGE IF NOT EXISTS "
            f"{self.database}.{self.target_schema}.{self.stage}"
        )

    def stage_file(self, table: str, local_parquet: str, database: str, date: str = "") -> str:
        """REMOVE existing files then PUT local parquet to the internal stage.

        For incremental tables pass date (YYYY-MM-DD) so each partition lands
        in its own subfolder: @STAGE/{database}/{table}/{date}/
        For full tables omit date:            @STAGE/{database}/{table}/
        """
        self.ensure_stage()
        local = Path(local_parquet).resolve()
        fq_stage = f"@{self.database}.{self.target_schema}.{self.stage}"
        stage_folder = f"{fq_stage}/{database}/{table}"
        if date:
            stage_folder = f"{stage_folder}/{date}"
        stage_path = f"{stage_folder}/{local.name}"

        self.execute(f"REMOVE {stage_folder}/")
        self.execute(
            f"PUT 'file://{local.as_posix()}' '{stage_folder}/' "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        return stage_path

    def list_stage_files(self, database: str, table: str, date: str = "") -> list[dict]:
        """List files under @STAGE/{database}/{table}/ or …/{date}/."""
        fq_stage = f"@{self.database}.{self.target_schema}.{self.stage}"
        path = f"{fq_stage}/{database}/{table}"
        if date:
            path = f"{path}/{date}"
        rows = self.execute(f"LIST {path}/")
        return [
            {"name": r[0], "size": r[1], "last_modified": r[3]}
            for r in (rows or [])
        ]

    def get_landing_stats(self, table: str, load_date: str = "") -> tuple[int, int | None]:
        """Return (row_count, max_id), optionally filtered to a single load_date."""
        fq_table = f"{self.database}.{self.target_schema}.{table}"
        where = f"WHERE LOAD_DATE = '{load_date}'" if load_date else ""
        rows = self.execute(
            f"SELECT COUNT(*), MAX(TRY_TO_NUMBER(ID)) FROM {fq_table} {where}"
        )
        if rows:
            return int(rows[0][0]), (int(rows[0][1]) if rows[0][1] is not None else None)
        return 0, None

    def load_from_stage(self, table: str, stage_path: str, columns: list[str]) -> int:
        """Full-replace: CREATE OR REPLACE TABLE then COPY INTO."""
        fq_table = f"{self.database}.{self.target_schema}.{table}"
        col_defs = ", ".join(f"{c.upper()} VARCHAR" for c in columns)

        self.execute(f"CREATE OR REPLACE TABLE {fq_table} ({col_defs})")
        self.execute(f"""
            COPY INTO {fq_table}
            FROM '{stage_path}'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        rows = self.execute(f"SELECT COUNT(*) FROM {fq_table}")
        return rows[0][0] if rows else 0

    def load_incremental(
        self,
        table: str,
        stage_path: str,
        columns: list[str],
        merge_key: str = "ID",
    ) -> tuple[int, int]:
        """Upsert staged parquet into the landing table via MERGE.

        Uses a session-scoped TEMP table so COPY INTO and MERGE share one
        Snowflake session (TEMP tables are invisible across connections).
        The landing table is clustered by LOAD_DATE for efficient date-range
        pruning across partitions.

        Returns (total_rows_in_table, rows_affected_by_merge).
        """
        fq_table = f"{self.database}.{self.target_schema}.{table}"
        fq_temp  = f"{self.database}.{self.target_schema}.{table}__INCR_TEMP"
        col_defs = ", ".join(f"{c.upper()} VARCHAR" for c in columns)

        non_key     = [c for c in columns if c.upper() != merge_key.upper()]
        update_set  = ", ".join(f"t.{c.upper()} = s.{c.upper()}" for c in non_key)
        insert_cols = ", ".join(c.upper() for c in columns)
        insert_vals = ", ".join(f"s.{c.upper()}" for c in columns)

        with self._conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"CREATE OR REPLACE TEMP TABLE {fq_temp} ({col_defs})")
                cur.execute(f"""
                    COPY INTO {fq_temp}
                    FROM '{stage_path}'
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'ABORT_STATEMENT'
                """)
                # Create target table on first run, with clustering on LOAD_DATE.
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {fq_table} ({col_defs}) "
                    f"CLUSTER BY (LOAD_DATE)"
                )
                # Schema migration: add LOAD_DATE if the table existed before
                # this column was introduced (no-op when column already exists).
                cur.execute(
                    f"ALTER TABLE {fq_table} "
                    f"ADD COLUMN IF NOT EXISTS LOAD_DATE VARCHAR"
                )
                cur.execute(f"""
                    MERGE INTO {fq_table} t
                    USING {fq_temp} s
                    ON t.{merge_key.upper()} = s.{merge_key.upper()}
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """)
                merge_row    = cur.fetchone() or (0, 0)
                rows_affected = int(merge_row[0]) + int(merge_row[1])

                cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
                total_rows = int((cur.fetchone() or (0,))[0])
            finally:
                cur.close()

        return total_rows, rows_affected
