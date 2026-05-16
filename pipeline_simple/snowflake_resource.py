"""Minimal Snowflake resource — internal stage + full replace only."""
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
    # Renamed from 'schema' because that shadows a Pydantic internal on
    # ConfigurableResource. Functionally identical.
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

    def load_full_replace(self, table: str, local_parquet: str, columns: list[str]) -> int:
        """PUT local parquet -> internal stage -> COPY INTO with full replace.

        Returns row count after load.
        """
        self.ensure_stage()
        local = Path(local_parquet).resolve()
        fq_table = f"{self.database}.{self.target_schema}.{table}"
        fq_stage = f"@{self.database}.{self.target_schema}.{self.stage}"

        # 1. PUT the local parquet onto the internal stage.
        self.execute(
            f"PUT 'file://{local.as_posix()}' '{fq_stage}/{table}/' "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )

        # 2. CREATE OR REPLACE the target table with VARCHAR columns.
        #    Full-replace semantics: every run rebuilds the table from scratch.
        col_defs = ", ".join(f'{c.upper()} VARCHAR' for c in columns)
        self.execute(f"CREATE OR REPLACE TABLE {fq_table} ({col_defs})")

        # 3. COPY INTO from the staged file.
        self.execute(f"""
            COPY INTO {fq_table}
            FROM '{fq_stage}/{table}/{local.name}'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        rows = self.execute(f"SELECT COUNT(*) FROM {fq_table}")
        return rows[0][0] if rows else 0
    
    def stage_file(self, table: str, local_parquet: str) -> str:
        """PUT local parquet onto the internal stage. Returns the stage path."""
        self.ensure_stage()
        local = Path(local_parquet).resolve()
        fq_stage = f"@{self.database}.{self.target_schema}.{self.stage}"
        stage_path = f"{fq_stage}/{table}/{local.name}"

        self.execute(
            f"PUT 'file://{local.as_posix()}' '{fq_stage}/{table}/' "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        return stage_path

    def load_from_stage(self, table: str, stage_path: str, columns: list[str]) -> int:
        """CREATE OR REPLACE TABLE and COPY INTO from the staged file."""
        fq_table = f"{self.database}.{self.target_schema}.{table}"

        col_defs = ", ".join(f'{c.upper()} VARCHAR' for c in columns)
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
