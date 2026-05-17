"""Read the manifest. Each row = one table to load."""
import csv
from pathlib import Path
from typing import NamedTuple


class TableConfig(NamedTuple):
    table: str
    database: str
    load_mode: str  # "full" | "incremental"


def load_table_manifest(path: str | Path) -> list[TableConfig]:
    """Return a TableConfig for every non-empty row in the manifest CSV."""
    with open(path, newline="", encoding="utf-8") as fh:
        return [
            TableConfig(
                table=r["table_name"].strip(),
                database=r["database"].strip(),
                load_mode=(r.get("load_mode") or "full").strip() or "full",
            )
            for r in csv.DictReader(fh)
            if r.get("table_name")
        ]


def load_table_names(path: str | Path) -> list[str]:
    return [cfg.table for cfg in load_table_manifest(path)]
