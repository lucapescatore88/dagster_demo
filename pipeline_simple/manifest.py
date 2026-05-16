"""Read the manifest. Each row = one table to load."""
import csv
from pathlib import Path


def load_table_names(path: str | Path) -> list[str]:
    with open(path, newline="", encoding="utf-8") as fh:
        return [r["table_name"].strip() for r in csv.DictReader(fh) if r.get("table_name")]
