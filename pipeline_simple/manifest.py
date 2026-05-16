"""Read the manifest. Each row = one table to load."""
import csv
from pathlib import Path


def load_table_manifest(path: str | Path) -> list[tuple[str, str]]:
    """Return [(table_name, database), ...] for every non-empty row."""
    with open(path, newline="", encoding="utf-8") as fh:
        return [
            (r["table_name"].strip(), r["database"].strip())
            for r in csv.DictReader(fh)
            if r.get("table_name")
        ]


def load_table_names(path: str | Path) -> list[str]:
    return [t for t, _ in load_table_manifest(path)]
