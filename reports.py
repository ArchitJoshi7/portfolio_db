"""
Report generation: console tables and CSV export.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, List

from tabulate import tabulate


def print_table(title: str, rows: List[dict]) -> None:
    if not rows:
        print(f"{title}: No data.")
        return
    headers = list(rows[0].keys())
    print(f"\n=== {title} ===")
    print(tabulate([list(r.values()) for r in rows], headers=headers, tablefmt="github", floatfmt=".2f"))


def export_csv(path: str | Path, rows: List[dict]) -> Path:
    path = Path(path)
    if not rows:
        # create empty with headers unknown
        path.write_text("")
        return path
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return path

