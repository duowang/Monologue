"""Build the public sample: first and last day per source (capped) plus a text-free coverage file.

The full dataset lives in a private repository. This command regenerates the small,
deterministic slice that is committed publicly to document the format and the date range.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from monologue.common import SOURCES, iter_csv_files, iter_rows, read_day_csv, write_day_csv

DEFAULT_OUT_DIR = Path("sample")
DEFAULT_ROWS_PER_FILE = 10
COVERAGE_FILE = "coverage.json"


def build_coverage(data_dir: Path) -> dict:
    """Rows per source per month, plus totals. Contains no joke text."""
    per_month: dict[str, Counter[str]] = defaultdict(Counter)
    totals: dict[str, dict] = {}
    for row in iter_rows(data_dir):
        month = row.date[:7]
        per_month[row.source][month] += 1
        entry = totals.setdefault(row.source, {"rows": 0, "first_date": row.date, "last_date": row.date})
        entry["rows"] += 1
        entry["first_date"] = min(entry["first_date"], row.date)
        entry["last_date"] = max(entry["last_date"], row.date)
    days = Counter(source for source, _ in iter_csv_files(data_dir))
    return {
        "sources": {
            source: {
                **totals[source],
                "days": days[source],
                "rows_by_month": dict(sorted(per_month[source].items())),
            }
            for source in SOURCES
            if source in totals
        },
        "total_rows": sum(t["rows"] for t in totals.values()),
        "total_days": sum(days.values()),
    }


def build_sample(data_dir: Path, out_dir: Path, rows_per_file: int = DEFAULT_ROWS_PER_FILE) -> list[Path]:
    """Write the earliest and latest day of each source, keeping at most rows_per_file rows."""
    written: list[Path] = []
    for source in SOURCES:
        files = [path for src, path in iter_csv_files(data_dir) if src == source]
        source_out = out_dir / source
        if source_out.is_dir():
            for stale in source_out.glob("*.csv"):
                stale.unlink()
        if not files:
            continue
        for path in dict.fromkeys((files[0], files[-1])):
            rows = read_day_csv(path)[:rows_per_file]
            by_author: dict[str, list[str]] = defaultdict(list)
            for row in rows:
                by_author[row["name"]].append(row["monologue"])
            written.append(write_day_csv(source_out, path.stem, by_author))

    out_dir.mkdir(parents=True, exist_ok=True)
    coverage_path = out_dir / COVERAGE_FILE
    coverage_path.write_text(json.dumps(build_coverage(data_dir), indent=2) + "\n", encoding="utf-8")
    written.append(coverage_path)
    return written


def add_arguments(parser) -> None:
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Where to write the sample.")
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=DEFAULT_ROWS_PER_FILE,
        help=f"Maximum rows kept per sample day file (default {DEFAULT_ROWS_PER_FILE}).",
    )


def run(args) -> int:
    written = build_sample(Path(args.data_dir), Path(args.out_dir), args.rows_per_file)
    for path in written:
        print(f"[written] {path}")
    return 0
