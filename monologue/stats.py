"""Summarize the dataset: rows, files, date ranges, and top authors per source."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from monologue.common import SOURCES, iter_csv_files, iter_rows


def compute(data_dir: Path) -> dict:
    files = Counter(source for source, _ in iter_csv_files(data_dir))
    rows: Counter[str] = Counter()
    authors: dict[str, Counter[str]] = defaultdict(Counter)
    dates: dict[str, list[str]] = {}
    for row in iter_rows(data_dir):
        rows[row.source] += 1
        authors[row.source][row.author] += 1
        lo, hi = dates.get(row.source, (row.date, row.date))
        dates[row.source] = [min(lo, row.date), max(hi, row.date)]

    overall_authors: Counter[str] = Counter()
    for counter in authors.values():
        overall_authors.update(counter)

    return {
        "sources": {
            source: {
                "files": files[source],
                "rows": rows[source],
                "first_date": dates[source][0],
                "last_date": dates[source][1],
                "authors": authors[source].most_common(),
            }
            for source in SOURCES
            if source in dates
        },
        "total_files": sum(files.values()),
        "total_rows": sum(rows.values()),
        "top_authors": overall_authors.most_common(),
    }


def render_markdown(stats: dict, top_n: int = 12) -> str:
    lines = ["| Source | Days | Rows | From | To |", "|---|---:|---:|---|---|"]
    for name, s in stats["sources"].items():
        lines.append(f"| {name} | {s['files']:,} | {s['rows']:,} | {s['first_date']} | {s['last_date']} |")
    lines.append(f"| **Total** | **{stats['total_files']:,}** | **{stats['total_rows']:,}** | | |")
    lines += ["", "| Host | Rows |", "|---|---:|"]
    for author, count in stats["top_authors"][:top_n]:
        lines.append(f"| {author} | {count:,} |")
    return "\n".join(lines)


def add_arguments(parser) -> None:
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Markdown tables.")
    parser.add_argument("--top", type=int, default=12, help="How many hosts to list.")


def run(args) -> int:
    stats = compute(Path(args.data_dir))
    if args.json:
        print(json.dumps(stats, indent=2, ensure_ascii=False))
    else:
        print(render_markdown(stats, args.top))
    return 0
