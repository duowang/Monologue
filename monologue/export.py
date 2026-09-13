"""Flatten every day CSV into a single TSV or JSON Lines file."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from monologue.common import SOURCES, iter_rows

FORMATS = ("tsv", "jsonl")


def add_arguments(parser) -> None:
    parser.add_argument("--format", choices=FORMATS, default="tsv")
    parser.add_argument("--output", "-o", default="-", help="Output path, or '-' for stdout (default).")
    parser.add_argument(
        "--source", action="append", choices=SOURCES, help="Restrict to a source (repeatable)."
    )


def export(data_dir: Path, fmt: str, out, sources=SOURCES) -> int:
    count = 0
    if fmt == "tsv":
        out.write("source\tdate\tname\tmonologue\n")
    for row in iter_rows(data_dir, sources):
        if fmt == "tsv":
            text = row.text.replace("\t", " ")
            out.write(f"{row.source}\t{row.date}\t{row.author}\t{text}\n")
        else:
            record = {"source": row.source, "date": row.date, "name": row.author, "monologue": row.text}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
        count += 1
    return count


def run(args) -> int:
    sources = tuple(args.source) if args.source else SOURCES
    if args.output == "-":
        count = export(Path(args.data_dir), args.format, sys.stdout, sources)
    else:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fh:
            count = export(Path(args.data_dir), args.format, fh, sources)
        print(f"Wrote {count} rows to {path}", file=sys.stderr)
    return 0
