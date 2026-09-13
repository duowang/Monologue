"""Integrity checks over the committed data files."""

import csv
from datetime import datetime
from pathlib import Path

import pytest

from monologue.common import CSV_FIELDS, SOURCES, iter_csv_files

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

pytestmark = pytest.mark.skipif(not DATA_DIR.is_dir(), reason="data directory not present")


def test_every_file_is_named_by_date_and_has_the_standard_header():
    problems = []
    for _source, path in iter_csv_files(DATA_DIR):
        try:
            datetime.strptime(path.stem, "%Y-%m-%d")
        except ValueError:
            problems.append(f"{path}: filename is not a date")
        with path.open(encoding="utf-8", newline="") as fh:
            header = next(csv.reader(fh), None)
        if header != list(CSV_FIELDS):
            problems.append(f"{path}: header {header!r}")
    assert not problems, "\n".join(problems[:20])


def test_no_empty_files_and_every_source_present():
    seen = set()
    for source, path in iter_csv_files(DATA_DIR):
        seen.add(source)
        with path.open(encoding="utf-8", newline="") as fh:
            rows = [r for r in csv.DictReader(fh) if r.get("monologue", "").strip()]
        assert rows, f"{path} has no rows"
    assert seen == set(SOURCES)
