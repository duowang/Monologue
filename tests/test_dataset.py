"""Integrity checks over committed CSV files: the public sample always, the full data when present."""

import csv
from datetime import datetime
from pathlib import Path

import pytest

from monologue.common import CSV_FIELDS, SOURCES, iter_csv_files
from monologue.sample import DEFAULT_ROWS_PER_FILE

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = ROOT / "sample"
DATA_DIR = ROOT / "data"


def check_files(root: Path) -> None:
    problems = []
    seen = set()
    for source, path in iter_csv_files(root):
        seen.add(source)
        try:
            datetime.strptime(path.stem, "%Y-%m-%d")
        except ValueError:
            problems.append(f"{path}: filename is not a date")
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            rows = [r for r in reader if any(cell.strip() for cell in r)]
        if header != list(CSV_FIELDS):
            problems.append(f"{path}: header {header!r}")
        if not rows:
            problems.append(f"{path}: no rows")
    assert not problems, "\n".join(problems[:20])
    assert seen == set(SOURCES)


def test_public_sample_is_valid_and_small():
    check_files(SAMPLE_DIR)
    for _source, path in iter_csv_files(SAMPLE_DIR):
        with path.open(encoding="utf-8", newline="") as fh:
            assert len(list(csv.DictReader(fh))) <= DEFAULT_ROWS_PER_FILE, f"{path} exceeds sample cap"
    assert (SAMPLE_DIR / "coverage.json").is_file()


@pytest.mark.skipif(not DATA_DIR.is_dir(), reason="full dataset not present")
def test_full_dataset_is_valid():
    check_files(DATA_DIR)
