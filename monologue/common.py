"""Shared helpers: text normalization, author names, dates, and CSV I/O."""

from __future__ import annotations

import csv
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

CSV_FIELDS = ("name", "monologue")
SOURCES = ("newsmax", "latenighter", "scraps", "youtube")
DATA_DIR_ENV = "MONOLOGUE_DATA_DIR"
DEFAULT_DATA_DIR = Path("data")

# Lower-cased spellings that appear in source pages, mapped to one canonical name.
CANONICAL_AUTHORS: dict[str, str] = {
    "conan": "Conan O'Brien",
    "conan o'brian": "Conan O'Brien",
    "conan obrien": "Conan O'Brien",
    "conan o'brien": "Conan O'Brien",
    "jay": "Jay Leno",
    "jay leno": "Jay Leno",
    "letterman": "David Letterman",
    "david letterman": "David Letterman",
    "kimmel": "Jimmy Kimmel",
    "jimmy kimmel": "Jimmy Kimmel",
    "fallon": "Jimmy Fallon",
    "jimmy fallon": "Jimmy Fallon",
    "corden": "James Corden",
    "james corden": "James Corden",
    "colbert": "Stephen Colbert",
    "stephen colbert": "Stephen Colbert",
    "ferguson": "Craig Ferguson",
    "craig ferguson": "Craig Ferguson",
    "meyers": "Seth Meyers",
    "seth meyers": "Seth Meyers",
    "john oliver": "John Oliver",
    "jon stewart": "Jon Stewart",
    "desi lydic": "Desi Lydic",
    "jordan klepper": "Jordan Klepper",
    "ronny chieng": "Ronny Chieng",
    "michael kosta": "Michael Kosta",
    "taylor tomlinson": "Taylor Tomlinson",
    "trevor noah": "Trevor Noah",
}


def default_data_dir() -> Path:
    """The data directory: $MONOLOGUE_DATA_DIR if set, else ./data."""
    return Path(os.environ.get(DATA_DIR_ENV) or DEFAULT_DATA_DIR)


def normalize_text(value: str | None) -> str:
    """Collapse all whitespace runs to single spaces and trim."""
    return re.sub(r"\s+", " ", value or "").strip()


def canonical_author(name: str | None) -> str:
    """Return the canonical spelling of a host name, or the cleaned input if unknown."""
    cleaned = normalize_text(name)
    return CANONICAL_AUTHORS.get(cleaned.lower(), cleaned)


def iso_date(value: str) -> str:
    """Convert a WordPress ISO timestamp (e.g. '2024-02-27T10:00:00') to YYYY-MM-DD."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d")


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


def date_from_filename(path: Path) -> date | None:
    try:
        return parse_iso_date(path.stem)
    except ValueError:
        return None


@dataclass(frozen=True)
class Row:
    source: str
    date: str
    author: str
    text: str


def write_day_csv(output_dir: Path, date_value: str, by_author: Mapping[str, Iterable[str]]) -> Path:
    """Write one day's jokes, grouped by author, to <output_dir>/<date>.csv."""
    path = Path(output_dir) / f"{date_value}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_FIELDS))
        writer.writeheader()
        for author, quotes in by_author.items():
            for quote in quotes:
                writer.writerow({"name": author, "monologue": quote})
    return path


def iter_csv_files(data_dir: Path, sources: Iterable[str] = SOURCES) -> Iterator[tuple[str, Path]]:
    """Yield (source, path) for every day CSV under data_dir, sorted by source then date."""
    for source in sources:
        root = Path(data_dir) / source
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.csv"), key=lambda p: p.stem):
            yield source, path


def read_day_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def iter_rows(data_dir: Path, sources: Iterable[str] = SOURCES) -> Iterator[Row]:
    """Yield every non-empty row in the dataset with author names canonicalized."""
    for source, path in iter_csv_files(data_dir, sources):
        date_value = path.stem
        for raw in read_day_csv(path):
            author = canonical_author(raw.get("name"))
            text = normalize_text(raw.get("monologue"))
            if author and text:
                yield Row(source=source, date=date_value, author=author, text=text)
