"""Shared helpers: text normalization, author names, CSV I/O, and HTTP retries."""

from __future__ import annotations

import csv
import re
import time
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import requests

CSV_FIELDS = ("name", "monologue")
SOURCES = ("newsmax", "latenighter", "scraps")
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


def get_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, object] | None = None,
    timeout: float = 30,
    retries: int = 3,
    backoff: float = 0.8,
) -> requests.Response | None:
    """GET with simple retries. Returns None on 404; raises the last error otherwise."""
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
            if response.status_code == 404:
                return None
            if response.status_code == 429:
                time.sleep(backoff * (attempt + 1))
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(backoff)
    if last_error is None:
        raise RuntimeError(f"Gave up after {retries} attempts: {url}")
    raise last_error


def fetch_wp_posts(
    session: requests.Session, api_url: str, tag_id: int, *, per_page: int = 100
) -> Iterator[dict]:
    """Page through a WordPress REST API posts endpoint filtered by tag."""
    page = 1
    while True:
        params = {
            "tags": tag_id,
            "per_page": per_page,
            "page": page,
            "_fields": "id,date,link,title,content",
        }
        response = get_with_retry(session, api_url, params=params, timeout=35, retries=4)
        if response is None:
            break
        posts = response.json()
        if not posts:
            break
        yield from posts
        total_pages = int(response.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            break
        page += 1


def add_date_window_args(parser, default_from: str) -> None:
    parser.add_argument("--from-date", default=default_from, help="Earliest date to keep (YYYY-MM-DD).")
    parser.add_argument("--to-date", default=None, help="Latest date to keep (default: today).")


def add_overwrite_args(parser) -> None:
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Rewrite day files that already exist (default: skip them).",
    )


def resolve_window(args) -> tuple[date, date]:
    from_date = parse_iso_date(args.from_date)
    to_date = parse_iso_date(args.to_date) if args.to_date else today_utc()
    return from_date, to_date
