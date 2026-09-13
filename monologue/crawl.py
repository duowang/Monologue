"""Building blocks shared by the crawlers: date windows, the day-file store, and run summaries."""

from __future__ import annotations

import argparse
import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from monologue.common import date_from_filename, parse_iso_date, today_utc, write_day_csv
from monologue.http import DEFAULT_USER_AGENT

log = logging.getLogger(__name__)

QuotesByAuthor = dict[str, list[str]]


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    def contains(self, value: date | str) -> bool:
        if isinstance(value, str):
            value = parse_iso_date(value)
        return self.start <= value <= self.end

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> DateWindow:
        start = parse_iso_date(args.from_date)
        end = parse_iso_date(args.to_date) if args.to_date else today_utc()
        if end < start:
            raise SystemExit(f"--to-date {end} is before --from-date {start}")
        return cls(start, end)


class DayStore:
    """One CSV per day under a source directory."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def path(self, date_value: str) -> Path:
        return self.root / f"{date_value}.csv"

    def exists(self, date_value: str) -> bool:
        return self.path(date_value).exists()

    def save(
        self, date_value: str, by_author: Mapping[str, Iterable[str]], *, overwrite: bool = False
    ) -> str:
        """Write a day file unless it already exists. Returns 'saved' or 'skipped'."""
        if not overwrite and self.exists(date_value):
            log.info("[skipped] date=%s file=%s", date_value, self.path(date_value))
            return "skipped"
        path = write_day_csv(self.root, date_value, by_author)
        total = sum(len(list(v)) for v in by_author.values())
        log.info("[saved] date=%s authors=%d rows=%d file=%s", date_value, len(by_author), total, path)
        return "saved"

    def prune(self, keep: Iterable[str], window: DateWindow) -> list[Path]:
        """Delete day files inside the window whose dates are not in `keep`."""
        keep_set = set(keep)
        removed: list[Path] = []
        if not self.root.is_dir():
            return removed
        for path in sorted(self.root.glob("*.csv")):
            file_date = date_from_filename(path)
            if file_date and window.contains(file_date) and path.stem not in keep_set:
                path.unlink()
                removed.append(path)
                log.info("[pruned] file=%s", path)
        return removed


class DayCollector:
    """Accumulates quotes by day and author across many posts before writing."""

    def __init__(self) -> None:
        self._days: dict[str, QuotesByAuthor] = defaultdict(lambda: defaultdict(list))

    def add(self, date_value: str, by_author: Mapping[str, Iterable[str]]) -> None:
        day = self._days[date_value]
        for author, quotes in by_author.items():
            day[author].extend(quotes)

    def __len__(self) -> int:
        return len(self._days)

    def dates(self) -> list[str]:
        return sorted(self._days)

    def flush(self, store: DayStore, *, overwrite: bool) -> Counter[str]:
        counts: Counter[str] = Counter()
        for date_value in self.dates():
            counts[store.save(date_value, self._days[date_value], overwrite=overwrite)] += 1
        return counts


class CrawlSummary(Counter):
    """Counts of outcomes such as saved / skipped / ignored / missing / pruned."""

    def __str__(self) -> str:
        return " ".join(f"{key}={self[key]}" for key in sorted(self))


def add_crawl_args(parser: argparse.ArgumentParser, *, default_from: str | None) -> None:
    if default_from:
        parser.add_argument("--from-date", default=default_from, help="Earliest date to keep (YYYY-MM-DD).")
        parser.add_argument("--to-date", default=None, help="Latest date to keep (default: today).")
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Rewrite day files that already exist (default: skip them).",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to send (empty string keeps the requests default).",
    )


def post_field(post: Mapping, *keys: str) -> str:
    """Read nested WordPress fields such as ('title', 'rendered') with a '' fallback."""
    value: object = post
    for key in keys:
        if not isinstance(value, Mapping):
            return ""
        value = value.get(key, "")
    return value if isinstance(value, str) else ""
