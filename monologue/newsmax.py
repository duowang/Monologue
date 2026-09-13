"""Crawler for Newsmax "Best of Late Nite Jokes" pages (newsmax.com/jokes/<page>).

Pages are numbered sequentially and each page is one broadcast day. Newsmax stopped
publishing the column on 2018-09-28; every page number past the end now serves that
final day, which is why the crawler stops after a run of identical dates.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from monologue.common import canonical_author, normalize_text
from monologue.crawl import CrawlSummary, DayStore, add_crawl_args
from monologue.http import get, make_session

log = logging.getLogger(__name__)

SOURCE = "newsmax"
PAGE_URL = "https://www.newsmax.com/jokes/{page}/"
DEFAULT_START_PAGE = 1756
DEFAULT_PAGE_WINDOW = 1000
MIN_JOKE_CHARS = 11

# Regex fragments found in the host image's alt/src attributes, mapped to canonical names.
HOST_PATTERNS = {
    "Jay": "Jay Leno",
    "Meyers": "Seth Meyers",
    "Letterman": "David Letterman",
    "Kimmel": "Jimmy Kimmel",
    "Conan": "Conan O'Brien",
    "Fallon": "Jimmy Fallon",
    "Corden": "James Corden",
    "Colbert": "Stephen Colbert",
    "Ferguson": "Craig Ferguson",
}
BAD_NAME_TOKENS = ("newsmax", "jokes", "personalities")
NAME_IN_ALT_PATTERNS = (
    r"\bwith\s+([A-Za-z.'\- ]+)$",
    r"\bstarring(?:\s+with)?\s+([A-Za-z.'\- ]+)$",
    r"\bhosted by\s+([A-Za-z.'\- ]+)$",
    r"\bfeaturing\s+([A-Za-z.'\- ]+)$",
)


def match_known_host(value: str | None) -> str | None:
    if not value:
        return None
    for pattern, name in HOST_PATTERNS.items():
        if re.search(pattern, value, flags=re.IGNORECASE):
            return name
    return None


def _title_case(token: str) -> str:
    return "'".join(part.capitalize() for part in token.split("'"))


def clean_candidate_name(value: str | None) -> str | None:
    """Turn free text like 'late night with seth meyers' into a plausible person name, or None."""
    if not value:
        return None
    raw = normalize_text(value)
    if "://" in raw or raw.startswith("/"):
        return None
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg)\b", raw, flags=re.IGNORECASE):
        return None
    text = normalize_text(re.sub(r"[^A-Za-z.' -]", " ", raw))
    if not text or any(token in text.lower() for token in BAD_NAME_TOKENS):
        return None
    words = [w for w in (w.strip(".'-") for w in text.split()) if w]
    if not 2 <= len(words) <= 5:
        return None
    cleaned = normalize_text(" ".join(_title_case(w) for w in words))
    return canonical_author(cleaned) if len(cleaned) >= 3 else None


def infer_name_from_alt(alt: str | None) -> str | None:
    text = normalize_text(alt)
    if not text:
        return None
    for pattern in NAME_IN_ALT_PATTERNS:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = clean_candidate_name(match.group(1))
            if candidate:
                return candidate
    return clean_candidate_name(text)


def infer_name_from_src(src: str | None) -> str | None:
    if not src:
        return None
    path = urlparse(src).path or src
    stem, _ = os.path.splitext(os.path.basename(unquote(path)))
    if not stem:
        return None
    stem = re.sub(r"(?i)^newsmax_jokes_personalities_", "", stem)
    stem = re.sub(r"(?i)_?jokes?$", "", stem)
    stem = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", stem.replace("_", " "))
    return clean_candidate_name(stem)


def parse_host(header_node) -> str | None:
    img = header_node.find("img")
    alt = img.attrs.get("alt", "") if img is not None else ""
    src = img.attrs.get("src", "") if img is not None else ""

    # Historical quirk: some Seth Meyers entries carried the wrong alt text.
    if alt == "Late Night With Seth Meyers":
        resolved = match_known_host(src) or match_known_host(alt)
    else:
        resolved = match_known_host(alt) or match_known_host(src)
    if resolved:
        return resolved

    header_text = normalize_text(" ".join(header_node.stripped_strings))
    return infer_name_from_alt(alt) or infer_name_from_src(src) or infer_name_from_alt(header_text)


def parse_date(soup: BeautifulSoup) -> str | None:
    node = soup.find("div", class_="jokesDate")
    if node is None:
        return None
    text = normalize_text(node.get_text(" ", strip=True))
    try:
        return datetime.strptime(text, "%A %b %d %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_jokes(header_node) -> list[str]:
    jokes: list[str] = []
    node = header_node.find_next_sibling()
    while node is not None:
        if node.name == "div" and "jokesHeader" in node.attrs.get("class", []):
            break
        if node.name == "p":
            text = normalize_text(node.get_text(" ", strip=True))
            if len(text) >= MIN_JOKE_CHARS:
                jokes.append(text)
        node = node.find_next_sibling()
    return jokes


def parse_page(html: str) -> tuple[str | None, dict[str, list[str]]]:
    """Return (date, {host: [jokes]}) for one Newsmax jokes page."""
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", class_="jokespage")
    date_value = parse_date(soup)
    if container is None or date_value is None:
        return None, {}

    by_host: dict[str, list[str]] = {}
    for header in container.find_all("div", class_="jokesHeader"):
        host = parse_host(header)
        if host is None:
            continue
        jokes = extract_jokes(header)
        if jokes:
            by_host.setdefault(host, []).extend(jokes)
    return date_value, by_host


def crawl(
    session: requests.Session,
    store: DayStore,
    *,
    start_page: int,
    end_page: int,
    overwrite: bool = False,
    stop_after_miss: int = 50,
    stop_after_same_date: int = 20,
    timeout: float = 20.0,
    retries: int = 3,
    sleep: float = 0.1,
) -> CrawlSummary:
    """Walk page numbers from start_page to end_page, writing one day file per page."""
    if end_page < start_page:
        raise ValueError("end_page must be >= start_page")

    summary = CrawlSummary()
    misses = 0
    same_date_run = 0
    previous_date: str | None = None

    for page in range(start_page, end_page + 1):
        date_value: str | None = None
        by_host: dict[str, list[str]] = {}
        try:
            response = get(session, PAGE_URL.format(page=page), timeout=timeout, retries=retries)
            if response is not None:
                date_value, by_host = parse_page(response.text)
        except requests.RequestException as exc:
            log.warning("[error] page=%d reason=%s", page, exc)

        if not date_value or not by_host:
            summary["missing"] += 1
            misses += 1
            previous_date, same_date_run = None, 0
            log.info("[missing] page=%d", page)
            if misses >= stop_after_miss:
                log.info("Stopping after %d consecutive misses.", misses)
                break
        else:
            misses = 0
            same_date_run = same_date_run + 1 if date_value == previous_date else 1
            previous_date = date_value
            log.debug("page=%d date=%s", page, date_value)
            summary[store.save(date_value, by_host, overwrite=overwrite)] += 1
            if same_date_run >= stop_after_same_date:
                log.info("Stopping after %d consecutive pages dated %s.", same_date_run, date_value)
                break

        if sleep > 0:
            time.sleep(sleep)
    return summary


def add_arguments(parser: argparse.ArgumentParser) -> None:
    add_crawl_args(parser, default_from=None)
    parser.add_argument("--start-page", type=int, default=DEFAULT_START_PAGE)
    parser.add_argument(
        "--end-page",
        type=int,
        default=None,
        help=f"Last page to fetch (default: --start-page + {DEFAULT_PAGE_WINDOW}).",
    )
    parser.add_argument(
        "--stop-after-miss", type=int, default=50, help="Stop after N consecutive empty pages."
    )
    parser.add_argument(
        "--stop-after-same-date",
        type=int,
        default=20,
        help="Stop after N consecutive pages resolving to the same date (the end of the archive).",
    )
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0.1, help="Seconds to wait between pages.")


def run(args: argparse.Namespace) -> int:
    end_page = args.end_page if args.end_page is not None else args.start_page + DEFAULT_PAGE_WINDOW
    if end_page < args.start_page:
        raise SystemExit("--end-page must be >= --start-page")
    summary = crawl(
        make_session(args.user_agent or None),
        DayStore(Path(args.data_dir) / SOURCE),
        start_page=args.start_page,
        end_page=end_page,
        overwrite=args.overwrite_existing,
        stop_after_miss=args.stop_after_miss,
        stop_after_same_date=args.stop_after_same_date,
        timeout=args.timeout,
        retries=args.retries,
        sleep=args.sleep,
    )
    log.info("Summary: %s", summary)
    return 0
