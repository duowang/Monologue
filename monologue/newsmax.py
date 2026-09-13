"""Crawler for Newsmax "Best of Late Nite Jokes" pages (newsmax.com/jokes/<page>).

Newsmax stopped publishing this feature on 2018-09-28, so this crawler is mainly
kept for reproducibility. Pages are numbered sequentially; each page is one day.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from monologue.common import (
    add_overwrite_args,
    canonical_author,
    get_with_retry,
    normalize_text,
    write_day_csv,
)

SOURCE = "newsmax"
BASE_URL = "https://www.newsmax.com/jokes/{page}"
ARCHIVE_URL = "https://www.newsmax.com/jokes/archive/"
DEFAULT_START_PAGE = 1756
DEFAULT_FALLBACK_WINDOW = 1000

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
BAD_NAME_TOKENS = {"newsmax", "jokes", "personalities"}


def match_known_host(value: str | None) -> str | None:
    if not value:
        return None
    for pattern, name in HOST_PATTERNS.items():
        if re.search(pattern, value, flags=re.IGNORECASE):
            return name
    return None


def _title_case(token: str) -> str:
    if "'" in token:
        return "'".join(part.capitalize() for part in token.split("'"))
    return token.capitalize()


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
    words = [w.strip(".'-") for w in text.split()]
    words = [w for w in words if w]
    if not 2 <= len(words) <= 5:
        return None
    cleaned = normalize_text(" ".join(_title_case(w) for w in words))
    if len(cleaned) < 3:
        return None
    return canonical_author(cleaned)


def infer_name_from_alt(alt: str | None) -> str | None:
    text = normalize_text(alt)
    if not text:
        return None
    for pattern in (
        r"\bwith\s+([A-Za-z.'\- ]+)$",
        r"\bstarring(?:\s+with)?\s+([A-Za-z.'\- ]+)$",
        r"\bhosted by\s+([A-Za-z.'\- ]+)$",
        r"\bfeaturing\s+([A-Za-z.'\- ]+)$",
    ):
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
            if len(text) > 10:
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


def discover_latest_page(session: requests.Session, *, timeout: float, retries: int) -> int:
    response = get_with_retry(session, ARCHIVE_URL, timeout=timeout, retries=retries)
    if response is None:
        raise RuntimeError(f"Archive endpoint returned 404: {ARCHIVE_URL}")
    for pattern, haystack in (
        (r"/jokes/(\d+)/?$", response.url),
        (
            r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']https?://[^"\']*/jokes/(\d+)/?["\']',
            response.text,
        ),
        (r"/jokes/(\d+)", response.text),
    ):
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    raise RuntimeError("Unable to infer latest page id from archive page.")


def add_arguments(parser) -> None:
    add_overwrite_args(parser)
    parser.add_argument("--start-page", type=int, default=DEFAULT_START_PAGE)
    parser.add_argument("--end-page", type=int, default=None)
    parser.add_argument(
        "--auto-end", action="store_true", help="Infer the latest page id from /jokes/archive/."
    )
    parser.add_argument(
        "--fallback-window",
        type=int,
        default=DEFAULT_FALLBACK_WINDOW,
        help="Pages to scan past --start-page when no end page is known.",
    )
    parser.add_argument(
        "--stop-after-miss", type=int, default=50, help="Stop after N consecutive empty pages."
    )
    parser.add_argument(
        "--stop-after-same-date",
        type=int,
        default=20,
        help="Stop after N consecutive pages resolving to the same date (guards against redirects).",
    )
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0.1, help="Seconds to wait between pages.")
    parser.add_argument("--user-agent", default="", help="Optional User-Agent header.")


def run(args) -> int:
    output_dir = Path(args.data_dir) / SOURCE
    skip_existing = not args.overwrite_existing

    session = requests.Session()
    if args.user_agent:
        session.headers.update({"User-Agent": args.user_agent})

    end_page = args.end_page
    if end_page is None and args.auto_end:
        try:
            end_page = discover_latest_page(session, timeout=args.timeout, retries=args.retries)
            print(f"Discovered latest page: {end_page}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] auto-end discovery failed ({type(exc).__name__}: {exc})")
    if end_page is None:
        end_page = args.start_page + args.fallback_window
        print(f"Scanning bounded window [{args.start_page}, {end_page}]")
    if end_page < args.start_page:
        raise SystemExit("--end-page must be >= --start-page")

    counts = {"saved": 0, "skipped": 0, "missing": 0}
    misses = 0
    same_date = 0
    previous_date = None

    for page in range(args.start_page, end_page + 1):
        status, date_value, path = "missing", None, None
        try:
            response = get_with_retry(
                session, BASE_URL.format(page=page), timeout=args.timeout, retries=args.retries
            )
            if response is not None:
                date_value, by_host = parse_page(response.text)
                if date_value and by_host:
                    path = output_dir / f"{date_value}.csv"
                    if skip_existing and path.exists():
                        status = "skipped"
                    else:
                        write_day_csv(output_dir, date_value, by_host)
                        status = "saved"
        except Exception as exc:  # noqa: BLE001
            print(f"[error] page={page} reason={exc}")

        counts[status] += 1
        if status == "missing":
            misses += 1
            previous_date, same_date = None, 0
            print(f"[missing] page={page}")
        else:
            misses = 0
            same_date = same_date + 1 if date_value == previous_date else 1
            previous_date = date_value
            print(f"[{status}] page={page} date={date_value} file={path}")

        if misses >= args.stop_after_miss:
            print(f"Stopping after {misses} consecutive misses.")
            break
        if same_date >= args.stop_after_same_date:
            print(f"Stopping after {same_date} consecutive pages dated {date_value}.")
            break
        if args.sleep > 0:
            time.sleep(args.sleep)

    print("Summary:", " ".join(f"{k}={v}" for k, v in counts.items()))
    return 0
