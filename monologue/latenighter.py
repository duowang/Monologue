"""Crawler for LateNighter "Monologues Round-Up" posts via the WordPress REST API.

Each post collects the best jokes from one night's shows. Hosts are identified from
section headings, or from the attribution that follows a quote.
"""

from __future__ import annotations

import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from monologue.common import (
    add_date_window_args,
    add_overwrite_args,
    canonical_author,
    fetch_wp_posts,
    iso_date,
    normalize_text,
    parse_iso_date,
    resolve_window,
    write_day_csv,
)

SOURCE = "latenighter"
WP_POSTS_API = "https://latenighter.com/wp-json/wp/v2/posts"
MONOLOGUES_TAG_ID = 180
DEFAULT_FROM_DATE = "2018-09-29"
UNKNOWN = "Unknown"

# Substrings (lower-case) that identify a host in a heading or attribution.
HOST_ALIASES = {
    "Stephen Colbert": ("stephen colbert", "colbert"),
    "Jimmy Kimmel": ("jimmy kimmel", "kimmel"),
    "Seth Meyers": ("seth meyers", "meyers"),
    "Jimmy Fallon": ("jimmy fallon", "fallon"),
    "Desi Lydic": ("desi lydic", "lydic"),
    "Jon Stewart": ("jon stewart", "stewart"),
    "Jordan Klepper": ("jordan klepper", "klepper"),
    "Ronny Chieng": ("ronny chieng", "chieng"),
    "Michael Kosta": ("michael kosta", "kosta"),
    "Taylor Tomlinson": ("taylor tomlinson", "tomlinson"),
}

QUOTE_RE = re.compile(r"[“\"](.{20,400}?)[”\"]")


def infer_host(text: str | None) -> str | None:
    lower = normalize_text(text).lower()
    for canonical, aliases in HOST_ALIASES.items():
        if any(alias in lower for alias in aliases):
            return canonical_author(canonical)
    return None


def infer_host_from_tail(text: str) -> str | None:
    """Look for a host name after the last closing quote, e.g. '"..." — Seth Meyers'."""
    tail = normalize_text(text)
    closings = list(re.finditer(r'[”"]', tail))
    if closings:
        tail = tail[closings[-1].end() :]
    return infer_host(tail)


def extract_inline_quotes(text: str) -> list[str]:
    seen: set[str] = set()
    quotes: list[str] = []
    for match in QUOTE_RE.findall(normalize_text(text)):
        quote = normalize_text(match)
        if len(quote.split()) < 5 or quote in seen:
            continue
        seen.add(quote)
        quotes.append(quote)
    return quotes


def parse_quote_text(raw: str) -> str:
    quotes = extract_inline_quotes(raw)
    return quotes[0] if quotes else normalize_text(raw).strip('“”"')


def parse_post(content_html: str) -> dict[str, list[str]]:
    """Return {host: [quotes]} from a round-up post's rendered HTML."""
    soup = BeautifulSoup(content_html, "html.parser")
    current_host: str | None = None
    quotes: dict[str, list[str]] = {}

    for node in soup.find_all(["h2", "h3", "h4", "blockquote"]):
        if node.name != "blockquote":
            heading_host = infer_host(node.get_text(" ", strip=True))
            if heading_host:
                current_host = heading_host
            continue
        raw = normalize_text(node.get_text(" ", strip=True))
        quote = parse_quote_text(raw)
        if len(quote) < 20:
            continue
        host = infer_host_from_tail(raw) or current_host or infer_host(raw) or UNKNOWN
        quotes.setdefault(host, []).append(quote)

    # Feature-style posts embed quotes inside paragraphs instead of blockquotes.
    if not quotes:
        for node in soup.find_all(["p", "li"]):
            text = normalize_text(node.get_text(" ", strip=True))
            if len(text) < 30:
                continue
            inline = extract_inline_quotes(text)
            if inline:
                quotes.setdefault(infer_host(text) or UNKNOWN, []).extend(inline)
    return quotes


def add_arguments(parser) -> None:
    add_date_window_args(parser, DEFAULT_FROM_DATE)
    add_overwrite_args(parser)


def run(args) -> int:
    output_dir = Path(args.data_dir) / SOURCE
    skip_existing = not args.overwrite_existing
    from_date, to_date = resolve_window(args)

    session = requests.Session()
    counts = {"saved": 0, "skipped": 0, "ignored": 0}

    for post in fetch_wp_posts(session, WP_POSTS_API, MONOLOGUES_TAG_ID):
        date_value = iso_date(post["date"])
        if not from_date <= parse_iso_date(date_value) <= to_date:
            counts["ignored"] += 1
            continue
        path = output_dir / f"{date_value}.csv"
        if skip_existing and path.exists():
            counts["skipped"] += 1
            print(f"[skipped] date={date_value} file={path}")
            continue
        quotes = parse_post(post.get("content", {}).get("rendered", ""))
        if not quotes:
            counts["ignored"] += 1
            print(f"[ignored] date={date_value} reason=no-quotes")
            continue
        write_day_csv(output_dir, date_value, quotes)
        counts["saved"] += 1
        total = sum(len(v) for v in quotes.values())
        print(f"[saved] date={date_value} hosts={len(quotes)} quotes={total} file={path}")

    print("Summary:", " ".join(f"{k}={v}" for k, v in counts.items()))
    return 0
