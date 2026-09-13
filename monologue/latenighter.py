"""Crawler for LateNighter "Monologues Round-Up" posts via the WordPress REST API.

Each post collects the best jokes from one night's shows. Hosts are identified from
section headings, or from the attribution that follows a quote.
"""

from __future__ import annotations

import argparse
import html
import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from monologue.common import canonical_author, iso_date, normalize_text
from monologue.crawl import CrawlSummary, DateWindow, DayCollector, DayStore, add_crawl_args, post_field
from monologue.http import iter_wp_posts, make_session

log = logging.getLogger(__name__)

SOURCE = "latenighter"
WP_POSTS_API = "https://latenighter.com/wp-json/wp/v2/posts"
MONOLOGUES_TAG_ID = 180
DEFAULT_FROM_DATE = "2018-09-29"
UNKNOWN = "Unknown"
MIN_QUOTE_CHARS = 20

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

# Round-up posts are titled e.g. "Monologues Round-Up: ..."; other posts with the same tag are
# news articles about late night and are not jokes.
ROUNDUP_TITLE_RE = re.compile(r"round-?up|monologue", re.IGNORECASE)

# A quoted span. Some articles mistype the opening mark as ‘” so that sequence is accepted too.
QUOTE_RE = re.compile(r"(?:‘”|[“\"])(.{20,1500}?)[”\"]")
CLOSING_QUOTE_RE = re.compile(r'[”"]')
# Text between two quotes that is only an attribution clause ("," Kimmel said. ") — no quote marks.
ATTRIBUTION_GAP_RE = re.compile(r"^[^“”\"]{1,60}$")
SENTENCE_END = (".", "!", "?")


def is_roundup_post(title: str) -> bool:
    return bool(ROUNDUP_TITLE_RE.search(html.unescape(title)))


def infer_host(text: str | None) -> str | None:
    lower = normalize_text(text).lower()
    for canonical, aliases in HOST_ALIASES.items():
        if any(alias in lower for alias in aliases):
            return canonical_author(canonical)
    return None


def infer_host_from_tail(text: str) -> str | None:
    """Look for a host name after the last closing quote, e.g. '"..." — Seth Meyers'."""
    tail = normalize_text(text)
    closings = list(CLOSING_QUOTE_RE.finditer(tail))
    if closings:
        tail = tail[closings[-1].end() :]
    return infer_host(tail)


def is_sentence_start(quote: str) -> bool:
    """True when a quote begins like a sentence rather than mid-clause ("...were on his laptop")."""
    first = quote.lstrip("‘'(")
    return bool(first) and (first[0].isupper() or first[0].isdigit() or first[0] in "$#@")


def clean_quote(quote: str) -> str:
    """Drop the comma or semicolon that belongs to the article's attribution ("...," he said)."""
    return normalize_text(quote).rstrip(",; ")


def split_quotes(text: str) -> list[str]:
    """Quoted spans in `text`, rejoining a quote that an attribution clause interrupted.

    '“A,” Kimmel said. “B.”' becomes 'A. B' and '“A,” he said, “b.”' becomes 'A, b.'
    """
    spans: list[tuple[str, int]] = []
    for match in QUOTE_RE.finditer(text):
        quote = normalize_text(match.group(1))
        if spans:
            previous, previous_end = spans[-1]
            gap = text[previous_end : match.start()]
            if previous.endswith(",") and ATTRIBUTION_GAP_RE.match(gap):
                if gap.rstrip().endswith(SENTENCE_END):
                    joined = previous.rstrip(",") + ". " + quote
                else:
                    joined = previous + " " + quote
                spans[-1] = (joined, match.end())
                continue
        spans.append((quote, match.end()))
    return [quote for quote, _ in spans]


def extract_inline_quotes(text: str, *, sentences_only: bool = False) -> list[str]:
    """Quoted spans of at least five words. With sentences_only, drop mid-sentence fragments."""
    seen: set[str] = set()
    quotes: list[str] = []
    for span in split_quotes(normalize_text(text)):
        quote = clean_quote(span)
        if len(quote.split()) < 5 or quote in seen:
            continue
        if sentences_only and not is_sentence_start(quote):
            continue
        seen.add(quote)
        quotes.append(quote)
    return quotes


def parse_quote_text(raw: str) -> str:
    quotes = extract_inline_quotes(raw)
    return quotes[0] if quotes else clean_quote(normalize_text(raw).strip('“”"'))


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
        if len(quote) < MIN_QUOTE_CHARS:
            continue
        host = infer_host_from_tail(raw) or current_host or infer_host(raw) or UNKNOWN
        quotes.setdefault(host, []).append(quote)

    # Feature-style posts embed quotes inside prose paragraphs instead of blockquotes. The
    # host named most recently in the prose owns the quotes that follow.
    if not quotes:
        current_host = None
        for node in soup.find_all(["h2", "h3", "h4", "p", "li"]):
            text = normalize_text(node.get_text(" ", strip=True))
            named = infer_host(text)
            if named:
                current_host = named
            if len(text) < 30:
                continue
            inline = extract_inline_quotes(text, sentences_only=True)
            if inline:
                quotes.setdefault(named or current_host or UNKNOWN, []).extend(inline)
    return quotes


def crawl(
    session: requests.Session,
    store: DayStore,
    window: DateWindow,
    *,
    overwrite: bool = False,
    prune: bool = False,
) -> CrawlSummary:
    """Fetch round-up posts inside `window`, merge them by day, and write day files."""
    summary = CrawlSummary()
    days = DayCollector()
    for post in iter_wp_posts(
        session, WP_POSTS_API, tag_id=MONOLOGUES_TAG_ID, after=window.start, before=window.end
    ):
        date_value = iso_date(post["date"])
        if not window.contains(date_value):
            summary["ignored"] += 1
            continue
        title = normalize_text(post_field(post, "title", "rendered"))
        if not is_roundup_post(title):
            summary["ignored"] += 1
            log.info("[ignored] date=%s reason=not-a-round-up title=%r", date_value, title)
            continue
        quotes = parse_post(post_field(post, "content", "rendered"))
        if not quotes:
            summary["ignored"] += 1
            log.info("[ignored] date=%s reason=no-quotes link=%s", date_value, post.get("link", ""))
            continue
        days.add(date_value, quotes)
    summary.update(days.flush(store, overwrite=overwrite))
    if prune:
        summary["pruned"] = len(store.prune(days.dates(), window))
    return summary


def add_arguments(parser: argparse.ArgumentParser) -> None:
    add_crawl_args(parser, default_from=DEFAULT_FROM_DATE)
    parser.add_argument(
        "--prune-stale",
        action="store_true",
        help="Delete day files inside the date window that the crawl no longer produces.",
    )


def run(args: argparse.Namespace) -> int:
    summary = crawl(
        make_session(args.user_agent or None),
        DayStore(Path(args.data_dir) / SOURCE),
        DateWindow.from_args(args),
        overwrite=args.overwrite_existing,
        prune=args.prune_stale,
    )
    log.info("Summary: %s", summary)
    return 0
