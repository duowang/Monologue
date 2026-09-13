"""Crawler for full late-night transcripts posted on scrapsfromtheloft.com.

Unlike the other two sources, these are complete episode transcripts rather than
curated jokes. Paragraphs prefixed with "Speaker:" are attributed to that speaker;
everything else is attributed to the show's host.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from monologue.common import (
    add_date_window_args,
    add_overwrite_args,
    canonical_author,
    date_from_filename,
    fetch_wp_posts,
    iso_date,
    normalize_text,
    parse_iso_date,
    resolve_window,
    write_day_csv,
)

SOURCE = "scraps"
WP_POSTS_API = "https://scrapsfromtheloft.com/wp-json/wp/v2/posts"
DEFAULT_FROM_DATE = "2017-01-01"

# WordPress tag id -> default author and the title keywords that mark a monologue transcript.
TAG_CONFIG: dict[int, dict] = {
    1578: {"author": "John Oliver", "title_keywords": ["last week tonight"]},
    654: {"author": "Daily Show", "title_keywords": ["daily show"]},
    1628: {"author": "Seth Meyers", "title_keywords": ["late night with seth meyers", "a closer look"]},
    3325: {
        "author": "Jimmy Kimmel",
        "title_keywords": ["jimmy kimmel live", "jimmy kimmel delivers first monologue"],
    },
    4530: {
        "author": "Jimmy Kimmel",
        "title_keywords": ["jimmy kimmel live", "jimmy kimmel delivers first monologue"],
    },
    1382: {"author": "Jimmy Fallon", "title_keywords": ["the tonight show starring jimmy fallon"]},
    1821: {"author": "Stephen Colbert", "title_keywords": ["the late show with stephen colbert"]},
}

# First names used as speaker labels in transcripts.
FIRST_NAME_ALIASES = {
    "john": "John Oliver",
    "jon": "Jon Stewart",
    "seth": "Seth Meyers",
    "jimmy": "Jimmy Kimmel",
    "stephen": "Stephen Colbert",
    "desi": "Desi Lydic",
}

SPEAKER_RE = re.compile(r"^([A-Za-z][A-Za-z .'-]{0,40}):\s+(.+)$")
NOISE_PREFIXES = (
    "aired on ",
    "main segment:",
    "other segments:",
    "the daily show ,",
    "the daily show,",
    "* * *",
)


def canonical_speaker(label: str) -> str | None:
    text = normalize_text(re.sub(r"[^a-z' ]", "", normalize_text(label).lower()))
    if text in FIRST_NAME_ALIASES:
        return FIRST_NAME_ALIASES[text]
    if 1 <= len(text.split()) <= 3:
        return canonical_author(" ".join(part.capitalize() for part in text.split()))
    return None


def is_relevant_post(title: str, link: str, title_keywords: list[str]) -> bool:
    lower_title = normalize_text(title).lower()
    if "transcript" not in lower_title and "transcript" not in normalize_text(link).lower():
        return False
    return not title_keywords or any(keyword in lower_title for keyword in title_keywords)


def is_noise_paragraph(text: str) -> bool:
    return len(text) < 40 or text.lower().startswith(NOISE_PREFIXES)


def parse_post(content_html: str, default_author: str) -> dict[str, list[str]]:
    """Return {speaker: [paragraphs]} for one transcript post."""
    soup = BeautifulSoup(content_html, "html.parser")
    quotes: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()

    for para in soup.select("p"):
        text = normalize_text(para.get_text(" ", strip=True))
        if not text:
            continue
        match = SPEAKER_RE.match(text)
        if match:
            speaker = canonical_speaker(match.group(1)) or default_author
            quote = normalize_text(match.group(2)).strip('“”"')
            if len(quote) >= 20 and quote not in seen:
                quotes[speaker].append(quote)
                seen.add(quote)
            continue
        if is_noise_paragraph(text):
            continue
        quote = text.strip('“”"')
        if quote not in seen:
            quotes[default_author].append(quote)
            seen.add(quote)
    return dict(quotes)


def add_arguments(parser) -> None:
    add_date_window_args(parser, DEFAULT_FROM_DATE)
    add_overwrite_args(parser)
    parser.add_argument(
        "--prune-stale",
        action="store_true",
        help="Delete day files inside the date window that the crawl no longer produces.",
    )


def run(args) -> int:
    output_dir = Path(args.data_dir) / SOURCE
    skip_existing = not args.overwrite_existing
    from_date, to_date = resolve_window(args)

    session = requests.Session()
    by_day: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    scanned = ignored = 0

    for tag_id, config in TAG_CONFIG.items():
        for post in fetch_wp_posts(session, WP_POSTS_API, tag_id):
            scanned += 1
            date_value = iso_date(post["date"])
            title = normalize_text(post.get("title", {}).get("rendered", ""))
            link = normalize_text(post.get("link", ""))
            if not from_date <= parse_iso_date(date_value) <= to_date or not is_relevant_post(
                title, link, config["title_keywords"]
            ):
                ignored += 1
                continue
            quotes = parse_post(post.get("content", {}).get("rendered", ""), config["author"])
            if not quotes:
                ignored += 1
                continue
            for author, entries in quotes.items():
                by_day[date_value][author].extend(entries)

    saved = skipped = 0
    for date_value in sorted(by_day):
        path = output_dir / f"{date_value}.csv"
        if skip_existing and path.exists():
            skipped += 1
            print(f"[skipped] date={date_value} file={path}")
            continue
        write_day_csv(output_dir, date_value, by_day[date_value])
        saved += 1
        total = sum(len(v) for v in by_day[date_value].values())
        print(f"[saved] date={date_value} authors={len(by_day[date_value])} quotes={total} file={path}")

    pruned = 0
    if args.prune_stale and output_dir.is_dir():
        keep = {output_dir / f"{d}.csv" for d in by_day}
        for existing in output_dir.glob("*.csv"):
            file_date = date_from_filename(existing)
            if file_date and from_date <= file_date <= to_date and existing not in keep:
                existing.unlink()
                pruned += 1
                print(f"[pruned] file={existing}")

    print(f"Summary: scanned={scanned} saved={saved} skipped={skipped} ignored={ignored} pruned={pruned}")
    return 0
