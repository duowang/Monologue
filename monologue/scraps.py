"""Crawler for full late-night transcripts posted on scrapsfromtheloft.com.

Unlike the other two sources, these are complete episode transcripts rather than
curated jokes. Paragraphs prefixed with "Speaker:" are attributed to that speaker;
everything else is attributed to the show's host.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from monologue.common import canonical_author, iso_date, normalize_text
from monologue.crawl import CrawlSummary, DateWindow, DayCollector, DayStore, add_crawl_args, post_field
from monologue.http import iter_wp_posts, make_session

log = logging.getLogger(__name__)

SOURCE = "scraps"
WP_POSTS_API = "https://scrapsfromtheloft.com/wp-json/wp/v2/posts"
DEFAULT_FROM_DATE = "2017-01-01"
MIN_QUOTE_CHARS = 20
MIN_PARAGRAPH_CHARS = 40


@dataclass(frozen=True)
class ShowTag:
    tag_id: int
    author: str
    title_keywords: tuple[str, ...]


# WordPress tags for each show, with the title keywords that mark a monologue transcript.
SHOW_TAGS = (
    ShowTag(1578, "John Oliver", ("last week tonight",)),
    ShowTag(654, "Daily Show", ("daily show",)),
    ShowTag(1628, "Seth Meyers", ("late night with seth meyers", "a closer look")),
    ShowTag(3325, "Jimmy Kimmel", ("jimmy kimmel live", "jimmy kimmel delivers first monologue")),
    ShowTag(4530, "Jimmy Kimmel", ("jimmy kimmel live", "jimmy kimmel delivers first monologue")),
    ShowTag(1382, "Jimmy Fallon", ("the tonight show starring jimmy fallon",)),
    ShowTag(1821, "Stephen Colbert", ("the late show with stephen colbert",)),
)

# Short speaker labels used in transcripts, mapped to full names.
SPEAKER_ALIASES = {
    "john": "John Oliver",
    "oliver": "John Oliver",
    "jon": "Jon Stewart",
    "seth": "Seth Meyers",
    "jimmy": "Jimmy Kimmel",
    "stephen": "Stephen Colbert",
    "desi": "Desi Lydic",
}

# Capitalized words that open a sentence with a colon but are not speakers ("Look:", "Namely:").
NOT_SPEAKERS = frozenset(
    {
        "also",
        "anyway",
        "because",
        "example",
        "finally",
        "first",
        "look",
        "meanwhile",
        "namely",
        "no",
        "note",
        "now",
        "okay",
        "plus",
        "reminder",
        "second",
        "so",
        "spoiler",
        "third",
        "translation",
        "update",
        "wait",
        "well",
        "yes",
    }
)

# Lowercase particles that can appear inside a proper name ("Bill de Blasio").
NAME_PARTICLES = frozenset({"da", "de", "del", "di", "la", "le", "van", "von"})

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
    """Return a speaker name for a "Label:" prefix, or None if the label is not a speaker.

    Real labels are either known aliases ("JOHN", "Oliver") or short Title Case / ALL CAPS
    names ("Announcer", "Alex Jones"). Sentence-case phrases ("Fun fact", "The point is")
    and single filler words ("Look") are prose, not speakers.
    """
    words = normalize_text(label).split()
    if not words or len(words) > 3:
        return None
    key = normalize_text(re.sub(r"[^a-z' ]", "", " ".join(words).lower()))
    if key in SPEAKER_ALIASES:
        return SPEAKER_ALIASES[key]
    if not all(word[0].isupper() or word.lower() in NAME_PARTICLES for word in words):
        return None
    if len(words) == 1 and key in NOT_SPEAKERS:
        return None
    return canonical_author(" ".join(part.capitalize() for part in key.split()))


def is_relevant_post(title: str, link: str, title_keywords: tuple[str, ...]) -> bool:
    lower_title = normalize_text(title).lower()
    if "transcript" not in lower_title and "transcript" not in normalize_text(link).lower():
        return False
    return not title_keywords or any(keyword in lower_title for keyword in title_keywords)


def is_noise_paragraph(text: str) -> bool:
    """Episode metadata lines that are not part of the transcript."""
    return text.lower().startswith(NOISE_PREFIXES)


def parse_post(content_html: str, default_author: str) -> dict[str, list[str]]:
    """Return {speaker: [paragraphs]} for one transcript post."""
    soup = BeautifulSoup(content_html, "html.parser")
    quotes: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()

    def add(speaker: str, quote: str, minimum: int) -> None:
        quote = quote.strip('“”"')
        if len(quote) >= minimum and quote not in seen:
            quotes[speaker].append(quote)
            seen.add(quote)

    for para in soup.select("p"):
        text = normalize_text(para.get_text(" ", strip=True))
        if not text or is_noise_paragraph(text):
            continue
        match = SPEAKER_RE.match(text)
        speaker = canonical_speaker(match.group(1)) if match else None
        if match and speaker:
            add(speaker, normalize_text(match.group(2)), MIN_QUOTE_CHARS)
        else:
            add(default_author, text, MIN_PARAGRAPH_CHARS)
    return dict(quotes)


def crawl(
    session: requests.Session,
    store: DayStore,
    window: DateWindow,
    *,
    overwrite: bool = False,
    prune: bool = False,
    show_tags: tuple[ShowTag, ...] = SHOW_TAGS,
) -> CrawlSummary:
    """Fetch transcript posts for every show tag inside `window` and write merged day files."""
    summary = CrawlSummary()
    days = DayCollector()
    for show in show_tags:
        for post in iter_wp_posts(
            session, WP_POSTS_API, tag_id=show.tag_id, after=window.start, before=window.end
        ):
            summary["scanned"] += 1
            date_value = iso_date(post["date"])
            title = normalize_text(post_field(post, "title", "rendered"))
            link = normalize_text(post.get("link", ""))
            if not window.contains(date_value) or not is_relevant_post(title, link, show.title_keywords):
                summary["ignored"] += 1
                log.debug("[ignored] date=%s title=%r", date_value, title)
                continue
            quotes = parse_post(post_field(post, "content", "rendered"), show.author)
            if not quotes:
                summary["ignored"] += 1
                log.info("[ignored] date=%s reason=no-text link=%s", date_value, link)
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
