"""Crawler for monologues published on the shows' own YouTube channels.

Each show uploads its monologue as a separate video within hours of airing, with
machine-generated captions that include audience-reaction markers. Those markers are
the laugh lines, so they double as joke boundaries.

Unlike the editor-curated sources, these rows are the host's spoken words as the
caption engine heard them: no punctuation cleanup, occasional mistranscriptions, and
the ad-libs and crowd work that a written round-up would leave out.

Needs the optional extra: pip install 'monologue[youtube]'
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from monologue.common import normalize_text
from monologue.crawl import CrawlSummary, DateWindow, DayCollector, DayStore, add_crawl_args
from monologue.http import DEFAULT_USER_AGENT

log = logging.getLogger(__name__)

SOURCE = "youtube"
DEFAULT_FROM_DATE = "2026-01-01"
CHANNEL_URL = "https://www.youtube.com/@{channel}/videos"

# How many uploads to list per channel, and how many consecutive too-old videos to
# tolerate before giving up on a channel (listings are newest first).
DEFAULT_MAX_VIDEOS = 60
DEFAULT_STOP_AFTER_OLD = 12

# Shows differ: some upload human-made en-US captions, others only auto-generated en.
CAPTION_LANGUAGES = ("en", "en-US", "en-GB", "en-CA", "en-AU")

MIN_JOKE_CHARS = 45
MIN_JOKE_WORDS = 8
MAX_JOKE_CHARS = 400

# Bracketed stage directions: "[Laughter]", "[ Cheers and applause ]", "[ Man boos ]", "[music]".
# Every one of them is a break in the host's speech, so all of them end a row.
BRACKET_RE = re.compile(r"\[[^\]]{0,60}\]")
# Manual captions mark a change of speaker with a dash at the start of a line: "-Melania."
SPEAKER_TURN_RE = re.compile(r"(?:^|(?<=\s))[-–—](?=[A-Z\"'“‘])")
MUSIC_RE = re.compile(r"[♪♫]+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")

# YouTube throttles caption requests per IP. When that happens every later fetch fails too,
# so the run stops instead of recording a whole listing as "no captions".
BLOCKED_ERRORS = ("IpBlocked", "RequestBlocked", "TooManyRequests", "YouTubeRequestFailed")


class CaptionsBlocked(RuntimeError):
    """Raised when YouTube is rejecting caption requests from this IP address."""


def is_blocked_error(exc: Exception) -> bool:
    if type(exc).__name__ in BLOCKED_ERRORS:
        return True
    message = str(exc).lower()
    return "blocking requests from your ip" in message or "too many requests" in message


@dataclass(frozen=True)
class Show:
    """One channel, plus the rules that pick its monologue uploads out of everything else."""

    channel: str
    author: str
    # Title must match this, when given (Meyers' monologue is the "A Closer Look" segment).
    title_include: re.Pattern | None = None
    # Description must match this, when given (Fallon's monologues use a fixed template).
    description_include: re.Pattern | None = None
    # Titles that look like interviews, music, or recurring bits rather than a monologue.
    title_exclude: tuple[re.Pattern, ...] = field(default_factory=tuple)
    min_duration: int = 120

    def matches_listing(self, title: str, duration: int | None) -> bool:
        """Title and duration rules only, for filtering a channel listing cheaply.

        Description rules cannot be judged here, so a show that relies on one keeps every
        candidate until its metadata is fetched.
        """
        if duration is not None and duration < self.min_duration:
            return False
        if any(pattern.search(title) for pattern in self.title_exclude):
            return False
        return not (self.title_include and not self.title_include.search(title))

    def matches(self, title: str, description: str, duration: int | None) -> bool:
        """The full rule set, applied once the video's metadata is known."""
        if not self.matches_listing(title, duration):
            return False
        return not (self.description_include and not self.description_include.search(description))


def _re(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.IGNORECASE)


# A guest interview leads with the guest's name: "Gal Gadot on Meeting Madonna",
# "Nicole Kidman Reveals ...". Only the first few words are considered so that a
# monologue title mentioning "... on YouTube" late is not caught.
INTERVIEW_TITLE = _re(
    r"^(?:\w[\w.'’-]*[ &]+){0,3}\w[\w.'’-]*\s+(?:on|talks?|reveals?|reacts?|explains?|"
    r"weighs in|remembers|recalls|performs|discusses|breaks down)\s"
)
MUSIC_TITLE = _re(r"\s[–—]\s")  # "Artist – Song"
GUEST_BIT_TITLE = _re(r"\bwith\s+[A-Z]|sponsored by|audience q&a|throwback|\bplays\b|challenge\b")

SHOWS: tuple[Show, ...] = (
    Show(
        channel="FallonTonight",
        author="Jimmy Fallon",
        # "Jimmy addresses the latest news, like ..." opens every Tonight Show monologue.
        description_include=_re(r"^jimmy addresses the latest news"),
    ),
    Show(
        channel="LateNightSeth",
        author="Seth Meyers",
        # "A Closer Look" is the show's political monologue; "Back" entries are reruns.
        title_include=_re(r"a closer look"),
        # "Back" entries are rerun compilations and "Out of Office" is the off-season
        # format, which is Seth in conversation with a writer rather than a monologue.
        title_exclude=(_re(r"a closer look back"), _re(r"out of office"), _re(r"throwback")),
    ),
    Show(
        channel="JimmyKimmelLive",
        author="Jimmy Kimmel",
        # No description template, so exclude everything that is plainly not a monologue.
        title_exclude=(
            INTERVIEW_TITLE,
            MUSIC_TITLE,
            GUEST_BIT_TITLE,
            _re(r"unnecessary censorship|ridiculous questions|kids help|lie witness|pedestrian question"),
        ),
        min_duration=420,
    ),
)
SHOWS_BY_CHANNEL = {show.channel: show for show in SHOWS}


@dataclass(frozen=True)
class Video:
    video_id: str
    title: str
    duration: int | None


class YouTubeClient:
    """Thin wrapper over yt-dlp and youtube-transcript-api, so crawl() can be faked in tests."""

    def __init__(self, user_agent: str = DEFAULT_USER_AGENT):
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            from yt_dlp import YoutubeDL
        except ImportError as exc:  # pragma: no cover - only without the extra
            raise SystemExit(
                "yt-dlp and youtube-transcript-api are required; run: pip install 'monologue[youtube]'"
            ) from exc
        self._YoutubeDL = YoutubeDL
        self._transcripts = YouTubeTranscriptApi()
        self._base_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "http_headers": {"User-Agent": user_agent} if user_agent else {},
        }

    def list_videos(self, channel: str, limit: int) -> list[Video]:
        opts = {**self._base_opts, "extract_flat": "in_playlist", "playlistend": limit}
        with self._YoutubeDL(opts) as ydl:
            info = ydl.extract_info(CHANNEL_URL.format(channel=channel), download=False)
        return [
            Video(
                video_id=entry["id"],
                title=normalize_text(entry.get("title")),
                duration=int(entry["duration"]) if entry.get("duration") else None,
            )
            for entry in (info or {}).get("entries", [])
            if entry and entry.get("id")
        ]

    def get_metadata(self, video_id: str) -> dict:
        with self._YoutubeDL(self._base_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        return {
            "upload_date": (info or {}).get("upload_date") or "",
            "description": (info or {}).get("description") or "",
            "title": normalize_text((info or {}).get("title")),
            "duration": (info or {}).get("duration"),
        }

    def get_transcript(self, video_id: str) -> str:
        try:
            fetched = self._transcripts.fetch(video_id, languages=list(CAPTION_LANGUAGES))
        except Exception as exc:
            if is_blocked_error(exc):
                first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else "captions blocked"
                raise CaptionsBlocked(first_line) from exc
            raise
        return " ".join(snippet.text for snippet in fetched)


def upload_date_to_iso(value: str) -> str | None:
    """Convert yt-dlp's YYYYMMDD upload_date to YYYY-MM-DD."""
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


SPLIT_MARKER = "\x00"


def split_jokes(transcript: str) -> list[str]:
    """Split a caption transcript into joke-sized rows.

    Captions mark an audience reaction with a bracketed direction ("[Laughter]") and a change
    of speaker with a leading dash. Both end a row: the first is where a joke lands, the second
    is where someone other than the host starts talking. A long stretch with neither is split
    on sentence boundaries so no row runs on for pages.
    """
    text = MUSIC_RE.sub(" ", transcript or "")
    text = BRACKET_RE.sub(SPLIT_MARKER, text)
    text = SPEAKER_TURN_RE.sub(SPLIT_MARKER, text)

    jokes: list[str] = []
    for chunk in text.split(SPLIT_MARKER):
        cleaned = normalize_text(chunk)
        if not cleaned:
            continue
        for piece in _cap_length(cleaned):
            if len(piece) >= MIN_JOKE_CHARS and len(piece.split()) >= MIN_JOKE_WORDS:
                jokes.append(piece)
    return jokes


def _cap_length(text: str) -> Iterator[str]:
    """Yield `text`, broken on sentence ends when it is longer than MAX_JOKE_CHARS."""
    if len(text) <= MAX_JOKE_CHARS:
        yield text
        return
    buffer = ""
    for sentence in SENTENCE_SPLIT_RE.split(text):
        candidate = f"{buffer} {sentence}".strip()
        if buffer and len(candidate) > MAX_JOKE_CHARS:
            yield buffer
            buffer = sentence
        else:
            buffer = candidate
    if buffer:
        yield buffer


def crawl(
    client: YouTubeClient,
    store: DayStore,
    window: DateWindow,
    *,
    overwrite: bool = False,
    shows: tuple[Show, ...] = SHOWS,
    max_videos: int = DEFAULT_MAX_VIDEOS,
    stop_after_old: int = DEFAULT_STOP_AFTER_OLD,
    sleep: float = 0.5,
) -> CrawlSummary:
    """Collect monologue captions from each show's channel and write day files."""
    summary = CrawlSummary()
    days = DayCollector()

    blocked = False
    for show in shows:
        if blocked:
            break
        try:
            videos = client.list_videos(show.channel, max_videos)
        except Exception as exc:  # noqa: BLE001 - a dead channel must not kill the run
            log.warning("[error] channel=%s reason=%s", show.channel, exc)
            summary["errors"] += 1
            continue

        consecutive_old = 0
        for video in videos:
            summary["scanned"] += 1
            # Cheap pre-filter on the listing, before paying for a metadata fetch.
            if not show.matches_listing(video.title, video.duration):
                summary["ignored"] += 1
                continue
            try:
                meta = client.get_metadata(video.video_id)
            except Exception as exc:  # noqa: BLE001
                log.warning("[error] video=%s reason=%s", video.video_id, exc)
                summary["errors"] += 1
                continue

            date_value = upload_date_to_iso(meta.get("upload_date", ""))
            if not date_value:
                summary["ignored"] += 1
                continue
            if not window.contains(date_value):
                consecutive_old += 1
                summary["ignored"] += 1
                if consecutive_old >= stop_after_old:
                    log.info("[stop] channel=%s past the date window", show.channel)
                    break
                continue
            consecutive_old = 0

            if not show.matches(meta["title"] or video.title, meta["description"], meta["duration"]):
                summary["ignored"] += 1
                log.debug("[ignored] video=%s title=%r", video.video_id, video.title)
                continue

            if sleep > 0:
                time.sleep(sleep)
            try:
                transcript = client.get_transcript(video.video_id)
            except CaptionsBlocked as exc:
                log.warning(
                    "[blocked] YouTube is refusing caption requests from this IP (%s). "
                    "Stopping; retry later or from another network.",
                    exc,
                )
                summary["blocked"] += 1
                blocked = True
                break
            except Exception as exc:  # noqa: BLE001 - captions are often missing or disabled
                log.info("[no-captions] video=%s date=%s reason=%s", video.video_id, date_value, exc)
                summary["no_captions"] += 1
                continue

            jokes = split_jokes(transcript)
            if not jokes:
                summary["ignored"] += 1
                continue
            days.add(date_value, {show.author: jokes})
            summary["monologues"] += 1
            log.info(
                "[found] date=%s author=%s rows=%d video=%s",
                date_value,
                show.author,
                len(jokes),
                video.video_id,
            )

    summary.update(days.flush(store, overwrite=overwrite))
    return summary


def add_arguments(parser: argparse.ArgumentParser) -> None:
    add_crawl_args(parser, default_from=DEFAULT_FROM_DATE)
    parser.add_argument(
        "--max-videos",
        type=int,
        default=DEFAULT_MAX_VIDEOS,
        help="How many recent uploads to inspect per channel.",
    )
    parser.add_argument(
        "--channel",
        action="append",
        choices=sorted(SHOWS_BY_CHANNEL),
        help="Restrict to one channel (repeatable).",
    )
    parser.add_argument("--sleep", type=float, default=0.5, help="Seconds to wait between videos.")


def run(args: argparse.Namespace) -> int:
    shows = tuple(SHOWS_BY_CHANNEL[c] for c in args.channel) if args.channel else SHOWS
    summary = crawl(
        YouTubeClient(args.user_agent),
        DayStore(Path(args.data_dir) / SOURCE),
        DateWindow.from_args(args),
        overwrite=args.overwrite_existing,
        shows=shows,
        max_videos=args.max_videos,
        sleep=args.sleep,
    )
    log.info("Summary: %s", summary)
    return 0
