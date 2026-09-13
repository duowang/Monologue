"""Offline tests for the YouTube source, using a fake client instead of the network."""

from datetime import date
from pathlib import Path

import pytest

from monologue import youtube
from monologue.common import read_day_csv
from monologue.crawl import DateWindow, DayStore
from monologue.youtube import Video, split_jokes, upload_date_to_iso

WINDOW = DateWindow(date(2026, 1, 1), date(2026, 12, 31))
FALLON = youtube.SHOWS_BY_CHANNEL["FallonTonight"]
MEYERS = youtube.SHOWS_BY_CHANNEL["LateNightSeth"]
KIMMEL = youtube.SHOWS_BY_CHANNEL["JimmyKimmelLive"]


class FakeClient:
    """Serves canned listings, metadata, and transcripts; records what was asked for."""

    def __init__(self, listings: dict[str, list[Video]], meta: dict[str, dict], transcripts: dict[str, str]):
        self.listings = listings
        self.meta = meta
        self.transcripts = transcripts
        self.transcripts_fetched: list[str] = []

    def list_videos(self, channel, limit):
        if channel not in self.listings:
            raise RuntimeError(f"channel unavailable: {channel}")
        return self.listings[channel][:limit]

    def get_metadata(self, video_id):
        return self.meta[video_id]

    def get_transcript(self, video_id):
        self.transcripts_fetched.append(video_id)
        if video_id not in self.transcripts:
            raise RuntimeError("no captions")
        return self.transcripts[video_id]


def meta(date_ymd: str, title: str, description: str = "", duration: int = 900) -> dict:
    return {"upload_date": date_ymd, "title": title, "description": description, "duration": duration}


# --- classification --------------------------------------------------------


@pytest.mark.parametrize(
    ("show", "title", "description", "expected"),
    [
        # Fallon is identified by the fixed description template.
        (FALLON, "Trump Goes Brunette | The Tonight Show", "Jimmy addresses the latest news, like...", True),
        (FALLON, "Sandra Bullock Talks Practical Magic", "Sandra talks about the reunion.", False),
        # Meyers' monologue is the "A Closer Look" segment; reruns are excluded.
        (MEYERS, "Trump's Cabinet Meeting Meltdown: A Closer Look", "", True),
        (MEYERS, "Trump's Toxic Convention: A Closer Look Out of Office", "", False),
        (MEYERS, "A Closer Look Back: Trump's Iran War", "", False),
        (MEYERS, "Trump's Flop, Iran Flare-Up: A Closer Look Out of Office", "", False),
        (MEYERS, "Paul Rudd Causes a Ruckus | Late Night Throwback", "", False),
        (MEYERS, "Late Night with Seth Meyers Audience Q&A: Jackals", "", False),
        # Kimmel has no template, so interviews, music, and bits are excluded by title.
        (KIMMEL, "Trump Bribes Americans For Votes, Brags About Crowd Size", "", True),
        (KIMMEL, "Trump Makes 9/11 About Himself & Jimmy to Interview Talarico on YouTube", "", True),
        (KIMMEL, "Gal Gadot on Meeting Madonna, Having Four Young Daughters", "", False),
        (KIMMEL, "Chef David Chang on His Kids Not Eating Anything He Cooks", "", False),
        (KIMMEL, "MUNA – Dancing On The Wall", "", False),
        (KIMMEL, "This Week in Unnecessary Censorship", "", False),
        (KIMMEL, "3 Ridiculous Questions with Joey King – Sponsored by Astral Tequila", "", False),
        (KIMMEL, "Nicole Kidman Reveals Her Ibiza Raving Disguise", "", False),
    ],
)
def test_show_matches(show, title, description, expected):
    assert show.matches(title, description, 900) is expected


def test_listing_filter_keeps_description_only_shows_until_metadata_is_known():
    # Fallon is matched on description, which a listing does not carry.
    assert FALLON.matches_listing("Trump Goes Brunette | The Tonight Show", 600) is True
    assert FALLON.matches("Trump Goes Brunette | The Tonight Show", "", 600) is False


def test_short_clips_are_rejected():
    assert KIMMEL.matches("Trump Bribes Americans For Votes", "", 60) is False
    assert FALLON.matches("Monologue", "Jimmy addresses the latest news", 30) is False


# --- transcript segmentation ----------------------------------------------


def test_split_jokes_uses_reaction_markers_as_boundaries():
    transcript = (
        "Thank you for watching at home, we appreciate every one of you tonight. [cheering] "
        "Tonight is the very first night of NFL football and the fantasies have been drafted. "
        "[ Cheers and applause ] "
        "Two teams from California are flying all the way to Australia to play a game. [ Man boos ]"
    )
    assert split_jokes(transcript) == [
        "Thank you for watching at home, we appreciate every one of you tonight.",
        "Tonight is the very first night of NFL football and the fantasies have been drafted.",
        "Two teams from California are flying all the way to Australia to play a game.",
    ]


def test_split_jokes_ends_a_row_when_another_speaker_starts():
    transcript = (
        "-Welcome everybody, welcome to the show tonight, you made it here at last! "
        "-Melania. -Well, formerly known as that, yes, and he lives in the Burger Office."
    )
    assert split_jokes(transcript) == [
        "Welcome everybody, welcome to the show tonight, you made it here at last!",
        "Well, formerly known as that, yes, and he lives in the Burger Office.",
    ]


def test_split_jokes_drops_short_fragments_music_and_stage_directions():
    assert split_jokes("[music] Yeah. [laughter] Right, ok. [applause]") == []
    assert split_jokes("♪♪ -Hey. -Hi.") == []
    assert split_jokes("") == []


def test_split_jokes_breaks_up_a_long_run_without_laughter():
    sentence = "This is a sentence about the news that runs on for a while and keeps going. "
    jokes = split_jokes(sentence * 12)
    assert len(jokes) > 1
    assert all(len(j) <= youtube.MAX_JOKE_CHARS for j in jokes)
    assert all(j.endswith(".") for j in jokes)


@pytest.mark.parametrize(("value", "expected"), [("20260910", "2026-09-10"), ("", None), ("nonsense", None)])
def test_upload_date_to_iso(value, expected):
    assert upload_date_to_iso(value) == expected


# --- crawl loop ------------------------------------------------------------


def test_crawl_writes_day_files_and_merges_shows(tmp_path: Path):
    client = FakeClient(
        listings={
            "FallonTonight": [Video("f1", "Trump Goes Brunette | The Tonight Show", 600)],
            "LateNightSeth": [Video("m1", "Trump's Convention: A Closer Look", 800)],
        },
        meta={
            "f1": meta("20260910", "Trump Goes Brunette", "Jimmy addresses the latest news, like..."),
            "m1": meta("20260910", "Trump's Convention: A Closer Look"),
        },
        transcripts={
            "f1": "Trump promised everyone five thousand dollars if the party wins. [laughter]",
            "m1": "The convention had empty seats and a gavel that was far too big. [applause]",
        },
    )
    summary = youtube.crawl(client, DayStore(tmp_path), WINDOW, shows=(FALLON, MEYERS), sleep=0)
    assert summary == {"scanned": 2, "monologues": 2, "saved": 1}
    rows = read_day_csv(tmp_path / "2026-09-10.csv")
    assert [r["name"] for r in rows] == ["Jimmy Fallon", "Seth Meyers"]


def test_crawl_skips_non_monologues_before_fetching_captions(tmp_path: Path):
    client = FakeClient(
        listings={
            "JimmyKimmelLive": [
                Video("k1", "Gal Gadot on Meeting Madonna", 800),
                Video("k2", "MUNA – Dancing On The Wall", 500),
                Video("k3", "Trump Bribes Americans For Votes", 825),
            ]
        },
        meta={"k3": meta("20260910", "Trump Bribes Americans For Votes")},
        transcripts={
            "k3": "He is bribing people for their votes with a five thousand dollar check. [laughter]"
        },
    )
    summary = youtube.crawl(client, DayStore(tmp_path), WINDOW, shows=(KIMMEL,), sleep=0)
    assert summary == {"scanned": 3, "ignored": 2, "monologues": 1, "saved": 1}
    assert client.transcripts_fetched == ["k3"]


def test_crawl_records_missing_captions_and_channel_errors(tmp_path: Path):
    client = FakeClient(
        listings={"LateNightSeth": [Video("m1", "A Closer Look at the News", 800)]},
        meta={"m1": meta("20260910", "A Closer Look at the News")},
        transcripts={},
    )
    summary = youtube.crawl(client, DayStore(tmp_path), WINDOW, shows=(MEYERS, FALLON), sleep=0)
    assert summary["no_captions"] == 1 and summary["errors"] == 1
    assert not list(tmp_path.glob("*.csv"))


def test_crawl_stops_once_past_the_date_window(tmp_path: Path):
    old = [Video(f"v{i}", f"A Closer Look number {i}", 800) for i in range(10)]
    client = FakeClient(
        listings={"LateNightSeth": old},
        meta={f"v{i}": meta("20200101", f"A Closer Look number {i}") for i in range(10)},
        transcripts={},
    )
    summary = youtube.crawl(client, DayStore(tmp_path), WINDOW, shows=(MEYERS,), stop_after_old=3, sleep=0)
    assert summary["scanned"] == 3 and summary["ignored"] == 3


# --- rate limiting ---------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (RuntimeError("YouTube is blocking requests from your IP. This usually is due to..."), True),
        (RuntimeError("Too Many Requests"), True),
        (RuntimeError("Subtitles are disabled for this video"), False),
    ],
)
def test_is_blocked_error(exc, expected):
    assert youtube.is_blocked_error(exc) is expected


def test_crawl_stops_immediately_when_captions_are_blocked(tmp_path: Path):
    class BlockingClient(FakeClient):
        def get_transcript(self, video_id):
            self.transcripts_fetched.append(video_id)
            raise youtube.CaptionsBlocked("blocking requests from your IP")

    videos = [Video(f"m{i}", f"A Closer Look number {i}", 800) for i in range(5)]
    client = BlockingClient(
        listings={"LateNightSeth": videos, "JimmyKimmelLive": videos},
        meta={f"m{i}": meta("20260910", f"A Closer Look number {i}") for i in range(5)},
        transcripts={},
    )
    summary = youtube.crawl(client, DayStore(tmp_path), WINDOW, shows=(MEYERS, KIMMEL), sleep=0)
    # One attempt, then the run stops rather than burning through both channels.
    assert summary["blocked"] == 1
    assert client.transcripts_fetched == ["m0"]
    assert "no_captions" not in summary
