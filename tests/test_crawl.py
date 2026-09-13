"""Offline tests for the crawl loops, using the FakeSession from conftest."""

from datetime import date
from pathlib import Path

import pytest
import requests

from monologue import latenighter, newsmax, scraps
from monologue.common import read_day_csv
from monologue.crawl import CrawlSummary, DateWindow, DayCollector, DayStore
from monologue.http import get, iter_wp_posts
from tests.conftest import FakeResponse, FakeSession, wp_post, wp_posts

FIXTURES = Path(__file__).parent / "fixtures"
WINDOW = DateWindow(date(2017, 1, 1), date(2030, 1, 1))


# --- building blocks -------------------------------------------------------


def test_date_window_contains_strings_and_dates():
    window = DateWindow(date(2024, 1, 1), date(2024, 1, 31))
    assert window.contains("2024-01-15") and window.contains(date(2024, 1, 31))
    assert not window.contains("2024-02-01")


def test_day_store_save_skip_overwrite_and_prune(tmp_path: Path):
    store = DayStore(tmp_path)
    assert store.save("2024-01-01", {"A": ["one"]}) == "saved"
    assert store.save("2024-01-01", {"A": ["two"]}) == "skipped"
    assert read_day_csv(store.path("2024-01-01"))[0]["monologue"] == "one"
    assert store.save("2024-01-01", {"A": ["two"]}, overwrite=True) == "saved"
    assert read_day_csv(store.path("2024-01-01"))[0]["monologue"] == "two"

    store.save("2024-01-02", {"A": ["x"]})
    store.save("2023-06-01", {"A": ["outside window"]})
    removed = store.prune(keep=["2024-01-01"], window=DateWindow(date(2024, 1, 1), date(2024, 12, 31)))
    assert [p.name for p in removed] == ["2024-01-02.csv"]
    assert store.exists("2023-06-01")


def test_day_collector_merges_posts_on_the_same_day(tmp_path: Path):
    days = DayCollector()
    days.add("2024-01-01", {"A": ["one"]})
    days.add("2024-01-01", {"A": ["two"], "B": ["three"]})
    assert days.flush(DayStore(tmp_path), overwrite=False) == {"saved": 1}
    rows = read_day_csv(tmp_path / "2024-01-01.csv")
    assert [(r["name"], r["monologue"]) for r in rows] == [("A", "one"), ("A", "two"), ("B", "three")]


def test_summary_str_is_sorted():
    summary = CrawlSummary(saved=2, ignored=1)
    assert str(summary) == "ignored=1 saved=2"


# --- http ------------------------------------------------------------------


def test_get_retries_then_succeeds():
    attempts = []

    def handler(url, params):
        attempts.append(1)
        return 503 if len(attempts) < 3 else FakeResponse(text="ok")

    assert get(FakeSession(handler), "https://x", retries=3).text == "ok"
    assert len(attempts) == 3


def test_get_returns_none_on_404_and_raises_after_retries():
    assert get(FakeSession(lambda u, p: 404), "https://x") is None
    with pytest.raises(requests.HTTPError):
        get(FakeSession(lambda u, p: 500), "https://x", retries=2)
    with pytest.raises(requests.ConnectionError):
        get(FakeSession(lambda u, p: requests.ConnectionError("down")), "https://x", retries=2)


def test_iter_wp_posts_pages_and_filters_server_side():
    def handler(url, params):
        page = params["page"]
        return wp_posts([{"id": page}], total_pages=2)

    session = FakeSession(handler)
    posts = list(iter_wp_posts(session, "https://x/posts", tag_id=7, after=date(2024, 1, 1)))
    assert [p["id"] for p in posts] == [1, 2]
    first = session.calls[0][1]
    assert first["tags"] == 7 and first["after"] == "2024-01-01T00:00:00" and first["order"] == "desc"


# --- crawlers --------------------------------------------------------------


def test_latenighter_crawl_writes_merged_day_files(tmp_path: Path):
    html = (FIXTURES / "latenighter_post.html").read_text()
    posts = [
        wp_post("2024-02-27", html, title="Monologues Round-Up"),
        wp_post("2024-02-27", "<blockquote>tiny</blockquote>", title="Monologues Round-Up"),
    ]
    summary = latenighter.crawl(FakeSession(lambda u, p: wp_posts(posts)), DayStore(tmp_path), WINDOW)
    assert summary == {"saved": 1, "ignored": 1}
    rows = read_day_csv(tmp_path / "2024-02-27.csv")
    assert {r["name"] for r in rows} == {"Jimmy Kimmel", "Stephen Colbert", "Desi Lydic"}


def test_latenighter_crawl_ignores_news_articles_and_prunes(tmp_path: Path):
    html = (FIXTURES / "latenighter_post.html").read_text()
    posts = [
        wp_post("2024-02-27", html, title="Monologues Round-Up: Tuesday"),
        wp_post("2026-01-27", html, title="Colbert, Kimmel Call ‘Bullsh*t’ on Victim-Blaming"),
    ]
    store = DayStore(tmp_path)
    store.save("2026-01-27", {"Jimmy Kimmel": ["from an earlier crawl of the news article"]})
    summary = latenighter.crawl(FakeSession(lambda u, p: wp_posts(posts)), store, WINDOW, prune=True)
    assert summary == {"saved": 1, "ignored": 1, "pruned": 1}
    assert store.exists("2024-02-27") and not store.exists("2026-01-27")


def test_latenighter_crawl_skips_existing(tmp_path: Path):
    html = (FIXTURES / "latenighter_post.html").read_text()
    session = FakeSession(lambda u, p: wp_posts([wp_post("2024-02-27", html, title="Monologues Round-Up")]))
    assert latenighter.crawl(session, DayStore(tmp_path), WINDOW) == {"saved": 1}
    assert latenighter.crawl(session, DayStore(tmp_path), WINDOW) == {"skipped": 1}
    assert latenighter.crawl(session, DayStore(tmp_path), WINDOW, overwrite=True) == {"saved": 1}


def test_scraps_crawl_filters_titles_and_prunes(tmp_path: Path):
    html = (FIXTURES / "scraps_post.html").read_text()
    oliver = scraps.ShowTag(1578, "John Oliver", ("last week tonight",))
    posts = [
        wp_post("2017-06-26", html, title="Last Week Tonight – Transcript"),
        wp_post("2017-06-27", html, title="Some interview transcript"),
    ]
    store = DayStore(tmp_path)
    store.save("2017-07-01", {"John Oliver": ["stale"]})
    summary = scraps.crawl(
        FakeSession(lambda u, p: wp_posts(posts)), store, WINDOW, prune=True, show_tags=(oliver,)
    )
    assert summary == {"scanned": 2, "ignored": 1, "saved": 1, "pruned": 1}
    assert store.exists("2017-06-26") and not store.exists("2017-07-01")


def test_newsmax_crawl_stops_on_repeated_dates(tmp_path: Path):
    html = (FIXTURES / "newsmax_page.html").read_text()
    session = FakeSession(lambda u, p: FakeResponse(text=html))
    summary = newsmax.crawl(
        session, DayStore(tmp_path), start_page=1, end_page=50, stop_after_same_date=3, sleep=0
    )
    assert summary == {"saved": 1, "skipped": 2}
    assert len(session.calls) == 3
    assert session.calls[0][0] == "https://www.newsmax.com/jokes/1/"


def test_newsmax_crawl_stops_on_misses_and_survives_errors(tmp_path: Path):
    def handler(url, params):
        return requests.ConnectionError("boom") if url.endswith("/2/") else 404

    summary = newsmax.crawl(
        FakeSession(handler), DayStore(tmp_path), start_page=1, end_page=50, stop_after_miss=3, retries=1
    )
    assert summary == {"missing": 3}
