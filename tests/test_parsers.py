from pathlib import Path

import pytest

from monologue import latenighter, newsmax, scraps

FIXTURES = Path(__file__).parent / "fixtures"


def test_newsmax_parse_page():
    date_value, by_host = newsmax.parse_page((FIXTURES / "newsmax_page.html").read_text())
    assert date_value == "2017-01-03"
    assert set(by_host) == {"James Corden", "Conan O'Brien", "Jimmy Fallon"}
    assert by_host["James Corden"] == [
        "I'm no expert, but I'm pretty sure you can't stop a nuclear missile by tweeting at it."
    ]


def test_newsmax_parse_page_rejects_missing_date():
    assert newsmax.parse_page("<div class='jokespage'></div>") == (None, {})


@pytest.mark.parametrize(
    ("src", "expected"),
    [
        ("/img/newsmax_jokes_personalities_Jimmy_Fallon_jokes.jpg", "Jimmy Fallon"),
        ("/img/SethMeyers.png", "Seth Meyers"),
        ("/img/logo.png", None),
    ],
)
def test_newsmax_infer_name_from_src(src, expected):
    assert newsmax.infer_name_from_src(src) == expected


def test_latenighter_parse_post():
    quotes = latenighter.parse_post((FIXTURES / "latenighter_post.html").read_text())
    assert list(quotes["Jimmy Kimmel"]) == [
        "This is funny, Trump owes the state of New York 450 million dollars. That’s the funny part."
    ]
    assert quotes["Stephen Colbert"] == [
        "Over 1 in 5 GOP primary voters said they would not vote for Trump in November."
    ]
    assert quotes["Desi Lydic"] == ["Some quote from the Daily Show that is long enough to be kept."]
    assert "Unknown" not in quotes


def test_latenighter_paragraph_fallback():
    html = "<p>Kimmel opened with “a joke that is long enough to be extracted from prose” tonight.</p>"
    assert latenighter.parse_post(html) == {
        "Jimmy Kimmel": ["a joke that is long enough to be extracted from prose"]
    }


def test_scraps_parse_post():
    quotes = scraps.parse_post((FIXTURES / "scraps_post.html").read_text(), "John Oliver")
    assert quotes["John Oliver"] == [
        "Last Week Tonight with John Oliver Season 4 Episode 17 Aired on June 25, 2017",
        "Welcome to Last Week Tonight. I’m John Oliver. Thank you for joining us tonight.",
        "Look, before we begin: you may remember, last week, we did a story about coal.",
    ]
    assert quotes["Announcer"] == ["And now, a word from our sponsors, who are wonderful people."]


@pytest.mark.parametrize(
    ("title", "link", "keywords", "expected"),
    [
        ("Last Week Tonight – Transcript", "https://x/a", ["last week tonight"], True),
        ("Last Week Tonight", "https://x/transcript", ["last week tonight"], True),
        ("Some Interview", "https://x/transcript", ["last week tonight"], False),
        ("Last Week Tonight", "https://x/a", ["last week tonight"], False),
    ],
)
def test_scraps_is_relevant_post(title, link, keywords, expected):
    assert scraps.is_relevant_post(title, link, keywords) is expected
