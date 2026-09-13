from pathlib import Path

from monologue.common import canonical_author, iter_rows, normalize_text, write_day_csv


def test_normalize_text_collapses_whitespace():
    assert normalize_text("  a \n\t b  ") == "a b"
    assert normalize_text(None) == ""


def test_canonical_author_fixes_known_spellings():
    assert canonical_author("Conan O'Brian") == "Conan O'Brien"
    assert canonical_author("colbert") == "Stephen Colbert"
    assert canonical_author("Somebody New") == "Somebody New"


def test_write_and_read_round_trip(tmp_path: Path):
    day = tmp_path / "newsmax"
    write_day_csv(day, "2017-01-03", {"Conan O'Brian": ['A joke, with "quotes"'], "Jay": ["Second"]})
    rows = list(iter_rows(tmp_path))
    assert [(r.author, r.text) for r in rows] == [
        ("Conan O'Brien", 'A joke, with "quotes"'),
        ("Jay Leno", "Second"),
    ]
    assert rows[0].source == "newsmax" and rows[0].date == "2017-01-03"
