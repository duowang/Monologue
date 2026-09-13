import json
from pathlib import Path

from monologue.cli import main
from monologue.common import write_day_csv


def make_dataset(root: Path) -> None:
    write_day_csv(root / "newsmax", "2017-01-03", {"Jay Leno": ["First joke", "Second\tjoke"]})
    write_day_csv(root / "latenighter", "2024-02-27", {"Seth Meyers": ["Third joke"]})


def test_export_tsv(tmp_path: Path, capsys):
    make_dataset(tmp_path)
    assert main(["--data-dir", str(tmp_path), "export"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "source\tdate\tname\tmonologue"
    assert lines[1:] == [
        "newsmax\t2017-01-03\tJay Leno\tFirst joke",
        "newsmax\t2017-01-03\tJay Leno\tSecond joke",
        "latenighter\t2024-02-27\tSeth Meyers\tThird joke",
    ]


def test_export_jsonl_to_file_with_source_filter(tmp_path: Path):
    make_dataset(tmp_path)
    out = tmp_path / "out" / "rows.jsonl"
    assert (
        main(
            [
                "--data-dir",
                str(tmp_path),
                "export",
                "--format",
                "jsonl",
                "-o",
                str(out),
                "--source",
                "latenighter",
            ]
        )
        == 0
    )
    records = [json.loads(line) for line in out.read_text().splitlines()]
    assert records == [
        {"source": "latenighter", "date": "2024-02-27", "name": "Seth Meyers", "monologue": "Third joke"}
    ]


def test_stats_json(tmp_path: Path, capsys):
    make_dataset(tmp_path)
    assert main(["--data-dir", str(tmp_path), "stats", "--json"]) == 0
    stats = json.loads(capsys.readouterr().out)
    assert stats["total_rows"] == 3
    assert stats["sources"]["newsmax"] == {
        "files": 1,
        "rows": 2,
        "first_date": "2017-01-03",
        "last_date": "2017-01-03",
        "authors": [["Jay Leno", 2]],
    }
    assert stats["top_authors"][0] == ["Jay Leno", 2]


def test_stats_markdown(tmp_path: Path, capsys):
    make_dataset(tmp_path)
    main(["--data-dir", str(tmp_path), "stats"])
    out = capsys.readouterr().out
    assert "| newsmax | 1 | 2 | 2017-01-03 | 2017-01-03 |" in out
