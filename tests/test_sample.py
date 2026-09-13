import json
from pathlib import Path

from monologue.cli import main
from monologue.common import write_day_csv


def test_sample_keeps_first_and_last_day_capped(tmp_path: Path):
    data = tmp_path / "data"
    write_day_csv(data / "newsmax", "2009-06-02", {"Jay Leno": [f"joke {i}" for i in range(15)]})
    write_day_csv(data / "newsmax", "2012-01-01", {"Jay Leno": ["middle"]})
    write_day_csv(data / "newsmax", "2018-09-28", {"Conan O'Brien": ["last"]})
    write_day_csv(data / "scraps", "2020-01-01", {"John Oliver": ["only day"]})
    out = tmp_path / "sample"
    (out / "newsmax").mkdir(parents=True)
    (out / "newsmax" / "1999-01-01.csv").write_text("name,monologue\nstale,row\n")

    assert main(["--data-dir", str(data), "sample", "--out-dir", str(out), "--rows-per-file", "10"]) == 0

    assert sorted(p.name for p in (out / "newsmax").glob("*.csv")) == ["2009-06-02.csv", "2018-09-28.csv"]
    assert sorted(p.name for p in (out / "scraps").glob("*.csv")) == ["2020-01-01.csv"]
    first = (out / "newsmax" / "2009-06-02.csv").read_text().splitlines()
    assert len(first) == 11  # header + 10 rows

    coverage = json.loads((out / "coverage.json").read_text())
    assert coverage["total_rows"] == 18
    assert coverage["total_days"] == 4
    assert coverage["sources"]["newsmax"] == {
        "rows": 17,
        "first_date": "2009-06-02",
        "last_date": "2018-09-28",
        "days": 3,
        "rows_by_month": {"2009-06": 15, "2012-01": 1, "2018-09": 1},
    }
    assert "joke" not in json.dumps(coverage)


def test_data_dir_env_default(tmp_path: Path, monkeypatch, capsys):
    write_day_csv(tmp_path / "latenighter", "2024-02-27", {"Seth Meyers": ["a joke"]})
    monkeypatch.setenv("MONOLOGUE_DATA_DIR", str(tmp_path))
    assert main(["stats", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["total_rows"] == 1
