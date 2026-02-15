# Monologue Dataset

This repo stores late-night monologue/joke text in date-based CSV files from multiple sources.

## Repository structure

- `newsmax_crawler.py`: crawls Newsmax "Best of Late Nite Jokes" pages.
- `latenighter_crawler.py`: crawls LateNighter "Monologues Round-Up" pages.
- `scraps_crawler.py`: crawls selected transcript posts from scrapsfromtheloft.com.
- `newsmax/`: Newsmax CSV files (`YYYY-MM-DD.csv`, with older years in subfolders).
- `latenighter/`: LateNighter CSV files (`YYYY-MM-DD.csv`).
- `scraps/`: Scraps transcript CSV files (`YYYY-MM-DD.csv`).
- `csv2sql.py`: imports all available source CSV files into Postgres.
- `schema.sql`: table definitions.

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Crawl commands

### Newsmax

```bash
python3 newsmax_crawler.py \
  --start-page 1840 \
  --auto-end \
  --stop-after-same-date 8 \
  --stop-after-miss 100 \
  --skip-existing
```

Notes:

- Newsmax content currently plateaus at `2018-09-28`.
- `--auto-end` attempts latest-page discovery from `/jokes/archive/`.
- Use lower timeout/retry values if you are hitting frequent `ReadTimeout` errors.

### LateNighter

```bash
python3 latenighter_crawler.py \
  --from-date 2018-09-29 \
  --skip-existing
```

### Scraps (transcript source)

```bash
python3 scraps_crawler.py \
  --from-date 2017-01-01 \
  --skip-existing
```

If filtering rules are updated and you need to remove stale files:

```bash
python3 scraps_crawler.py \
  --from-date 2017-01-01 \
  --overwrite-existing \
  --prune-stale
```

## Export all CSV rows to one text file

The following command creates a tab-separated text file:

```bash
python3 - <<'PY'
import csv
from pathlib import Path

out_path = Path("monologues_all_sources.txt")
roots = [Path("newsmax"), Path("latenighter"), Path("scraps")]

files = []
for root in roots:
    if root.exists():
        files.extend(sorted(root.rglob("*.csv")))

with out_path.open("w", encoding="utf-8", newline="") as out:
    out.write("source\tdate\tname\tmonologue\n")
    for csv_path in sorted(files):
        source = csv_path.parts[0]
        date = csv_path.stem
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                name = (row.get("name") or "").strip().replace("\t", " ")
                text = (row.get("monologue") or "").strip().replace("\t", " ")
                text = " ".join(text.split())
                if name and text:
                    out.write(f"{source}\t{date}\t{name}\t{text}\n")
PY
```

## Import to Postgres

```bash
python3 csv2sql.py
```

Environment variables used by `csv2sql.py`:

- `MONOLOGUE_DB_USER`
- `MONOLOGUE_DB_PASSWORD` (defaults to user value)
- `MONOLOGUE_DB_NAME` (defaults to user value)
- `MONOLOGUE_DB_HOST` (defaults to `localhost`)
