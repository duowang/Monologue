# Monologue

**47,000+ late-night TV monologue jokes, 2009 to today, as plain CSV.**

A small, hand-curated text dataset built from three public sources, plus the Python crawlers
that keep it growing. Useful for humor research, NLP experiments, topic and sentiment analysis
of political comedy, or just reading a decade of jokes.

[![CI](https://github.com/duowang/Monologue/actions/workflows/ci.yml/badge.svg)](https://github.com/duowang/Monologue/actions/workflows/ci.yml)
![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)
[![License: MIT](https://img.shields.io/badge/code-MIT-green.svg)](LICENSE)

```text
source       date        name             monologue
newsmax      2017-01-03  James Corden     I'm no expert, but I'm pretty sure you can't stop a nuclear missile by tweeting at it.
latenighter  2024-02-27  Jimmy Fallon     Trump actually had two versions of his speech. A victory speech in case he won. And a victory speech in case he lost.
```

## The data

| Source | Days | Rows | From | To |
|---|---:|---:|---|---|
| newsmax | 1,966 | 26,230 | 2009-06-02 | 2018-09-28 |
| latenighter | 224 | 6,941 | 2024-02-27 | 2026-01-27 |
| scraps | 181 | 13,995 | 2017-06-26 | 2025-11-17 |
| **Total** | **2,371** | **47,166** | | |

| Host | Rows |
|---|---:|
| John Oliver | 12,287 |
| Jimmy Fallon | 6,587 |
| Jimmy Kimmel | 5,560 |
| Conan O'Brien | 4,712 |
| Seth Meyers | 3,665 |
| Jay Leno | 3,296 |
| Craig Ferguson | 3,293 |
| Stephen Colbert | 2,432 |
| David Letterman | 1,385 |
| James Corden | 1,020 |
| Jon Stewart | 356 |
| Daily Show | 326 |

Regenerate these tables any time with `monologue stats`.

### Layout

```text
data/
  newsmax/       one CSV per broadcast day; 2009-2016 are grouped in year folders
  latenighter/   one CSV per round-up post
  scraps/        one CSV per transcript day
  monologues.tsv everything flattened into one tab-separated file
```

Every day file is named `YYYY-MM-DD.csv` and has the same two columns:

| Column | Meaning |
|---|---|
| `name` | Host or speaker, normalized to one spelling per person |
| `monologue` | One joke or one transcript paragraph |

The flattened file [`data/monologues.tsv`](data/monologues.tsv) adds `source` and `date` columns
in front, so you can load the whole dataset with one call:

```python
import pandas as pd

df = pd.read_csv("data/monologues.tsv", sep="\t")
df.groupby("name").size().sort_values(ascending=False).head()
```

### Sources, and how they differ

| Source | What it is | Character |
|---|---|---|
| **newsmax** | Newsmax's daily "Best of Late Nite Jokes" column | Short, editor-selected one-liners. The column ended on 2018-09-28. |
| **latenighter** | LateNighter's "Monologues Round-Up" posts | Short, editor-selected jokes. Attribution is inferred from headings and quote tails, so a few rows are `Unknown`. |
| **scraps** | Full episode transcripts from Scraps from the Loft | Complete monologue and desk-segment transcripts, one paragraph per row. Much longer, and includes labeled speakers other than the host (announcers, guests, clips). |

If you want only curated jokes, use `newsmax` and `latenighter`. If you want long-form text,
use `scraps`. The two kinds are not directly comparable in length or density.

## Install

```bash
python -m pip install -e ".[dev]"
```

This installs a `monologue` command. Add the `db` extra if you want the Postgres loader.

## Usage

```bash
monologue stats                              # Markdown tables like the ones above
monologue stats --json                       # machine-readable, with per-source host counts
monologue export -o data/monologues.tsv      # rebuild the flattened file
monologue export --format jsonl --source scraps -o scraps.jsonl
```

### Updating the dataset

Each crawler skips days that already exist, so re-running is cheap and safe.

```bash
monologue crawl latenighter --from-date 2024-01-01
monologue crawl scraps --from-date 2017-01-01
monologue crawl newsmax --start-page 1840 --auto-end --stop-after-same-date 8   # archive only; no new content since 2018
```

Pass `--overwrite-existing` to rebuild days after changing a parser, and
`monologue crawl scraps --prune-stale` to delete day files a stricter filter no longer produces.

### Loading into Postgres

```bash
psql "$DATABASE_URL" -f schema.sql
MONOLOGUE_DB_USER=me MONOLOGUE_DB_NAME=monologue monologue import-db
```

Rows are inserted with `ON CONFLICT DO NOTHING` on the joke text, so re-importing never
creates duplicates. Set `MONOLOGUE_DB_PASSWORD` and `MONOLOGUE_DB_HOST` as needed, or pass
`--dsn` with a full libpq connection string.

## Development

```bash
ruff check . && ruff format --check .
pytest
```

The test suite covers the three parsers with small HTML fixtures, the export and stats
commands, and an integrity pass over every committed CSV (date-named, standard header,
non-empty). CI runs the same checks on Python 3.9 and 3.12.

```text
monologue/
  common.py       text normalization, canonical host names, CSV I/O, HTTP retries
  newsmax.py      page-number crawler and HTML parser
  latenighter.py  WordPress API crawler and round-up parser
  scraps.py       WordPress API crawler and transcript parser
  export.py       TSV / JSONL flattening
  stats.py        dataset summary
  db.py           Postgres loader
  cli.py          argparse entry point
```

## Attribution and licensing

The code in this repository is released under the [MIT License](LICENSE).

The joke text belongs to the shows and writers who created it and was collected from
[Newsmax](https://www.newsmax.com/jokes/), [LateNighter](https://latenighter.com/), and
[Scraps from the Loft](https://scrapsfromtheloft.com/). It is provided here for research and
educational use. Please credit the original hosts and sources if you build on it.

## Citation

```bibtex
@misc{wang_monologue,
  author = {Duo Wang},
  title  = {Monologue: a dataset of late-night TV monologue jokes},
  year   = {2026},
  url    = {https://github.com/duowang/Monologue}
}
```
