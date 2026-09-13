# Monologue

**47,000+ late-night TV monologue jokes, 2009 to today, as plain CSV.**

A hand-curated text dataset built from three public sources, plus the Python crawlers that
keep it growing. Useful for humor research, NLP experiments, and topic or sentiment analysis
of political comedy.

This repo holds the code, the tooling, and a small public [sample](sample/) that documents
the format and how far back and how fresh the data is. The full text is not published here;
see [Getting the full dataset](#getting-the-full-dataset).

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
| latenighter | 225 | 6,843 | 2024-02-27 | 2026-02-26 |
| scraps | 184 | 14,608 | 2017-06-26 | 2026-03-02 |
| **Total** | **2,375** | **47,681** | | |

| Host | Rows |
|---|---:|
| John Oliver | 13,099 |
| Jimmy Fallon | 6,579 |
| Jimmy Kimmel | 5,547 |
| Conan O'Brien | 4,712 |
| Seth Meyers | 3,651 |
| Jay Leno | 3,296 |
| Craig Ferguson | 3,293 |
| Stephen Colbert | 2,410 |
| David Letterman | 1,385 |
| James Corden | 1,020 |
| Jon Stewart | 345 |
| Daily Show | 326 |

Regenerate these tables any time with `monologue stats`.

### The public sample

```text
sample/
  coverage.json          rows per source per month, no joke text
  newsmax/               earliest and latest day: 2009-06-02, 2018-09-28
  latenighter/           earliest and latest day: 2024-02-27, 2026-02-26
  scraps/                earliest and latest day: 2017-06-26, 2026-03-02
```

The sample policy is simple and mechanical:

- **Two day files per source**, the first and the last, each capped at 10 rows.
- **A coverage file with no text**, so depth and continuity can be inspected or charted
  without exposing any jokes.
- **Regenerated after every crawl** with `monologue sample`, so the latest date in
  `sample/` is always the freshness of the full dataset.

### File format

In both the sample and the full dataset, every day file is named `YYYY-MM-DD.csv` and has
the same two columns:

| Column | Meaning |
|---|---|
| `name` | Host or speaker, normalized to one spelling per person |
| `monologue` | One joke or one transcript paragraph |

The full dataset also ships a flattened `monologues.tsv` with `source` and `date` columns in
front, so the whole corpus loads with one call:

```python
import pandas as pd

df = pd.read_csv("monologues.tsv", sep="\t")
df.groupby("name").size().sort_values(ascending=False).head()
```

### Getting the full dataset

The complete text has the same layout as `sample/` plus the flattened TSV. It is available
for research and educational use: contact me. The joke text belongs to the shows and writers
who created it.

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

Every command reads from `--data-dir`, which defaults to `$MONOLOGUE_DATA_DIR` or `./data`.
Point it at the directory holding the full dataset:

```bash
export MONOLOGUE_DATA_DIR=/path/to/full-dataset
```

```bash
monologue stats                              # Markdown tables like the ones above
monologue stats --json                       # machine-readable, with per-source host counts
monologue export -o "$MONOLOGUE_DATA_DIR/monologues.tsv"
monologue export --format jsonl --source scraps -o scraps.jsonl
monologue sample                             # refresh sample/ from the full dataset
```

The commands also work against `sample/` for a quick look at the tooling:

```bash
monologue --data-dir sample stats
```

### Updating the dataset

Each crawler skips days that already exist, so re-running is cheap and safe.

```bash
monologue crawl latenighter --from-date 2026-01-01
monologue crawl scraps --from-date 2026-01-01
monologue crawl newsmax --start-page 1840 --end-page 2100    # archive only; nothing new since 2018
monologue sample
```

The two WordPress sources are filtered by date on the server, so a narrow `--from-date` is a
fast incremental update. Newsmax is walked by page number and stops on its own once pages
start repeating the final day.

Useful flags:

- `--overwrite-existing` rebuilds days after a parser change.
- `monologue crawl scraps --prune-stale` deletes day files a stricter filter no longer produces.
- `--user-agent` overrides the default `monologue-crawler/<version>` header.
- `-v` before the command shows every fetch; `-q` shows only warnings.

After a crawl, commit the refreshed `sample/` here.

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

The test suite covers the three parsers with small HTML fixtures, the crawl loops against a
fake HTTP session (retries, paging, skipping, pruning, stop conditions), the export, stats, and
sample commands, and an integrity pass over `sample/` (date-named, standard header, non-empty,
within the row cap). The same pass runs over the full dataset when `data/` is present.
CI runs everything on Python 3.9 and 3.12.

```text
monologue/
  common.py       text normalization, canonical host names, dates, CSV I/O
  http.py         session with User-Agent, retries with backoff, WordPress paging
  crawl.py        date windows, the day-file store, run summaries
  newsmax.py      page-number crawler and HTML parser
  latenighter.py  WordPress API crawler and round-up parser
  scraps.py       WordPress API crawler and transcript parser
  export.py       TSV / JSONL flattening
  stats.py        dataset summary
  sample.py       public sample and coverage.json generator
  db.py           Postgres loader
  cli.py          argparse entry point
```

## Attribution and licensing

The code in this repository is released under the [MIT License](LICENSE).

The joke text belongs to the shows and writers who created it and was collected from
[Newsmax](https://www.newsmax.com/jokes/), [LateNighter](https://latenighter.com/), and
[Scraps from the Loft](https://scrapsfromtheloft.com/). Only a small sample is published
here; the full dataset is available for research and educational use on request. Please credit
the original hosts and sources if you build on it.
