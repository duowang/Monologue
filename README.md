# Monologue Dataset (Newsmax)

## Repository structure

- `newsmax_crawler.py`: crawls Newsmax "Best of Late Nite Jokes" pages and writes date-based CSV files.
- `newsmax/`: raw CSV files (`YYYY-MM-DD.csv`) grouped by year folders for 2009-2016 and root-level files for 2017.
- `csv2sql.py`: imports CSV rows into Postgres.
- `schema.sql`: table definitions.

## Crawl continuation

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Continue crawling with auto-detected upper bound:

```bash
python3 newsmax_crawler.py \
  --start-page 1840 \
  --auto-end \
  --stop-after-same-date 8 \
  --stop-after-miss 100 \
  --skip-existing
```

Notes:

- `--auto-end` tries to discover the latest available jokes page from `/jokes/archive/`.
- `--skip-existing` prevents rewriting dates that are already present.
- `--stop-after-same-date` prevents endless loops when many page ids resolve to the same last date.
- If Newsmax has stopped publishing this content, the crawler exits after consecutive misses.
