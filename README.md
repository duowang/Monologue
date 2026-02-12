# Monologue Dataset (Newsmax)

## Repository structure

- `newsmax_crawler.py`: crawls Newsmax "Best of Late Nite Jokes" pages and writes date-based CSV files.
- `latenighter_crawler.py`: crawls LateNighter "Monologues Round-Up" posts and writes date-based CSV files.
- `scraps_crawler.py`: crawls transcript posts from scrapsfromtheloft.com and writes date-based CSV files.
- `newsmax/`: raw CSV files (`YYYY-MM-DD.csv`) grouped by year folders for 2009-2016 and root-level files for 2017.
- `latenighter/`: raw CSV files (`YYYY-MM-DD.csv`) from LateNighter (current coverage starts in 2024).
- `scraps/`: raw CSV files (`YYYY-MM-DD.csv`) from scrapsfromtheloft.com transcript pages.
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

## Post-2018 continuation

Newsmax `jokes` pages currently top out at `2018-09-28` content. To continue collecting newer monologue jokes:

```bash
python3 latenighter_crawler.py \
  --from-date 2018-09-29 \
  --skip-existing
```

Alternative source crawl (transcript-heavy archive, broad 2017+ coverage):

```bash
python3 scraps_crawler.py \
  --from-date 2017-01-01 \
  --skip-existing
```

If filters change and you need to clean stale CSVs:

```bash
python3 scraps_crawler.py \
  --from-date 2017-01-01 \
  --overwrite-existing \
  --prune-stale
```
