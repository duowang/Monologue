"""Load the dataset into Postgres (see schema.sql). Requires the optional `db` extra."""

from __future__ import annotations

import os
import random
from pathlib import Path

from monologue.common import iter_rows

INSERT_SQL = (
    "INSERT INTO monologue (author, date, source, content) VALUES (%s, %s, %s, %s) "
    "ON CONFLICT (content) DO NOTHING"
)
BATCH_SIZE = 500


def connection_string() -> str:
    user = os.environ.get("MONOLOGUE_DB_USER", "")
    password = os.environ.get("MONOLOGUE_DB_PASSWORD", user)
    dbname = os.environ.get("MONOLOGUE_DB_NAME", user)
    host = os.environ.get("MONOLOGUE_DB_HOST", "localhost")
    return f"dbname={dbname} user={user} password={password} host={host}"


def add_arguments(parser) -> None:
    parser.add_argument(
        "--dsn", default=None, help="libpq connection string (default: MONOLOGUE_DB_* env vars)."
    )
    parser.add_argument("--no-shuffle", action="store_true", help="Insert in file order instead of shuffled.")


def run(args) -> int:
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise SystemExit("psycopg2 is not installed; run: pip install 'monologue[db]'") from exc

    rows = [(r.author, r.date, r.source, r.text) for r in iter_rows(Path(args.data_dir))]
    if not args.no_shuffle:
        random.shuffle(rows)

    inserted = 0
    with psycopg2.connect(args.dsn or connection_string()) as conn, conn.cursor() as cur:
        for start in range(0, len(rows), BATCH_SIZE):
            cur.executemany(INSERT_SQL, rows[start : start + BATCH_SIZE])
            inserted += cur.rowcount if cur.rowcount >= 0 else 0
            conn.commit()
    print(f"Processed {len(rows)} rows; inserted {inserted} new rows.")
    return 0
