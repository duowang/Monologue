import csv
import os
from random import shuffle
import psycopg2


def csv2sql(dirname, filename, source_name, connect_str):
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    with open(os.path.join(dirname, filename), "r", encoding="utf-8", newline="") as csvfile:
        sql = (
            "insert into monologue (author, date, source, content) "
            "values (%s, %s, %s, %s)"
        )
        date = filename[:-4]
        rows = list(csv.DictReader(csvfile))
        shuffle(rows)
        for row in rows:
            try:
                cur.execute(
                    sql,
                    (
                        row["name"],
                        date,
                        source_name,
                        row["monologue"].strip(),
                    ),
                )
            except psycopg2.IntegrityError as e:
                conn.rollback()
                print(dirname, filename, "existed")
                print(row['name'], row['monologue'])
                continue
            except Exception:
                conn.rollback()
                print(dirname, filename)
                raise
            conn.commit()
    conn.close()


if __name__ == '__main__':
    user = os.environ.get("MONOLOGUE_DB_USER", "")
    password = os.environ.get("MONOLOGUE_DB_PASSWORD", user)
    dbname = os.environ.get("MONOLOGUE_DB_NAME", user)
    host = os.environ.get("MONOLOGUE_DB_HOST", "localhost")
    connect_str = (
        f"dbname={dbname} user={user} password={password} host='{host}'"
    )

    # csv2sql("newsmax", "2017-07-10.csv", connect_str)
    # raise SystemExit(0)
    source_dirs = {}

    if os.path.isdir("newsmax"):
        year_dirs = []
        for entry in sorted(os.listdir("newsmax")):
            path = os.path.join("newsmax", entry)
            if os.path.isdir(path) and entry.isdigit() and len(entry) == 4:
                year_dirs.append(path)
        source_dirs["newsmax"] = year_dirs + ["newsmax"]

    if os.path.isdir("latenighter"):
        source_dirs["latenighter"] = ["latenighter"]

    if os.path.isdir("scraps"):
        source_dirs["scraps"] = ["scraps"]

    for source_name, directories in source_dirs.items():
        for dirname in directories:
            for filename in sorted(os.listdir(dirname)):
                if filename.endswith(".csv"):
                    csv2sql(dirname, filename, source_name, connect_str)
