import csv
import os
from random import shuffle
import psycopg2


def csv2sql(dirname, filename, connect_str):
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    with open(os.path.join(dirname, filename), "r", encoding="utf-8", newline="") as csvfile:
        sql = "insert into monologue (author, date, source, content) values ($${author}$$, $${date}$$, $${source}$$, $${content}$$)"
        date = filename[:-4]
        rows = list(csv.DictReader(csvfile))
        shuffle(rows)
        for row in rows:
            insert_sql = sql.format(author=row['name'],
                                    date=date,
                                    source="newsmax",
                                    content=row['monologue'].strip())
            try:
                cur.execute(insert_sql)
            except psycopg2.IntegrityError as e:
                print(dirname, filename, "existed")
                print(row['name'], row['monologue'])
                conn.commit()
                continue
            except Exception:
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
    year_dirs = []
    for entry in sorted(os.listdir("newsmax")):
        path = os.path.join("newsmax", entry)
        if os.path.isdir(path) and entry.isdigit() and len(entry) == 4:
            year_dirs.append(path)

    for dirname in year_dirs + ["newsmax"]:
        for filename in sorted(os.listdir(dirname)):
            if filename.endswith(".csv"):
                csv2sql(dirname, filename, connect_str)
