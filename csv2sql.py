import csv
import psycopg2
import os
from random import shuffle


def csv2sql(dirname, filename):
    user = ""
    connect_str = "dbname={user} user={user} password={user} host='localhost'"
    conn = psycopg2.connect(connect_str.format(user=user))
    cur = conn.cursor()
    with open(dirname + "/" + filename , 'r') as csvfile:
        # sql = "insert into monologue_test values ('{author}', '{date}', '{source}', '{content}'"
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
                print dirname, filename, "existed"
                print row['name'], row['monologue']
                conn.commit()
                continue
            except Exception:
                print dirname, filename
                exit
            conn.commit()
    conn.close()


if __name__ == '__main__':
    #csv2sql("newsmax", "2017-07-10.csv")
    #exit()
    for dirname in ["/2009", "/2010", "/2011", "/2012", "/2013", "/2014", "/2015", "/2016", ""]:
        for filename in sorted(os.listdir("newsmax" + dirname)):
            if filename[-4:] == ".csv":
                csv2sql("newsmax" + dirname, filename)
