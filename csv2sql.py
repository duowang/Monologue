import csv
import psycopg2
import os


def csv2sql(filename):
    conn = psycopg2.connect("dbname='monologue' user='dwang' host='localhost'")
    cur = conn.cursor()
    with open("newsmax/" + filename, 'r') as csvfile:
        # sql = "insert into monologue_test values ('{author}', '{date}', '{source}', '{content}'"
        sql = "insert into monologue_test values ($${author}$$, $${date}$$, $${source}$$, $${content}$$)"
        date = filename[:-4]
        reader = csv.DictReader(csvfile)
        for row in reader:
            insert_sql = sql.format(author=row['name'],
                                    date=date,
                                    source="newsmax",
                                    content=row['monologue'].strip())
            try:
                cur.execute(insert_sql)
            except psycopg2.IntegrityError as e:
                print "existed"
                print row['name'], row['monologue']
                conn.commit()
                continue
            conn.commit()
    conn.close()


if __name__ == '__main__':
    for filename in os.listdir("newsmax/"):
        if filename.startswith("2015") or filename.startswith("2016"):
            print filename
            csv2sql(filename)
