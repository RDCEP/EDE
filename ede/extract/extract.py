import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

def return_all_metadata():
    cur.execute("select filename, filesize from grid_meta")
    rows = cur.fetchall()
    for row in rows:
        print row

def main():
    return_all_metadata()

if __name__ == "__main__":
    main()