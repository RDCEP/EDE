import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

def return_all_metadata():
    cur.execute("select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta")
    rows = cur.fetchall()
    output = []
    for row in rows:
        new_doc = {}
        new_doc['filename'] = row[0]
        new_doc['filesize'] = row[1]
        new_doc['filetype'] = row[2]
        #print type(row[3])
        #print row[3]
        #new_doc['meta_data'] = datetime.strftime(row[3])
        print row[4]
        print type(row[4])
        new_doc['date_created'] = row[4]
        new_doc['date_inserted'] = row[5]
        #print new_doc
        output.append(new_doc)
        #break
    print output


def main():
    return_all_metadata()

if __name__ == "__main__":
    main()