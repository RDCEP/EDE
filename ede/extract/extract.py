import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

# Q0: return metadata of all grid datasets in DB
def return_all_metadata():
    query = "select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    cur.execute(query)
    rows = cur.fetchall()
    output = []
    for row in rows:
        new_doc = {}
        new_doc['filename'] = row[0]
        new_doc['filesize'] = row[1]
        new_doc['filetype'] = row[2]
        new_doc['meta_data'] = row[3]
        new_doc['date_created'] = datetime.strftime(row[4], "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(row[5], "%Y-%m-%d %H:%M:%S")
        output.append(new_doc)
    return output

# Q1: select ( lat, lon, var(t, lat, lon) ) of tiles that intersect with
# some rectangle + time t is fixed (dragging on leaflet/D3 map)
def return_within_rectangle_fixed_time(meta_id, var_id, rec, time):
    poly = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326))" %\
              (rec[0][0], rec[0][1], rec[1][0], rec[1][1], rec[2][0], rec[2][1], rec[3][0], rec[3][1], rec[4][0], rec[4][1])
    query = "SELECT ST_X(geom), ST_Y(geom), val FROM (SELECT (ST_PixelAsCentroids(rast)).* FROM grid_data " \
            "WHERE ST_Intersects(rast, %s and meta_id=%s and var_id=%s and time='%s') foo;" %\
            (poly, meta_id, var_id, time)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print row[0]
        print row[1]


# Q2: select ( lat, lon, var(t, lat, lon) ) with (lat, lon)
# in some region + time t is fixed (on a region- not tile-basis as in 1)
def return_within_region_fixed_time():
    print "todo"


# Q3: compute average of var(t, lat, lon) over (lat,lon) within some
# polygon + time t is fixed (spatial average at some time) aka Zonal Statistics
def return_aggregate_polygon_fixed_time():
    print "todo"


# Q4: compute average of var(t, lat, lon) over t in [t_0, t_1] + (lat, lon)
# within some polygon (temporal average within some region) aka Map Algebra needed here
def return_aggregate_time_within_polygon():
    print "todo"


# Q5: return all bands from a raster
def return_all_frames():
    print "todo"


def main():
    # Q0
    #return_all_metadata()

    # Q1:
    meta_id = 1
    var_id = 1
    rect = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    time = '1999-12-27 00:00:00-06'
    return_within_rectangle_fixed_time(meta_id, var_id, rect, time)

if __name__ == "__main__":
    main()