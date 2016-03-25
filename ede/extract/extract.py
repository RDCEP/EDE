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
def return_tiles_within_region_fixed_time(meta_id, var_id, poly, time):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326))" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "SELECT ST_X(geom), ST_Y(geom), val FROM (SELECT (ST_PixelAsCentroids(rast)).* FROM grid_data " \
            "WHERE ST_Intersects(rast, %s and meta_id=%s and var_id=%s and time='%s') foo;" %\
            (poly_str, meta_id, var_id, time)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]


# Q2: select ( lat, lon, var(t, lat, lon) ) with (lat, lon)
# in some region + time t is fixed (on a region- not tile-basis as in 1)
def return_within_region_fixed_time(meta_id, var_id, poly, time):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s and time='%s') foo;" %\
            (poly_str, meta_id, var_id, time)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]


# Q3: compute average of var(t, lat, lon) over (lat,lon) within some
# polygon + time t is fixed (spatial average at some time) aka Zonal Statistics
def return_aggregate_polygon_fixed_time(meta_id, var_id, poly, time):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s and time='%s';" %\
            (poly_str, meta_id, var_id, time)
    cur.execute(query)
    rows = cur.fetchall()
    res = rows[0][0]
    print res
    count = res[0]
    sum = res[1]
    avg = res[2]
    stddev = res[3]
    min = res[4]
    max = res[5]
    print count, sum, avg, stddev, min, max

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

    # Q1
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    time = '1999-12-27 00:00:00-06'
    #return_within_polygon_fixed_time(meta_id, var_id, poly, time)

    # Q2
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    time = '1999-12-27 00:00:00-06'
    #return_within_region_fixed_time(meta_id, var_id, poly, time)

    # Q3
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    time = '1999-12-27 00:00:00-06'
    return_aggregate_polygon_fixed_time(meta_id, var_id, poly, time)

    # Q4


if __name__ == "__main__":
    main()