import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime
import time

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
cur = conn.cursor()


# Q0: return metadata of all grid datasets in DB
def return_all_metadata():
    query = "select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    cur.execute(query)
    rows = cur.fetchall()
    # the response JSON
    out = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    out['request']['accuracy'] = 2
    out['request']['geo_accuracy'] = 4
    for row in rows:
        new_doc = {}
        new_doc['filename'] = row[0]
        new_doc['filesize'] = row[1]
        new_doc['filetype'] = row[2]
        new_doc['meta_data'] = row[3]
        new_doc['date_created'] = datetime.strftime(row[4], "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(row[5], "%Y-%m-%d %H:%M:%S")
        out['response']['data'].append(new_doc)

    return out


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
    res = rows[0][0].split(',')
    count = res[0]
    sum = res[1]
    mean = res[2]
    stddev = res[3]
    min = res[4]
    max = res[5]


# Q4: compute average of var(t, lat, lon) over t in [t_0, t_1] + (lat, lon)
# within some polygon (temporal average within some region) aka Map Algebra needed here
def return_aggregate_time_within_polygon(meta_id, var_id, poly, start_time, end_time):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
            "where meta_id=%s and var_id=%s and time>='%s' and time<='%s' group by time))" %\
          (poly_str, meta_id, var_id, start_time, end_time)
    query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
            "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
            "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        print lon, lat, val


# Q5: return all time frames from a raster
def return_all_frames(meta_id, var_id):
    tmp = "with foo as (select st_astext((ST_PixelAsCentroids(rast)).geom) as pos, time, " \
            "(ST_PixelAsCentroids(rast)).val as val from grid_data where meta_id=%s and var_id=%s)" %\
          (meta_id, var_id)
    query = tmp + '\n' + "select ST_X(pos), ST_Y(pos), array_to_json(array_agg((time, val))) from foo group by foo.pos;"
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        lon = row[0]
        lat = row[1]
        vals = row[2] # list of elems with elem = { 'f1': date as string, 'f2': value as float }


def main():
    # Q0
    print return_all_metadata()

    # Q1
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    time = '1999-12-27 00:00:00-06'
    #print return_within_polygon_fixed_time(meta_id, var_id, poly, time)

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
    #return_aggregate_polygon_fixed_time(meta_id, var_id, poly, time)

    # Q4
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    start_time = '1999-12-27 00:00:00-06'
    end_time = '2016-12-27 00:00:00-06'
    #return_aggregate_time_within_polygon(meta_id, var_id, poly, start_time, end_time)

    # Q5
    meta_id = 1
    var_id = 1
    #return_all_frames(meta_id, var_id)

if __name__ == "__main__":
    main()