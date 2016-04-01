from datetime import datetime
import time
from ede.database import db_session


# Q0: return metadata of all grid datasets in DB
def return_all_metadata():
    query = "select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
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
def return_tiles_within_region_fixed_time(meta_id, var_id, poly, date):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326))" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "SELECT ST_X(geom), ST_Y(geom), val FROM (SELECT (ST_PixelAsCentroids(rast)).* FROM grid_data " \
            "WHERE ST_Intersects(rast, %s and meta_id=%s and var_id=%s and date=%s) foo;" %\
            (poly_str, meta_id, var_id, date)
    print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
    out['response']['metadata']['region'] = poly
    out['response']['metadata']['units'] = 'TKTK'
    out['response']['metadata']['format'] = 'grid'
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = { 'type': 'Point', 'coordinates': [lon, lat] }
        new_data_item['properties'] = { 'values': [val] }
        out['response']['data'].append(new_data_item)
    query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    rows = db_session.execute(query)
    for row in rows:
        date_str = row
    print date_str
    out['response']['metadata']['timesteps'] = [date_str]
    return out


# Q2: select ( lat, lon, var(t, lat, lon) ) with (lat, lon)
# in some region + time t is fixed (on a region- not tile-basis as in 1)
def return_within_region_fixed_time(meta_id, var_id, poly, date):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s and date=%s) foo;" %\
            (poly_str, meta_id, var_id, date)
    print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
    out['response']['metadata']['timesteps'] = [date]
    out['response']['metadata']['region'] = poly
    out['response']['metadata']['units'] = 'TKTK'
    out['response']['metadata']['format'] = 'grid'
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = { 'type': 'Point', 'coordinates': [lon, lat] }
        new_data_item['properties'] = { 'values': [val] }
        out['response']['data'].append(new_data_item)
    return out


# Q3: compute average of var(t, lat, lon) over (lat,lon) within some
# polygon + time t is fixed (spatial average at some time) aka Zonal Statistics
def return_aggregate_polygon_fixed_time(meta_id, var_id, poly, date):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s and date=%s;" %\
            (poly_str, meta_id, var_id, date)
    print query
    rows = db_session.execute(query)
    for row in rows:
        res = row[0].lstrip('(').rstrip(')').split(',')
    count = int(res[0])
    sum = float(res[1])
    mean = float(res[2])
    stddev = float(res[3])
    min = float(res[4])
    max = float(res[5])
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
    out['response']['metadata']['timesteps'] = [date]
    out['response']['metadata']['units'] = 'TKTK'
    out['response']['metadata']['format'] = 'polygon'
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = date.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min':min, 'max': max}
    out['response']['data'].append(new_data_item)
    return out


# Q4: compute average of var(t, lat, lon) over t in [t_0, t_1] + (lat, lon)
# within some polygon (temporal average within some region) aka Map Algebra needed here
def return_aggregate_time_within_polygon(meta_id, var_id, poly, start_date, end_date):
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
            "where meta_id=%s and var_id=%s and date>=%s and date<=%s group by date))" %\
          (poly_str, meta_id, var_id, start_date, end_date)
    query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
            "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
            "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
    out['response']['metadata']['timesteps'] = [start_date, end_date]
    out['response']['metadata']['units'] = 'TKTK'
    out['response']['metadata']['format'] = 'grid'
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['response']['data'].append(new_data_item)
    return out


# Q5: return all time frames from a raster
def return_all_frames(meta_id, var_id):
    tmp = "with foo as (select st_astext((ST_PixelAsCentroids(rast)).geom) as pos, time, " \
            "(ST_PixelAsCentroids(rast)).val as val from grid_data where meta_id=%s and var_id=%s)" %\
          (meta_id, var_id)
    query = tmp + '\n' + "select ST_X(pos), ST_Y(pos), array_to_json(array_agg((time, val))) from foo group by foo.pos;"
    print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
    out['response']['metadata']['timesteps'] = []
    out['response']['metadata']['units'] = 'TKTK'
    out['response']['metadata']['format'] = 'grid'
    out['response']['data'] = []
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['request']['url'] = '/api/v0'
    for row in rows:
        lon = row[0]
        lat = row[1]
        vals = row[2] # list of elems with elem = { 'f1': date as string, 'f2': value as float }
        new_meta_item = [ v['f1'] for v in vals ]
        out['response']['metadata']['timesteps'].append(new_meta_item)
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [ v['f2'] for v in vals]}
        out['response']['data'].append(new_data_item)
    return out


def main():
    # Q0
    print "Testing Q0..."
    #print return_all_metadata()

    # Q1
    print "Testing Q1..."
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    date = 1
    print return_tiles_within_region_fixed_time(meta_id, var_id, poly, date)

    # Q2
    print "Testing Q2..."
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    date = 1
    #print return_within_region_fixed_time(meta_id, var_id, poly, date)

    # Q3
    print "Testing Q3..."
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    date = 1
    #print return_aggregate_polygon_fixed_time(meta_id, var_id, poly, date)

    # Q4
    print "Testing Q4..."
    meta_id = 1
    var_id = 1
    poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    start_date = 1
    end_date = 2
    #print return_aggregate_time_within_polygon(meta_id, var_id, poly, start_date, end_date)

    # Q5
    print "Testing Q5..."
    meta_id = 1
    var_id = 1
    # print return_all_frames(meta_id, var_id)

if __name__ == "__main__":
    main()