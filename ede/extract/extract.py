from datetime import datetime
import time
from ede.database import db_session


def return_gridmeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta where uid in %s" % ids_str
    else:
        query = "select filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    rows = db_session.execute(query)
    # The response JSON
    out = {}
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['filename'] = row[0]
        new_doc['filesize'] = row[1]
        new_doc['filetype'] = row[2]
        new_doc['meta_data'] = row[3]
        new_doc['date_created'] = datetime.strftime(row[4], "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(row[5], "%Y-%m-%d %H:%M:%S")
        out['data'].append(new_doc)
    return out


def return_griddata_select(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s and date=%s) foo;" %\
            (poly_str, meta_id, var_id, date)
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s) foo;" %\
            (poly_str, meta_id, var_id)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s and date=%s) foo;" %\
            (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
            "grid_data where meta_id=%s and var_id=%s) foo;" %\
            (poly_str, meta_id, var_id)
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = { 'type': 'Point', 'coordinates': [lon, lat] }
        new_data_item['properties'] = { 'values': [val] }
        out['data'].append(new_data_item)
    query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    rows = db_session.execute(query)
    for row in rows:
        date_str = str(row[0])
    out['metadata'] = {}
    out['metadata']['dates'] = [date_str]
    return out


def return_griddata_aggregate_spatial(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s and date=%s;" %\
            (poly_str, meta_id, var_id, date)
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s;" %\
            (poly_str, meta_id, var_id)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s and date=%s;" %\
            (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
            "where meta_id=%s and var_id=%s;" %\
            (poly_str, meta_id, var_id)
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
    out = {}
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['metadata'] = {}
    out['data'] = []
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min':min, 'max': max}
    out['response']['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata'].append(date_str)
    return out


def return_griddata_aggregate_temporal(meta_id, var_id, polys, dates):
    """Do temporal aggregation over specific dates & for points within specific polygons.

    If no polygons are passed we default to the entire globe.
    If no dates are passed we default to all dates.

    :param meta_id:
    :param var_id:
    :param polys:
    :param dates:
    :return:
    """
    poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
    date_str = '(' + ','.join(map(str, dates)) + ')'
    #print date_str
    tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
            "where meta_id=%s and var_id=%s and date in %s group by date))" %\
          (poly_str, meta_id, var_id, date_str)
    query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
            "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
            "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # print query
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['metadata'] = {}
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
    query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid in %s" % date_str
    rows = db_session.execute(query)
    out['response']['metadata']['timesteps'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['timesteps'].append(date_str)
    return out