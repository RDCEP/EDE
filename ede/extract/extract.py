from datetime import datetime
import time
from ede.database import db_session
from ede.extract.sql import *


def return_gridmeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = GRID_META_BY_UID.format(ids_str)
    else:
        query = GRID_META
    rows = db_session.execute(query)
    # The response JSON
    out = dict(data=list(), metadata=dict())
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['filename'] = row[1]
        new_doc['filesize'] = row[2]
        new_doc['filetype'] = row[3]
        new_doc['meta_data'] = row[4]
        new_doc['date_created'] = datetime.strftime(row[5], "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(row[6], "%Y-%m-%d %H:%M:%S")
        out['data'].append(new_doc)
    return out


def make_poly_str(poly):
    return MAKE_POLY.format(
        poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0],
        poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])


def return_griddata(meta_id, var_id, poly, date):
    if poly:
        if date:
            # poly + date specified
            query = GRID_DATA_BY_DATE.format(
                make_poly_str(poly), meta_id, var_id, date)
        else:
            # only poly specified
            query = GRID_DATA.format(
                make_poly_str(poly), meta_id, var_id)
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        # neither poly nor date specified
        query = GRID_DATA.format(make_poly_str(poly), meta_id, var_id)
        if date:
            # only date specified
            query = GRID_DATA_BY_DATE.format(
                make_poly_str(poly), meta_id, var_id, date)

    rows = db_session.execute(query)
    # the response JSON
    out = dict(data=list(), metadata=dict())
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = { 'type': 'Point', 'coordinates': [lon, lat] }
        new_data_item['properties'] = { 'values': [val] }
        out['data'].append(new_data_item)
    if date:
        query = DATE_BY_ID.format(date)
    else:
        query = DATE
    rows = db_session.execute(query)
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
    return out


def return_griddata_by_id(meta_id, var_id, poly, date):
    query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).* from " \
            "grid_data as gd, regions as r where gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date=%s) foo;" %\
            (meta_id, var_id, poly, date)
    rows = db_session.execute(query)
    for row in rows:
        print row

    # poly + date specified
    if poly and date:
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).* from " \
            "grid_data as gd, regions as r where gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date=%s) foo;" %\
            (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
            "from (select (ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).* from " \
            "grid_data as gd, regions as r where gd.meta_id=%s and gd.var_id=%s and r.uid=%s) foo;" %\
            (meta_id, var_id, poly)
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
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = {}
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
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
    #print query
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
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min':min, 'max': max}
    out['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = {}
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
    return out


def return_griddata_aggregate_spatial_by_id(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, true))) from grid_data as gd, regions as r " \
                "where gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date=%s;" %\
                (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, true))) from grid_data as gd, regions as r " \
                "where gd.meta_id=%s and gd.var_id=%s and r.uid=%s;" %\
                (meta_id, var_id, poly)
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
    #print query
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
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min':min, 'max': max}
    out['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = {}
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
    return out


def return_griddata_aggregate_temporal(meta_id, var_id, poly, dates):
    # poly + dates specified
    if poly and dates:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s and date in %s group by date))" %\
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s group by date))" %\
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only dates specified
    elif dates:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s and date in %s group by date))" %\
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # neither poly nor dates specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s group by date))" %\
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
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
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['data'].append(new_data_item)
    if dates:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid in %s" % date_str
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = {}
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
    return out


def return_griddata_aggregate_temporal_by_id(meta_id, var_id, poly, dates):
    # poly + dates specified
    if poly and dates:
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp =   "with foo as (select array(select ROW(ST_Union(ST_Clip(gd.rast, r.geom)), 1)::rastbandarg as rast " \
                "from grid_data as gd, regions as r where " \
                "gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date in %s group by date))" %\
                (meta_id, var_id, poly, date_str)
        query = tmp + '\n' +    "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only poly specified
    elif poly:
        tmp =   "with foo as (select array(select ROW(ST_Union(ST_Clip(gd.rast, r.geom)), 1)::rastbandarg as rast " \
                "from grid_data as gd, regions as r where " \
                "gd.meta_id=%s and gd.var_id=%s and r.uid=%s group by date))" %\
                (meta_id, var_id, poly)
        query = tmp + '\n' +    "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only dates specified
    elif dates:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s and date in %s group by date))" %\
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # neither poly nor dates specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" %\
              (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1], poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
                "where meta_id=%s and var_id=%s group by date))" %\
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    print query
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
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['data'].append(new_data_item)
    if dates:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid in %s" % date_str
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['metadata'] = {}
    out['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['metadata']['dates'].append(date_str)
    return out


def return_polymeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select uid, name, attributes from regions_meta where uid in %s" % ids_str
    else:
        query = "select uid, name, attributes from regions_meta"
    rows = db_session.execute(query)
    # The response JSON
    out = {}
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['name'] = row[1]
        new_doc['attributes'] = row[1]
        out['data'].append(new_doc)
    return out


def return_polydata(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select uid, ST_AsGeoJSON(geom) from regions where uid in %s" % ids_str
    else:
        query = "select uid, ST_AsGeoJSON(geom) from regions"
    rows = db_session.execute(query)
    # The response JSON
    out = {}
    out['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['status'] = 'OK'
    out['status_code'] = 200
    out['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['geo'] = row[1]
        out['data'].append(new_doc)
    return out