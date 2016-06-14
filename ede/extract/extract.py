from __future__ import print_function
import sys
from datetime import datetime
import time
from ede.database import db_session
from sqlalchemy.exc import SQLAlchemyError


class RasterExtractionException(Exception):
    """Represents an exception that can occur during the extraction of raster data from the DB.
    """

    def __init__(self, message):
        super(RasterExtractionException, self).__init__(message)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def return_gridmeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = ("select uid, filename, filesize, filetype, meta_data, date_created, date_inserted "
                 "from grid_meta where uid in {}".format(ids_str))
    else:
        query = "select uid, filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("Could not return gridmeta with ids: {}".format(ids))

    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for (uid, filename, filesize, filetype, meta_data, date_created, date_inserted) in rows:
        new_doc = {}
        new_doc['uid'] = uid
        new_doc['filename'] = filename
        new_doc['filesize'] = filesize
        new_doc['filetype'] = filetype
        new_doc['meta_data'] = meta_data
        new_doc['date_created'] = datetime.strftime(date_created, "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(date_inserted, "%Y-%m-%d %H:%M:%S")
        out['response']['data'].append(new_doc)
    return out


def return_griddata_datasetid_varid_polyid_timeid(dataset_id, var_id, poly_id, time_id):
    pass


def return_griddata_datasetid_varid_poly_timeid(dataset_id, var_id, poly, time_id):
    pass


def return_griddata_datasetid_varid_timeid(dataset_id, var_id, time_id):
    query = ("SELECT rast from grid_data_psims_time_lat_lon where dataset_id={} and var_id={} and time_id={};".
             format(dataset_id, var_id, time_id))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("") # TODO: improve msg
    for row in rows:
        return row[0] # the rast field


def return_griddata_by_id(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
                "from (select (ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).* from " \
                "grid_data as gd, regions as r where gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date=%s) foo;" % \
                (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
                "from (select (ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).* from " \
                "grid_data as gd, regions as r where gd.meta_id=%s and gd.var_id=%s and r.uid=%s) foo;" % \
                (meta_id, var_id, poly)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
                "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
                "grid_data where meta_id=%s and var_id=%s and date=%s) foo;" % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "SELECT ST_X(geom), ST_Y(geom), val " \
                "from (select (ST_PixelAsCentroids(ST_Clip(rast, %s, TRUE))).* from " \
                "grid_data where meta_id=%s and var_id=%s) foo;" % \
                (poly_str, meta_id, var_id)
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['response']['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['dates'].append(date_str)
    out['response']['metadata']['region'] = poly
    query = "select vname from grid_vars where uid=%s" % var_id
    rows = db_session.execute(query)
    for row in rows:
        vname = row[0]
    query = "select meta_data from grid_meta where uid=%s" % meta_id
    rows = db_session.execute(query)
    for row in rows:
        meta_data = row[0]
    for var in meta_data['variables']:
        if var['name'] == vname:
            for attr in var['attributes']:
                if attr['name'] == 'units':
                    units = attr['value']
    out['response']['metadata']['units'] = units
    out['response']['metadata']['format'] = 'grid'
    return out


def return_griddata_aggregate_spatial(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s and date=%s;" % \
                (poly_str, meta_id, var_id, date)
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s;" % \
                (poly_str, meta_id, var_id)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s and date=%s;" % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s;" % \
                (poly_str, meta_id, var_id)
    # print query
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
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min': min, 'max': max}
    out['response']['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['dates'].append(date_str)
    out['response']['metadata']['region'] = poly
    query = "select vname from grid_vars where uid=%s" % var_id
    rows = db_session.execute(query)
    for row in rows:
        vname = row[0]
    query = "select meta_data from grid_meta where uid=%s" % meta_id
    rows = db_session.execute(query)
    for row in rows:
        meta_data = row[0]
    for var in meta_data['variables']:
        if var['name'] == vname:
            for attr in var['attributes']:
                if attr['name'] == 'units':
                    units = attr['value']
    out['response']['metadata']['units'] = units
    out['response']['metadata']['format'] = 'polygon'
    return out


def return_griddata_aggregate_spatial_by_id(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, true))) from grid_data as gd, regions as r " \
                "where gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date=%s;" % \
                (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, true))) from grid_data as gd, regions as r " \
                "where gd.meta_id=%s and gd.var_id=%s and r.uid=%s;" % \
                (meta_id, var_id, poly)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s and date=%s;" % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = "select ST_SummaryStats(ST_Union(ST_Clip(rast, %s, true))) from grid_data " \
                "where meta_id=%s and var_id=%s;" % \
                (poly_str, meta_id, var_id)
    # print query
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
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min': min, 'max': max}
    out['response']['data'].append(new_data_item)
    if date:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid=%s" % (date)
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['dates'].append(date_str)
    out['response']['metadata']['region'] = poly
    query = "select vname from grid_vars where uid=%s" % var_id
    rows = db_session.execute(query)
    for row in rows:
        vname = row[0]
    query = "select meta_data from grid_meta where uid=%s" % meta_id
    rows = db_session.execute(query)
    for row in rows:
        meta_data = row[0]
    for var in meta_data['variables']:
        if var['name'] == vname:
            for attr in var['attributes']:
                if attr['name'] == 'units':
                    units = attr['value']
    out['response']['metadata']['units'] = units
    out['response']['metadata']['format'] = 'polygon'
    return out


def return_griddata_aggregate_temporal(meta_id, var_id, poly, dates):
    # poly + dates specified
    if poly and dates:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s and date in %s group by date))" % \
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s group by date))" % \
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only dates specified
    elif dates:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s and date in %s group by date))" % \
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # neither poly nor dates specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s group by date))" % \
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['response']['data'].append(new_data_item)
    if dates:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid in %s" % date_str
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['dates'].append(date_str)
    out['response']['metadata']['region'] = poly
    query = "select vname from grid_vars where uid=%s" % var_id
    rows = db_session.execute(query)
    for row in rows:
        vname = row[0]
    query = "select meta_data from grid_meta where uid=%s" % meta_id
    rows = db_session.execute(query)
    for row in rows:
        meta_data = row[0]
    for var in meta_data['variables']:
        if var['name'] == vname:
            for attr in var['attributes']:
                if attr['name'] == 'units':
                    units = attr['value']
    out['response']['metadata']['units'] = units
    out['response']['metadata']['format'] = 'grid'
    return out


def return_griddata_aggregate_temporal_by_id(meta_id, var_id, poly, dates):
    # poly + dates specified
    if poly and dates:
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(gd.rast, r.geom)), 1)::rastbandarg as rast " \
              "from grid_data as gd, regions as r where " \
              "gd.meta_id=%s and gd.var_id=%s and r.uid=%s and gd.date in %s group by date))" % \
              (meta_id, var_id, poly, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only poly specified
    elif poly:
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(gd.rast, r.geom)), 1)::rastbandarg as rast " \
              "from grid_data as gd, regions as r where " \
              "gd.meta_id=%s and gd.var_id=%s and r.uid=%s group by date))" % \
              (meta_id, var_id, poly)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # only dates specified
    elif dates:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        date_str = '(' + ','.join(map(str, dates)) + ')'
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s and date in %s group by date))" % \
              (poly_str, meta_id, var_id, date_str)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    # neither poly nor dates specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        tmp = "with foo as (select array(select ROW(ST_Union(ST_Clip(rast, %s)), 1)::rastbandarg as rast from grid_data " \
              "where meta_id=%s and var_id=%s group by date))" % \
              (poly_str, meta_id, var_id)
        query = tmp + '\n' + "SELECT ST_X(geom), ST_Y(geom), val FROM " \
                             "(select (ST_PixelAsCentroids(ST_MapAlgebra((select * from foo)::rastbandarg[], " \
                             "'st_stddev4ma(double precision[], int[], text[])'::regprocedure))).*) foo;"
    rows = db_session.execute(query)
    # the response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        lon = row[0]
        lat = row[1]
        val = row[2]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': [val]}
        out['response']['data'].append(new_data_item)
    if dates:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates where uid in %s" % date_str
    else:
        query = "select to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
    rows = db_session.execute(query)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = []
    for row in rows:
        date_str = str(row[0])
        out['response']['metadata']['dates'].append(date_str)
    out['response']['metadata']['region'] = poly
    query = "select vname from grid_vars where uid=%s" % var_id
    rows = db_session.execute(query)
    for row in rows:
        vname = row[0]
    query = "select meta_data from grid_meta where uid=%s" % meta_id
    rows = db_session.execute(query)
    for row in rows:
        meta_data = row[0]
    for var in meta_data['variables']:
        if var['name'] == vname:
            for attr in var['attributes']:
                if attr['name'] == 'units':
                    units = attr['value']
    out['response']['metadata']['units'] = units
    out['response']['metadata']['format'] = 'grid'
    return out


def return_polymeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select uid, name, attributes from regions_meta where uid in {}".format(ids_str)
    else:
        query = "select uid, name, attributes from regions_meta"
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("Could not return polymeta with ids: {}".format(ids))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for (uid, name, attributes) in rows:
        new_doc = {}
        new_doc['uid'] = uid
        new_doc['name'] = name
        new_doc['attributes'] = attributes
        out['response']['data'].append(new_doc)
    return out


def return_polydata(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select uid, ST_AsGeoJSON(geom) from regions where uid in %s".format(ids_str)
    else:
        query = "select uid, ST_AsGeoJSON(geom) from regions"
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("Could not return polydata with ids: {}".format(ids))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for (uid, geo) in rows:
        new_doc = {}
        new_doc['uid'] = uid
        new_doc['geo'] = geo
        out['response']['data'].append(new_doc)
    return out
