from datetime import datetime
import time
from ede.database import db_session


def return_gridmeta(ids):
    if ids:
        ids_str = '(' + ','.join(map(str, ids)) + ')'
        query = "select uid, filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta where uid in %s" % ids_str
    else:
        query = "select uid, filename, filesize, filetype, meta_data, date_created, date_inserted from grid_meta"
    rows = db_session.execute(query)
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['filename'] = row[1]
        new_doc['filesize'] = row[2]
        new_doc['filetype'] = row[3]
        new_doc['meta_data'] = row[4]
        new_doc['date_created'] = datetime.strftime(row[5], "%Y-%m-%d %H:%M:%S")
        new_doc['date_inserted'] = datetime.strftime(row[6], "%Y-%m-%d %H:%M:%S")
        out['response']['data'].append(new_doc)
    return out


def return_griddata(meta_id, var_id, poly, dates):
    if dates:
        date_str = '(' + ','.join(map(str, dates)) + ')'
        query = ("select to_char(date, 'YYYY-MM-DD HH24:MI:SS') from "
                 "grid_dates where uid in {} order by date").format(date_str)
    else:
        query = ("select to_char(date, 'YYYY-MM-DD HH24:MI:SS') from "
                 "grid_dates where meta_id={} order by date").format(meta_id)
    rows = db_session.execute(query)
    dates_array = []
    num_dates = 0
    for row in rows:
        date_str = str(row[0])
        dates_array.append(date_str)
        num_dates += 1
    # poly + date specified
    if poly and dates:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)".format(
            (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1],
             poly[3][0], poly[3][1], poly[4][0], poly[4][1]))
        query = ("select ST_X(geom), ST_Y(geom), array_agg(date_id || ';' || val) "
                 "from (select(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*, date_id "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id={} and grid_data.var_id={} and "
                 "grid_dates.uid = grid_data.date and grid_dates.uid in {}) foo "
                 "group by foo.geom;").format((poly_str, meta_id, var_id, date_str))
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)".format(
            (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1],
             poly[3][0], poly[3][1], poly[4][0], poly[4][1]))
        query = ("select ST_X(geom), ST_Y(geom), array_agg(date_id || ';' || val) "
                 "from (select(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*, date_id "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id={} and grid_data.var_id={} and "
                 "grid_dates.uid = grid_data.date) foo "
                 "group by foo.geom;").format(poly_str, meta_id, var_id)
    # only date specified
    elif dates:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)".format(
            (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1],
             poly[3][0], poly[3][1], poly[4][0], poly[4][1]))
        query = ("select ST_X(geom), ST_Y(geom), array_agg(date_id || ';' || val) "
                 "from (select(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*, date_id "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id={} and grid_data.var_id={} and "
                 "grid_dates.uid = grid_data.date and grid_dates.uid in {}) foo "
                 "group by foo.geom;").format(poly_str, meta_id, var_id, date_str)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)".format(
            (poly[0][0], poly[0][1], poly[1][0], poly[1][1],
             poly[2][0], poly[2][1], poly[3][0], poly[3][1],
             poly[4][0], poly[4][1]))
        query = ("select ST_X(geom), ST_Y(geom), array_agg(date_id || ';' || val) "
                 "from (select(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*, date_id "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id={} and grid_data.var_id={} and "
                 "grid_dates.uid = grid_data.date) foo "
                 "group by foo.geom;").format(poly_str, meta_id, var_id)
    print(query)
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
    for (lon, lat, dateids_vals) in rows:
        values = num_dates * [None]
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        dateids_vals = dateids_vals.lstrip('{').rstrip('}')
        dateids_vals = dateids_vals.split(',')
        dateids_vals = [e.split(';') for e in dateids_vals]
        dateids_vals = [(int(date_id), float(val)) for (date_id, val) in dateids_vals]
        for (date_id, val) in dateids_vals:
            values[date_id-1] = val
        new_data_item['properties'] = {'values': values}
        out['response']['data'].append(new_data_item)
    out['response']['metadata'] = {}
    out['response']['metadata']['dates'] = dates_array
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


def return_griddata_by_id(meta_id, var_id, poly, date):
    # poly + date specified
    if poly and date:
        query = ("select ST_X(geom), ST_Y(geom), array_agg(val), array_agg(date) from (select(ST_PixelAsCentroids("
                 "ST_Clip(rast, r.geom, TRUE))).*, to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') as date "
                 "from grid_data, grid_dates, regions"
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and "
                 "grid_dates.uid = grid_data.date and regions.uid=%s and grid_data.date=%s) foo "
                 "group by foo.geom;") % \
                (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = ("select ST_X(geom), ST_Y(geom), array_agg(val), array_agg(date) from (select(ST_PixelAsCentroids("
                 "ST_Clip(rast, r.geom, TRUE))).*, to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') as date "
                 "from grid_data, grid_dates, regions"
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and "
                 "grid_dates.uid = grid_data.date and regions.uid=%s) foo "
                 "group by foo.geom;") % \
                (meta_id, var_id, poly)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_X(geom), ST_Y(geom), array_agg(val), array_agg(date) from (select(ST_PixelAsCentroids("
                 "ST_Clip(rast, %s, TRUE))).*, to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') as date "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and "
                 "grid_dates.uid = grid_data.date and grid_data.date=%s) foo "
                 "group by foo.geom;") % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_X(geom), ST_Y(geom), array_agg(val), array_agg(date) from (select(ST_PixelAsCentroids("
                 "ST_Clip(rast, %s, TRUE))).*, to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') as date "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and "
                 "grid_dates.uid = grid_data.date) foo "
                 "group by foo.geom;") % \
                (poly_str, meta_id, var_id)
    print(query)
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
    for (lon, lat, vals, dates) in rows:
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
        new_data_item['properties'] = {'values': vals}
        new_data_item['dates'] = dates
        out['response']['data'].append(new_data_item)
    out['response']['metadata'] = {}
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
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s "
                 "and grid_data.date=grid_dates.uid and grid_data.date=%s") % \
                (poly_str, meta_id, var_id, date)
    # only poly specified
    elif poly:
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and grid_data.date=grid_dates.uid "
                 "group by grid_dates.date;") % \
                (poly_str, meta_id, var_id)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s "
                 "and grid_data.date=grid_dates.uid and grid_data.date=%s") % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and grid_data.date=grid_dates.uid "
                 "group by grid_dates.date;") % \
                (poly_str, meta_id, var_id)
    print(query)
    rows = db_session.execute(query)
    # the response json
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for (stats, date) in rows:
        res = stats.lstrip('(').rstrip(')').split(',')
        count = int(res[0])
        sum = float(res[1])
        mean = float(res[2])
        stddev = float(res[3])
        min = float(res[4])
        max = float(res[5])
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
        new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean,
                                       'stddev': stddev, 'min': min, 'max': max}
        new_data_item['date'] = date
        out['response']['data'].append(new_data_item)
    out['response']['metadata'] = {}
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
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, regions.geom, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates, regions "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s "
                 "and grid_data.date=grid_dates.uid and regions.uid=%s and grid_data.date=%s") % \
                (meta_id, var_id, poly, date)
    # only poly specified
    elif poly:
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, regions.geom, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates, regions "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and regions.uid=%s "
                 "and grid_data.date=grid_dates.uid "
                 "group by grid_dates.date;") % \
                (meta_id, var_id, poly)
    # only date specified
    elif date:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s "
                 "and grid_data.date=grid_dates.uid and grid_data.date=%s") % \
                (poly_str, meta_id, var_id, date)
    # neither poly nor date specified
    else:
        poly = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        poly_str = "ST_Polygon(ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s %s)'), 4326)" % \
                   (poly[0][0], poly[0][1], poly[1][0], poly[1][1], poly[2][0], poly[2][1], poly[3][0], poly[3][1],
                    poly[4][0], poly[4][1])
        query = ("select ST_SummaryStats(ST_Union(ST_Clip(grid_data.rast, %s, true))), "
                 "to_char(grid_dates.date, 'YYYY-MM-DD HH24:MI:SS') "
                 "from grid_data, grid_dates "
                 "where grid_data.meta_id=%s and grid_data.var_id=%s and grid_data.date=grid_dates.uid "
                 "group by grid_dates.date;") % \
                (poly_str, meta_id, var_id)
    print(query)
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
    for (stats, date) in rows:
        res = stats.lstrip('(').rstrip(')').split(',')
        count = int(res[0])
        sum = float(res[1])
        mean = float(res[2])
        stddev = float(res[3])
        min = float(res[4])
        max = float(res[5])
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
        new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean,
                                       'stddev': stddev, 'min': min, 'max': max}
        new_data_item['date'] = date
        out['response']['data'].append(new_data_item)
    out['response']['metadata'] = {}
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
    for (lon, lat, val) in rows:
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
    for (lon, lat, val) in rows:
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
        query = "select uid, name, attributes from regions_meta where uid in %s" % ids_str
    else:
        query = "select uid, name, attributes from regions_meta"
    rows = db_session.execute(query)
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['name'] = row[1]
        new_doc['attributes'] = row[1]
        out['response']['data'].append(new_doc)
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
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for row in rows:
        new_doc = {}
        new_doc['uid'] = row[0]
        new_doc['geo'] = row[1]
        out['response']['data'].append(new_doc)
    return out