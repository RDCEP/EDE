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


def return_gridmeta(dataset_ids):
    query = ("select uid, short_name, long_name, lat_start, lat_end, lat_step, num_lats, "
             "lon_start, lon_end, lon_step, num_lons, time_start, time_end, time_step, num_times, time_unit "
             "from grid_datasets")
    if dataset_ids:
        dataset_ids_str = '(' + ','.join(map(str, dataset_ids)) + ')'
        query += " where uid in {}".format(dataset_ids_str)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_gridmeta: could not return gridmeta with dataset_ids: {}".format(dataset_ids))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    for (uid, short_name, long_name, lat_start, lat_end, lat_step, num_lats,
         lon_start, lon_end, lon_step, num_lons,
         time_start, time_end, time_step, num_times, time_unit) in rows:
        new_doc = {}
        new_doc['uid'] = uid
        new_doc['short_name'] = short_name
        new_doc['long_name'] = long_name
        new_doc['lat_start'] = lat_start
        new_doc['lat_end'] = lat_end
        new_doc['lat_step'] = lat_step
        new_doc['num_lats'] = num_lats
        new_doc['lon_start'] = lon_start
        new_doc['lon_end'] = lon_end
        new_doc['lon_step'] = lon_step
        new_doc['num_lons'] = num_lons
        new_doc['time_start'] = time_start
        new_doc['time_end'] = time_end
        new_doc['time_step'] = time_step
        new_doc['num_times'] = num_times
        new_doc['time_unit'] = time_unit
        out['response']['data'].append(new_doc)
    return out


def return_griddata(dataset_id, var_id, poly, time_id):
    if poly is not None:
        # polygon is specified by id
        if isinstance(poly, int):
            query = ("SELECT ST_Clip(rast, r.geom, TRUE) "
                     "from grid_data as gd, regions as r "
                     "where gd.dataset_id={} and gd.var_id={} and r.uid={} and gd.time_id={}").format(dataset_id, var_id, poly, time_id)
        elif isinstance(poly, list):
            poly_str = ','.join(["{} {}".format(pt[0], pt[1]) for pt in poly])
            geom_str = "ST_Polygon(ST_GeomFromText('LINESTRING({})'), 4326)".format(poly_str)
            query = ("SELECT ST_Clip(rast, {}, TRUE) "
                     "from grid_data as gd "
                     "where gd.dataset_id={} and gd.var_id={} and gd.time_id={}").format(geom_str, dataset_id, var_id, time_id)
        else:
            raise RasterExtractionException("return_griddata: type of POST poly field not supported!")
    else:
        query = ("SELECT rast from grid_data where dataset_id={} and var_id={} and time_id={}".
                 format(dataset_id, var_id, time_id))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata: could not return griddata with dataset_id: {}, "
                                        "var_id: {}, poly: {}, time_id: {}".format(dataset_id, var_id, str(poly), time_id))
    for (rast,) in rows:
        return rast


def return_griddata_json(dataset_id, var_id, time_id):
    query = ("SELECT rast from grid_data where dataset_id={} and var_id={} and time_id={}".
             format(dataset_id, var_id, time_id))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata_json: could not return griddata with dataset_id: {}, "
                                        "var_id: {}, time_id: {}".format(dataset_id, var_id, time_id))
    raster = None
    print(rows)
    print(type(rows))
    print(rows[0])
    for (out,) in rows:
        raster = out
    return raster


def return_griddata_aggregate_spatial(dataset_id, var_id, poly, time_ids):
    time_ids_str = '(' + ','.join(map(str, time_ids)) + ')'
    if poly is not None:
        # polygon is specified by id
        if isinstance(poly, int):
            query = ("SELECT ST_SummaryStats(ST_Clip(rast, r.geom, TRUE)) "
                     "from grid_data as gd, regions as r "
                     "where gd.dataset_id={} and gd.var_id={} and r.uid={} and gd.time_id in {}").format(dataset_id, var_id, poly, time_ids_str)
        elif isinstance(poly, list):
            poly_str = ','.join(["{} {}".format(pt[0], pt[1]) for pt in poly])
            geom_str = "ST_Polygon(ST_GeomFromText('LINESTRING({})'), 4326)".format(poly_str)
            query = ("SELECT ST_SummaryStats(ST_Clip(rast, {}, TRUE)) "
                     "from grid_data as gd "
                     "where gd.dataset_id={} and gd.var_id={} and gd.time_id in {}").format(geom_str, dataset_id, var_id, time_ids_str)
        else:
            raise RasterExtractionException("return_griddata_aggregate_spatial: type of POST poly field not supported!")
    else:
        query = ("SELECT ST_SummaryStats(rast) from grid_data where dataset_id={} and var_id={} and time_id in {}".
                 format(dataset_id, var_id, time_ids_str))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata_aggregate_spatial: could not spatially-aggregate griddata with dataset_id: {}, "
                                        "var_id: {}, poly: {}, time_ids: {}".format(dataset_id, var_id, str(poly), time_ids))
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
        res = row[0].lstrip('(').rstrip(')').split(',')
        count = int(res[0])
        sum = float(res[1])
        mean = float(res[2])
        stddev = float(res[3])
        min = float(res[4])
        max = float(res[5])
        new_data_item = {}
        new_data_item['type'] = 'Feature'
        new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
        new_data_item['properties'] = {'count': count, 'sum': sum, 'mean': mean, 'stddev': stddev, 'min': min, 'max': max}
        out['response']['data'].append(new_data_item)
    return out


def return_griddata_aggregate_temporal(dataset_id, var_id, poly, time_ids):
    time_ids_str = '(' + ','.join(map(str, time_ids)) + ')'
    if poly is not None:
        # polygon is specified by id
        if isinstance(poly, int):
            tmp = ("with foo as (select array(select ROW(ST_Clip(rast, r.geom), 1)::rastbandarg as rast "
                   "from grid_data as gd, regions as r"
                   "where gd.dataset_id={} and gd.var_id={} and r.uid={} and gd.time_id in {}))").format(dataset_id, var_id, str(poly), time_ids_str)
        elif isinstance(poly, list):
            poly_str = ','.join(["{} {}".format(pt[0], pt[1]) for pt in poly])
            geom_str = "ST_Polygon(ST_GeomFromText('LINESTRING({})'), 4326)".format(poly_str)
            tmp = ("with foo as (select array(select ROW(ST_Clip(rast, {}), 1)::rastbandarg as rast "
                   "from grid_data "
                   "where dataset_id={} and var_id={} and time_id in {}))").format(geom_str, dataset_id, var_id, time_ids_str)
        else:
            raise RasterExtractionException("return_griddata_aggregate_temporal: type of POST poly field not supported!")
    else:
        tmp = ("with foo as (select array(select ROW(rast, 1)::rastbandarg as rast "
               "from grid_data "
               "where dataset_id={} and var_id={} and time_id in {}))").format(dataset_id, var_id, time_ids_str)
    query = tmp + '\n' + ("select ST_MapAlgebra((select * from foo)::rastbandarg[], "
                          "'st_stddev4ma(double precision[], int[], text[])'::regprocedure)")
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata_aggregate_temporal: could not temporally-aggregate griddata with dataset_id: {}, "
                                        "var_id: {}, poly: {}, time_ids: {}".format(dataset_id, var_id, str(poly), time_ids))
    for (rast,) in rows:
        return rast


def return_regionmeta(regionset_ids):
    query = "select uid, name, attributes from regions_meta"
    if regionset_ids:
        regionset_ids_str = '(' + ','.join(map(str, regionset_ids)) + ')'
        query += " where uid in {}".format(regionset_ids_str)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_regionmeta: could not return regionmeta with regionset_ids: {}".format(regionset_ids))
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


def return_regiondata(regionset_id, region_id):
    query = "select uid, ST_AsGeoJSON(geom) from regions where regionset_id={} and region_id={}".format(regionset_id, region_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_regiondata: could not return region data with regionset_id: {}, region_d: {}".format(regionset_id, region_id))
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
