from __future__ import print_function
import sys
from datetime import datetime
import time
from ede.database import db_session
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import json
from ede.api.utils import RasterExtractionException

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def return_rastermeta(dataset_id):
    query = ("SELECT rd.uid, rd.short_name, rd.long_name, "
             "rd.lon_start, rd.lon_end, rd.lon_step, rd.num_lons, "
             "rd.lat_start, rd.lat_end, rd.lat_step, rd.num_lats, "
             "rd.time_start, rd.time_end, rd.time_step, rd.num_times, rd.time_unit, rd.attrs, "
             "rv.uid, rv.name, rv.attrs"
             "FROM raster_datasets AS rd, raster_variables AS rv "
             "WHERE rd.uid=rv.dataset_id "
             "GROUP BY rv.dataset_id")
    if dataset_id:
        query += " AND uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rastermeta: could not return rastermeta with dataset_id: {}".
                                        format(dataset_id))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []
    for (uid, short_name, long_name,
         lon_start, lon_end, lon_step, num_lons,
         lat_start, lat_end, lat_step, num_lats,
         time_start, time_end, time_step, num_times, time_unit, attrs) in rows:
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
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    return out


def return_griddata(dataset_id, var_id, poly, time_id):
    if poly is not None:
        # polygon is specified by id
        if isinstance(poly, int):
            query = ("SELECT json_agg(json)::text "
                     "from grid_data as gd, regions as r "
                     "where gd.dataset_id={} and gd.var_id={} and r.uid={} and gd.time_id={} and "
                     "json->'properties'->'values'->>0 != 'null' and"
                     "ST_Contains(r.geom, gd.geom").format(dataset_id, var_id, poly, time_id)
        elif isinstance(poly, list):
            poly_str = ','.join(["{} {}".format(pt[0], pt[1]) for pt in poly])
            geom_str = "ST_Polygon(ST_GeomFromText('LINESTRING({})'), 4326)".format(poly_str)
            query = ("SELECT json_agg(json)::text "
                     "from grid_data as gd "
                     "where gd.dataset_id={} and gd.var_id={} and gd.time_id={} and "
                     "json->'properties'->'values'->>0 != 'null' and"
                     "ST_Contains({}, gd.geom").format(dataset_id, var_id, time_id, geom_str)
        else:
            raise RasterExtractionException("return_griddata: type of POST poly field not supported!")
    else:
        query = "select json_agg(json)::text from grid_data where dataset_id={} and var_id={} and time_id={} and " \
                "json->'properties'->'values'->>0 != 'null'".format(dataset_id, var_id, time_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata: could not return griddata with dataset_id: {}, "
                                        "var_id: {}, poly: {}, time_id: {}".format(dataset_id, var_id, str(poly), time_id))
    # conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    # cur = conn.cursor("My cursor to fetch the jsons!")
    # cur.itersize = 10000
    # cur.execute(query)
    # start_time = time.time()
    row = rows.first()
    # print("type of row[0]: {}".format(type(row[0])))
    # print("type of row[0][0]: {}".format(type(row[0][0])))
    # print("keys of dict row[0][0]: {}".format(row[0][0].keys()))
    # print("type of row[0][0]: {}".format(type(row[0][0])))
    # print("type of row[0]: {}".format(type(row[0].keys())))
    # print("keys: {}".format(row[0].keys()))
    # print("type of row[0][0]: {}".format(type(row[0][0])))
    # print("--- return_griddata, get result from postgres: %s seconds ---" % (time.time() - start_time))
    # for record in cur:
    #     yield record[0]
    # out = {}
    # out['request'] = {}
    # out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    # out['response'] = {}
    # out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    # out['response']['status'] = 'OK'
    # out['response']['status_code'] = 200
    # out['response']['data'] = row[0]
    # return out
    out = ("{{\"request\": {{\"url\": \"/api/v0/griddata/dataset/{0}/var/{1}/time/{2}\", \"datetime\": \"{3}\"}}, "
           "\"response\": {{\"status\": \"OK\", \"status_code\": 200, "
           "\"data\": {4}}}}}".format(dataset_id, var_id, time_id, time.strftime('%Y-%m-%d %H:%M:%S'), row[0]))
    # return row[0]
    return out


def return_griddata_aggregate_spatial(dataset_id, var_id, poly, time_id):
    if poly is not None:
        # polygon is specified by id
        if isinstance(poly, int):
            query = ("SELECT sum(cast(json->'properties'->'values'->>0 as double precision)) "
                     "from grid_data as gd, regions as r "
                     "where gd.dataset_id={} and gd.var_id={} and r.uid={} and gd.time_id={} and "
                     "ST_Contains(r.geom, gd.geom and "
                     "json->'properties'->'values'->>0 != 'null'").format(dataset_id, var_id, poly, time_id)
        elif isinstance(poly, list):
            poly_str = ','.join(["{} {}".format(pt[0], pt[1]) for pt in poly])
            geom_str = "ST_Polygon(ST_GeomFromText('LINESTRING({})'), 4326)".format(poly_str)
            query = ("SELECT sum(cast(json->'properties'->'values'->>0 as double precision)) "
                     "from grid_data as gd "
                     "where gd.dataset_id={} and gd.var_id={} and gd.time_id={} and "
                     "ST_Contains({}, gd.geom and "
                     "json->'properties'->'values'->>0 != 'null'").format(dataset_id, var_id, time_id, geom_str)
        else:
            raise RasterExtractionException("return_griddata_aggregate_spatial: type of POST poly field not supported!")
    else:
        query = "select sum(cast(json->'properties'->'values'->>0 as double precision)) from grid_data where dataset_id={} and var_id={} and time_id={} and " \
                "json->'properties'->'values'->>0 != 'null'".format(dataset_id, var_id, time_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_griddata_aggregate_spatial: could not spatially-aggregate griddata with dataset_id: {}, "
                                        "var_id: {}, poly: {}, time_id: {}".format(dataset_id, var_id, str(poly), time_id))
    # the response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['status'] = 'OK'
    out['response']['status_code'] = 200
    out['response']['data'] = []
    row = rows.first()
    sum = float(row[0])
    new_data_item = {}
    new_data_item['type'] = 'Feature'
    new_data_item['geometry'] = {'type': 'Polygon', 'coordinates': poly}
    new_data_item['properties'] = {'sum': sum}
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
