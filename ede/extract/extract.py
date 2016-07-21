from __future__ import print_function

import json
import sys
from datetime import datetime
from ede.database import db_session
from sqlalchemy.exc import SQLAlchemyError
from ede.api.utils import RasterExtractionException, RequestFormatException


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def return_rastermeta(dataset_id):

    where_cond = ""
    if dataset_id:
        where_cond = " AND rd.uid={} ".format(dataset_id)
    # TODO: not clear if array_agg's have same order, might not want to rely on that!
    query = ("SELECT rd.uid, rd.short_name, rd.long_name, "
             "rd.lon_start, rd.lon_end, rd.lon_step, rd.num_lons, "
             "rd.lat_start, rd.lat_end, rd.lat_step, rd.num_lats, "
             "rd.time_start, rd.time_end, rd.time_step, rd.num_times, rd.time_unit, rd.attrs, "
             "array_agg(rv.uid), array_agg(rv.name), array_agg(rv.attrs) "
             "FROM raster_datasets AS rd, raster_variables AS rv "
             "WHERE rd.uid=rv.dataset_id {}"
             "GROUP BY rd.uid, rd.short_name, rd.long_name, "
             "rd.lon_start, rd.lon_end, rd.lon_step, rd.num_lons, "
             "rd.lat_start, rd.lat_end, rd.lat_step, rd.num_lats, "
             "rd.time_start, rd.time_end, rd.time_step, rd.num_times, rd.time_unit, rd.attrs".
             format(where_cond))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rastermeta: could not return rastermeta with dataset_id: {}".
                                        format(dataset_id))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []

    for (ds_id, short_name, long_name,
         lon_start, lon_end, lon_step, num_lons,
         lat_start, lat_end, lat_step, num_lats,
         time_start, time_end, time_step, num_times, time_unit, attrs,
         var_ids, var_names, var_attrs) in rows:
        ds_item = {}
        ds_item['uid'] = ds_id
        ds_item['short_name'] = short_name
        ds_item['long_name'] = long_name
        ds_item['date_created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ds_item['date_inserted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ds_item['dims'] = []
        ds_item['dims'].append({
            'name': 'lon',
            'start': lon_start,
            'end': lon_end,
            'step': lon_step,
            'count': num_lons,
            'attrs': {}
        })
        ds_item['dims'].append({
            'name': 'lat',
            'start': lat_start,
            'end': lat_end,
            'step': lat_step,
            'count': num_lats,
            'attrs': {}
        })
        ds_item['dims'].append({
            'name': 'time',
            'start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'end': time_end.strftime('%Y-%m-%d %H:%M:%S'),
            'step': 1, # hardcoded
            'count': num_times,
            'attrs': {'units': time_unit}
        })
        ds_item['vars'] = []
        for i, var_id in enumerate(var_ids):
            var_item = {}
            var_item['uid'] = var_id
            var_item['name'] = var_names[i]
            var_item['dims'] = ['time','lat','lon']
            var_item['ndims'] = 3
            var_item['shape'] = [34,360,720]
            var_item['attrs'] = var_attrs[i]
            ds_item['vars'].append(var_item)
        ds_item['attrs'] = attrs
        out['response']['data'].append(ds_item)

    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


def _parse_json_body(request_body):
    """Parses POST JSON request

    :param request_body:
    :return: ('direct', region) resp. ('indirect', regionset_id, region_id) where region = GeoJSON dict
    :raises: RequestFormatException: If the JSON body is invalid
    """
    try:
        kind = request_body['kind']
        if kind == 'indirect':
            regionset_id = request_body['region']['set']
            region_id = request_body['region']['id']
            return (kind, regionset_id, region_id)
        elif kind == 'direct':
            region = request_body['region'] # as a GeoJSON
            return (kind, region)
        else:
            raise RequestFormatException("parse_json_body: only supports 'direct' or 'indirect' regions!")
    except Exception as e:
        eprint(e)
        raise RequestFormatException("parse_json_body: got malformed POST JSON request body!")


def return_rasterdata_single_time(dataset_id, var_id, time_id, request_body):

    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(jsonb_build_object('type','Feature'),'{{geometry}}',"
                 "jsonb_set(jsonb_build_object('type','Point','coordinates',null),'{{coordinates}}',"
                 "jsonb_build_array(st_x(rd.geom),st_y(rd.geom)))),'{{properties}}',"
                 "jsonb_set(jsonb_build_object('value',null),'{{value}}',rd.value::text::jsonb))) "
                 "FROM raster_data_single AS rd, regions AS r "
                 "WHERE rd.dataset_id={} AND rd.var_id={} AND rd.time_id={} AND rd.value IS NOT NULL "
                 "AND r.uid={} AND st_contains(r.geom, rd.geom)".
                 format(dataset_id, var_id, time_id, region_id))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(jsonb_build_object('type','Feature'),'{{geometry}}',"
                 "jsonb_set(jsonb_build_object('type','Point','coordinates',null),'{{coordinates}}',"
                 "jsonb_build_array(st_x(geom),st_y(geom)))),'{{properties}}',"
                 "jsonb_set(jsonb_build_object('value',null),'{{value}}',value::text::jsonb))) "
                 "FROM raster_data_single "
                 "WHERE dataset_id={} AND var_id={} AND time_id={} AND value IS NOT NULL "
                 "AND st_contains(st_setsrid(st_geomfromgeojson(\'{}\'),4326), geom)".
                 format(dataset_id, var_id, time_id, json.dumps(region)))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_single_time: could not return rasterdata with "
                                        "dataset_id: {}, var_id: {}, time_id: {}, request_body: {}".
                                        format(dataset_id, var_id, time_id, json.dumps(request_body)))
    # The response JSON
    (res,) = rows.first()
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    # The actual data
    out['response']['data'] = res
    # The metadata
    out['response']['metadata'] = {}
    out['response']['metadata']['region'] = request_body
    out['response']['metadata']['format'] = 'raster'
    # The variable's unit
    query = "SELECT attrs FROM raster_variables WHERE uid={}".format(var_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_single_time: could not return the variable's unit "
                                        "for var_id: {}".format(var_id))
    (attrs,) = rows.first()
    out['response']['metadata']['units'] = "N/A"
    try:
        for attr in attrs:
            if attr['name'] == 'units':
                out['response']['metadata']['units'] = attr['value']
    except KeyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_single_time: the attributes field of variable "
                                        "with var_id: {} stored in the DB is invalid!".format(var_id))
    # The human-readable time requested
    query = "SELECT time_start, time_step FROM raster_datasets WHERE uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_single_time: could not return time meta info from "
                                        "raster_datasets for dataset: {}".format(dataset_id))
    (time_start, time_step) = rows.first()
    time = time_start + (time_id-1) * time_step
    out['response']['metadata']['time'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


def return_rasterdata_time_range(dataset_id, var_id, time_id_start, time_id_step, time_id_end, request_body):

    time_ids = range(time_id_start, time_id_end+1, time_id_step)
    values_select = ','.join(["values[{}]".format(time_id) for time_id in time_ids])
    values_cond = ' AND '.join(["values[{}] IS NOT NULL".format(time_id) for time_id in time_ids])
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(jsonb_build_object('type','Feature'),'{{geometry}}',"
                 "jsonb_set(jsonb_build_object('type','Point','coordinates',null),'{{coordinates}}',"
                 "jsonb_build_array(st_x(rd.geom),st_y(rd.geom)))),'{{properties}}',"
                 "jsonb_set(jsonb_build_object('values',null),'{{values}}',jsonb_build_array({})))) "
                 "FROM raster_data_series AS rd, regions AS r "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_contains(r.geom, rd.geom) "
                 "AND {}".
                 format(values_select, dataset_id, var_id, region_id, values_cond))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(jsonb_build_object('type','Feature'),'{{geometry}}',"
                 "jsonb_set(jsonb_build_object('type','Point','coordinates',null),'{{coordinates}}',"
                 "jsonb_build_array(st_x(geom),st_y(geom)))),'{{properties}}',"
                 "jsonb_set(jsonb_build_object('values',null),'{{values}}',jsonb_build_array({})))) "
                 "FROM raster_data_series "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_contains(st_setsrid(st_geomfromgeojson(\'{}\'),4326), geom) "
                 "AND {}".
                 format(values_select, dataset_id, var_id, json.dumps(region), values_cond))
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_time_range: could not return rasterdata with "
                                        "dataset_id: {}, var_id: {}, "
                                        "time_id_start: {}, time_id_step: {}, time_id_end: {}, "
                                        "request_body: {}".
                                        format(dataset_id, var_id,
                                               time_id_start, time_id_step, time_id_end,
                                               json.dumps(request_body)))
    # The response JSON
    (res,) = rows.first()
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    # The actual data
    out['response']['data'] = res
    # The metadata
    out['response']['metadata'] = {}
    out['response']['metadata']['region'] = request_body
    out['response']['metadata']['format'] = 'raster'
    # The variable's unit
    query = "SELECT attrs FROM raster_variables WHERE uid={}".format(var_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_time_range: could not return the variable's unit "
                                        "for var_id: {}".format(var_id))
    (attrs,) = rows.first()
    out['response']['metadata']['units'] = "N/A"
    try:
        for attr in attrs:
            if attr['name'] == 'units':
                out['response']['metadata']['units'] = attr['value']
    except KeyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_time_range: the attributes field of variable "
                                        "with var_id: {} stored in the DB is invalid!".format(var_id))
    # The human-readable time requested
    query = "SELECT time_start, time_step FROM raster_datasets WHERE uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_time_range: could not return time meta info from "
                                        "raster_datasets for dataset: {}".format(dataset_id))
    (time_start, time_step) = rows.first()
    times = [time_start + (time_id-1) * time_step for time_id in time_ids]
    out['response']['metadata']['times'] = [time.strftime('%Y-%m-%d %H:%M:%S') for time in times]
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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


def return_regionmeta(regionset_id):

    query = "SELECT uid, name, attrs FROM regionsets"
    if regionset_id:
        query += " WHERE uid={}".format(regionset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_regionmeta: could not return regionmeta with regionset_id: {}".
                                        format(regionset_id))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []
    for (uid, name, attrs) in rows:
        rs_item = {}
        rs_item['uid'] = uid
        rs_item['name'] = name
        rs_item['attrs'] = attrs
        out['response']['data'].append(rs_item)
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


def return_regiondata(regionset_id, request_body):

    try:
        where_str = ""
        for attr in request_body['attrs']:
            where_str += "AND attrs->>\'{}\'=\'{}\'".format(attr['name'], attr['value'])
    except KeyError as e:
        eprint(e)
        raise RequestFormatException("return_regiondata: invalid POST request body")

    query = "SELECT uid, ST_AsGeoJSON(geom) FROM regions WHERE regionset_id={} {}".format(regionset_id, where_str)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_regiondata: could not return region data with regionset_id: {} and "
                                        "POST request body: {}".format(regionset_id, json.dumps(request_body)))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []
    for (uid, geo) in rows:
        region_item = {}
        region_item['uid'] = uid
        region_item['geo'] = geo
        out['response']['data'].append(region_item)
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out
