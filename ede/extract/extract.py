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
        print(query)
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

    json_template = dict(type='Feature', geometry=dict(type='Point', coordinates=None),
                         properties=dict(value=None))
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT st_clip(rd.rast, {}, r.geom, true) AS rast "
                 "FROM raster_data AS rd, regions AS r "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.rast, r.geom) "
                 "AND NOT st_bandisnodata(rd.rast, {}, false)), "
                 "tmp2 AS "
                 "(SELECT (st_pixelascentroids(rast)).* "
                 "FROM tmp1) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{geometry,coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{properties,value}}',val::text::jsonb)) "
                 "FROM tmp2".
                 format(time_id, dataset_id, var_id, region_id, time_id, json.dumps(json_template)))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT st_clip(rast, {}, st_setsrid(st_geomfromgeojson(\'{}\'),4326), true) AS rast "
                 "FROM raster_data "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(rast, st_setsrid(st_geomfromgeojson(\'{}\'),4326)) "
                 "AND NOT st_bandisnodata(rast, {}, false)), "
                 "tmp2 AS "
                 "(SELECT (st_pixelascentroids(rast)).* "
                 "FROM tmp1) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{geometry,coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{properties,value}}',val::text::jsonb)) "
                 "FROM tmp2".
                 format(time_id, json.dumps(region), dataset_id, var_id, json.dumps(region), time_id,
                        json.dumps(json_template)))
    try:
        print(query)
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

    json_template = dict(type='Feature', geometry=dict(type='Point', coordinates=None),
                         properties=dict(values=None))
    time_ids = range(time_id_start, time_id_end + 1, time_id_step)
    num_times = len(time_ids)
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT band, st_clip(rd.rast, band, r.geom, true) AS rast "
                 "FROM raster_data AS rd, regions AS r CROSS JOIN generate_series({}, {}, {}) AS band "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.rast, r.geom) "
                 "AND NOT st_bandisnodata(rd.rast, band, false)), "
                 "tmp2 AS "
                 "(SELECT band, (st_pixelascentroids(rast)).* "
                 "FROM tmp1), "
                 "tmp3 AS "
                 "(SELECT st_x(geom) AS lon, st_y(geom) AS lat, "
                 "fill_up_with_nulls(array_agg((band, val)::int_double_tuple), {}, {}, {}) AS values "
                 "FROM tmp2 "
                 "GROUP BY geom) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\','{{geometry,coordinates}}',"
                 "array_to_json(ARRAY[lon,lat])::jsonb),'{{properties,values}}',"
                 "array_to_json(values)::jsonb)) "
                 "FROM tmp3".
                 format(time_id_start, time_id_end, time_id_step, dataset_id, var_id, region_id,
                        time_id_start, time_id_step, num_times, json.dumps(json_template)))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT band, st_clip(rast, band, st_setsrid(st_geomfromgeojson(\'{}\'),4326), true) AS rast "
                 "FROM raster_data CROSS JOIN generate_series({}, {}, {}) AS band "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(rast, st_setsrid(st_geomfromgeojson(\'{}\'),4326)) "
                 "AND NOT st_bandisnodata(rast, band, false)), "
                 "tmp2 AS "
                 "(SELECT band, (st_pixelascentroids(rast)).* "
                 "FROM tmp1), "
                 "tmp3 AS "
                 "(SELECT st_x(geom) AS lon, st_y(geom) AS lat, "
                 "fill_up_with_nulls(array_agg((band, val)::int_double_tuple), {}, {}, {}) AS values "
                 "FROM tmp2 "
                 "GROUP BY geom) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\','{{geometry,coordinates}}',"
                 "array_to_json(ARRAY[lon,lat])::jsonb),'{{properties,values}}',"
                 "array_to_json(values)::jsonb)) "
                 "FROM tmp3".
                 format(json.dumps(region), time_id_start, time_id_end, time_id_step, dataset_id, var_id,
                        json.dumps(region), time_id_start, time_id_end, time_id_step, json.dumps(json_template)))
    try:
        print(query)
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


def return_rasterdata_aggregate_spatial_single_time(dataset_id, var_id, time_id, request_body):

    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("WITH tmp AS "
                 "(SELECT (st_summarystats(st_clip(rd.rast, {}, r.geom, true))).* "
                 "FROM raster_data AS rd, regions AS r "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.rast, r.geom) "
                 "AND NOT st_bandisnodata(rd.rast, {}, false)) "
                 "SELECT SUM(sum) / SUM(count) "
                 "FROM tmp "
                 "WHERE count != 0".
                 format(time_id, dataset_id, var_id, region_id, time_id))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("WITH tmp AS "
                 "(SELECT (st_summarystats(st_clip(rast, {}, st_setsrid("
                 "st_geomfromgeojson(\'{}\'),4326), true))).* "
                 "FROM raster_data "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(rast, st_setsrid(st_geomfromgeojson(\'{}\'),4326)) "
                 "AND NOT st_bandisnodata(rast, {}, false)) "
                 "SELECT SUM(sum) / SUM(count) "
                 "FROM tmp "
                 "WHERE count != 0".
                 format(time_id, json.dumps(region), dataset_id, var_id, json.dumps(region), time_id))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_single_time: could not return "
                                        "rasterdata with dataset_id: {}, var_id: {}, time_id: {}, request_body: {}".
                                        format(dataset_id, var_id, time_id, json.dumps(request_body)))
    # The aggregation values
    (res,) = rows.first()

    # The region as a GeoJSON
    region_as_geojson = None
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = "SELECT st_asgeojson(geom) FROM regions WHERE uid={}".format(region_id)
        try:
            rows = db_session.execute(query)
        except SQLAlchemyError as e:
            eprint(e)
            raise RasterExtractionException("return_rasterdata_aggregate_spatial: could not return geojson"
                                            "of indirect region with id: {}".format(region_id))
        (region_as_geojson,) = rows.first()
    elif kind == 'direct':
        (_, region) = request_args
        region_as_geojson = region

    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    # The actual data
    out['response']['data'] = []
    data_item = {'type': 'Feature', 'geometry': {}, 'properties': {'mean': None}}
    data_item['properties']['mean'] = res
    data_item['geometry'] = region_as_geojson
    out['response']['data'].append(data_item)
    # The metadata
    out['response']['metadata'] = {}
    out['response']['metadata']['region'] = request_body
    out['response']['metadata']['format'] = 'region'
    # The variable's unit
    query = "SELECT attrs FROM raster_variables WHERE uid={}".format(var_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_single_time: could not return "
                                        "the variable's unit for var_id: {}".format(var_id))
    (attrs,) = rows.first()
    out['response']['metadata']['units'] = "N/A"
    try:
        for attr in attrs:
            if attr['name'] == 'units':
                out['response']['metadata']['units'] = attr['value']
    except KeyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_single_time: the attributes field of "
                                        "variable with var_id: {} stored in the DB is invalid!".format(var_id))
    # The human-readable time requested
    query = "SELECT time_start, time_step FROM raster_datasets WHERE uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_single_time: could not return "
                                        "time meta info from raster_datasets for dataset: {}".format(dataset_id))
    (time_start, time_step) = rows.first()
    time = time_start + (time_id-1) * time_step
    out['response']['metadata']['time'] = time.strftime('%Y-%m-%d %H:%M:%S')
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


def return_rasterdata_aggregate_spatial_time_range(dataset_id, var_id, time_id_start, time_id_step,
                                                            time_id_end, request_body):

    time_ids = range(time_id_start, time_id_end + 1, time_id_step)
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("WITH tmp AS "
                 "(SELECT band, (st_summarystats(st_clip(rd.rast, r.geom, true), band)).* "
                 "FROM raster_data AS rd, regions AS r CROSS JOIN "
                 "generate_series({}, {}, {}) AS band "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.rast, r.geom) "
                 "AND NOT st_bandisnodata(rd.rast, band, false)) "
                 "SELECT SUM(sum) / SUM(count) "
                 "FROM tmp "
                 "WHERE count != 0 GROUP BY band ORDER BY band".
                 format(time_id_start, time_id_end, time_id_step, dataset_id, var_id, region_id))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("WITH tmp AS "
                 "(SELECT band, (st_summarystats(st_clip(rast, st_setsrid("
                 "st_geomfromgeojson(\'{}\'),4326), true), band)).* "
                 "FROM raster_data CROSS JOIN "
                 "generate_series({}, {}, {}) AS band "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(rast, st_setsrid(st_geomfromgeojson(\'{}\'),4326)) "
                 "AND NOT st_bandisnodata(rast, band, false)) "
                 "SELECT SUM(sum) / SUM(count) "
                 "FROM tmp "
                 "WHERE count != 0 GROUP BY band ORDER BY band".
                 format(json.dumps(region), time_id_start, time_id_end, time_id_step, dataset_id, var_id,
                        json.dumps(region)))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_time_range: could not return "
                                        "rasterdata with dataset_id: {}, var_id: {}, "
                                        "time_id_start: {}, time_id_step: {}, time_id_end: {}, request_body: {}".
                                        format(dataset_id, var_id,
                                               time_id_start, time_id_step, time_id_end, json.dumps(request_body)))
    # The aggregation values
    aggr_values = []
    for (aggr_value,) in rows:
        aggr_values.append(aggr_value)

    # The region as a GeoJSON
    region_as_geojson = None
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = "SELECT st_asgeojson(geom) FROM regions WHERE uid={}".format(region_id)
        try:
            rows = db_session.execute(query)
        except SQLAlchemyError as e:
            eprint(e)
            raise RasterExtractionException("return_rasterdata_aggregate_spatial_time_range: could not return geojson"
                                            "of indirect region with id: {}".format(region_id))
        (region_as_geojson,) = rows.first()
    elif kind == 'direct':
        (_, region) = request_args
        region_as_geojson = region

    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    # The actual data
    out['response']['data'] = []
    data_item = {'type': 'Feature', 'geometry': {}, 'properties': {'means': None}}
    data_item['properties']['means'] = aggr_values
    data_item['geometry'] = region_as_geojson
    out['response']['data'].append(data_item)
    # The metadata
    out['response']['metadata'] = {}
    out['response']['metadata']['region'] = request_body
    out['response']['metadata']['format'] = 'region'
    # The variable's unit
    query = "SELECT attrs FROM raster_variables WHERE uid={}".format(var_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_time_range: could not return "
                                        "the variable's unit for var_id: {}".format(var_id))
    (attrs,) = rows.first()
    out['response']['metadata']['units'] = "N/A"
    try:
        for attr in attrs:
            if attr['name'] == 'units':
                out['response']['metadata']['units'] = attr['value']
    except KeyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_time_range: the attributes field of "
                                        "variable with var_id: {} stored in the DB is invalid!".format(var_id))
    # The human-readable times requested
    query = "SELECT time_start, time_step FROM raster_datasets WHERE uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_spatial_time_range: could not return "
                                        "time meta info from raster_datasets for dataset: {}".format(dataset_id))
    (time_start, time_step) = rows.first()
    times = [time_start + (time_id - 1) * time_step for time_id in time_ids]
    out['response']['metadata']['times'] = [time.strftime('%Y-%m-%d %H:%M:%S') for time in times]
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


def return_rasterdata_aggregate_temporal(dataset_id, var_id, time_id_start, time_id_step, time_id_end, request_body):

    json_template = dict(type='Feature', geometry=dict(type='Point', coordinates=None),
                         properties=dict(mean=None))
    time_ids = range(time_id_start, time_id_end + 1, time_id_step)
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT st_clip(rd.rast, band, r.geom, true) AS rast "
                 "FROM raster_data AS rd, regions AS r CROSS JOIN generate_series({}, {}, {}) AS band "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.rast, r.geom) "
                 "AND NOT st_bandisnodata(rd.rast, band, false)), "
                 "tmp2 AS "
                 "(SELECT st_union(rast,'MEAN') AS rast "
                 "FROM tmp1 "
                 "GROUP BY st_envelope(rast)), "
                 "tmp3 AS (SELECT (st_pixelascentroids(rast)).* FROM tmp2) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{geometry,coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{properties,mean}}',val::text::jsonb)) "
                 "FROM tmp3".
                 format(time_id_start, time_id_end, time_id_step, dataset_id, var_id, region_id,
                        json.dumps(json_template)))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("WITH tmp1 AS "
                 "(SELECT st_clip(rast, band, st_setsrid(st_geomfromgeojson(\'{}\'),4326), true) AS rast "
                 "FROM raster_data CROSS JOIN generate_series({}, {}, {}) AS band "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(rast, st_setsrid(st_geomfromgeojson(\'{}\'),4326)) "
                 "AND NOT st_bandisnodata(rast, band, false)), "
                 "tmp2 AS "
                 "(SELECT st_union(rast,'MEAN') AS rast "
                 "FROM tmp1 "
                 "GROUP BY st_envelope(rast)), "
                 "tmp3 AS (SELECT (st_pixelascentroids(rast)).* FROM tmp2) "
                 "SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{geometry,coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{properties,mean}}',val::text::jsonb)) "
                 "FROM tmp3".
                 format(json.dumps(region), time_id_start, time_id_end, time_id_step, dataset_id, var_id,
                        json.dumps(region), json.dumps(json_template)))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_temporal: could not temporally aggregate "
                                        "rasterdata with "
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
        raise RasterExtractionException("return_rasterdata_aggregate_temporal: could not return the variable's unit "
                                        "for var_id: {}".format(var_id))
    (attrs,) = rows.first()
    out['response']['metadata']['units'] = "N/A"
    try:
        for attr in attrs:
            if attr['name'] == 'units':
                out['response']['metadata']['units'] = attr['value']
    except KeyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_temporal: the attributes field of variable "
                                        "with var_id: {} stored in the DB is invalid!".format(var_id))
    # The human-readable times requested
    query = "SELECT time_start, time_step FROM raster_datasets WHERE uid={}".format(dataset_id)
    try:
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_aggregate_temporal: could not return time meta info from "
                                        "raster_datasets for dataset: {}".format(dataset_id))
    (time_start, time_step) = rows.first()
    times = [time_start + (time_id-1) * time_step for time_id in time_ids]
    out['response']['metadata']['times'] = [time.strftime('%Y-%m-%d %H:%M:%S') for time in times]
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


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

    query = "SELECT uid, st_asgeojson(geom) FROM regions WHERE regionset_id={} {}".format(regionset_id, where_str)
    try:
        print(query)
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