from __future__ import print_function

import json
import sys
from datetime import datetime
from ede.database import db_session
from sqlalchemy.exc import SQLAlchemyError
from ede.api.utils import RasterExtractionException, RequestFormatException, UnsupportedCaseException


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# TODO: Check
def return_dataset_meta(dataset_id):

    where_cond = "d.uid=v.dataset_id"
    if dataset_id:
        where_cond += " AND d.uid={}".format(dataset_id)
    query = ("SELECT d.uid, d.attrs, array_agg(v.*) "
             "FROM dataset AS d, variable AS v "
             "WHERE {} "
             "GROUP BY d.uid".
             format(where_cond))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_dataset_meta: could not return rastermeta with dataset_id: {}".
                                        format(dataset_id))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []
    for (ds_id, ds_attrs, vars) in rows:
        ds_item = {}
        ds_item['uid'] = ds_id
        ds_item['attrs'] = ds_attrs
        ds_item['vars'] = {}
        ds_item['vars']['axisVars'] = []
        ds_item['vars']['dataVars'] = []
        for (uid, _, name, _, num_dims, dims_names, dims_sizes, attrs,
             min, max, type, axes, axes_mins, axes_maxs, axes_units) in vars:
            var_item = {}
            var_item['uid'] = uid
            var_item['name'] = name
            var_item['numDims'] = num_dims
            var_item['dimsNames'] = dims_names
            var_item['attrs'] = attrs
            var_item['globalMin'] = min
            var_item['globalMax'] = max
            if type in ['T','Z','Y','X']:
                var_item['type'] = type
                ds_item['vars']['axisVars'].append(var_item)
            if type == 'D':
                var_item['axesTypes'] = axes
                var_item['axesMins'] = axes_mins
                var_item['axesMaxs'] = axes_maxs
                var_item['axesUnits'] = axes_units
                ds_item['vars']['dataVars'].append(var_item)
        out['response']['data'].append(ds_item)
    out['response']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return out


# TODO: Check
def return_variable_meta(var_id):

    where_cond = "v.dataset_id=d.uid"
    if var_id:
        where_cond += " AND v.uid={}".format(var_id)
    query = ("SELECT v.*, d.attrs FROM variable AS v, dataset AS d WHERE {}".format(where_cond))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_variable_meta: could not return variable metadata with var_id: {}".
                                        format(var_id))
    # The response JSON
    out = {}
    out['request'] = {}
    out['request']['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out['response'] = {}
    out['response']['data'] = []
    for (var, ds_attrs) in rows:
        (uid, dataset_id, name, datatype, num_dims, dims_names, dims_sizes, var_attrs, min, max,
         type, axes_types, axes_mins, axes_maxs, axes_units) = var
        var_item = {}
        var_item['uid'] = uid
        var_item['dataset'] = {'uid': dataset_id, 'attrs': ds_attrs}
        var_item['name'] = name
        var_item['numDims'] = num_dims
        var_item['dimsNames'] = dims_names
        var_item['dimsSizes'] = dims_sizes
        var_item['attrs'] = var_attrs
        var_item['globalMin'] = min
        var_item['globalMax'] = max
        var_item['type'] = type
        if type == 'D':
            var_item['axes_types'] = axes_types
            var_item['axes_mins'] = axes_mins
            var_item['axes_maxs'] = axes_maxs
            var_item['axes_units'] = axes_units
            query = ("SELECT * "
                     "FROM variable "
                     "WHERE dataset_id={} AND type=ANY('{\"T\",\"Z\",\"Y\",\"X\"}')".
                     format(dataset_id))
            try:
                print(query)
                rows = db_session.execute(query)
            except SQLAlchemyError as e:
                eprint(e)
                raise RasterExtractionException("return_variable_meta: could not get axes for dataset with id: {}".
                                                format(dataset_id))
            var_item['axes'] = []
            for (uid, _, name, _, num_dims, dims_names, _, axis_attrs, min, max,
                 type, _, _, _, _) in rows:
                if type in axes_types:
                    axis_item = {}
                    axis_item['uid'] = uid
                    axis_item['name'] = name
                    axis_item['numDims'] = num_dims
                    axis_item['dimsNames'] = dims_names
                    axis_item['attrs'] = axis_attrs
                    axis_item['globalMin'] = min
                    axis_item['globalMax'] = max
                    axis_item['type'] = type
                    var_item['axes'].append(axis_item)
        out['response']['data'].append(var_item)
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


# TODO
def return_rasterdata(var_id, request_body):

    # find out what kind of variable it is
    query = ("SELECT type, axes FROM variable WHERE uid={}".format(var_id))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata: could not return type, axes for "
                                        "variable with uid: {}".
                                        format(var_id))
    # get actual data
    (type, axes) = rows.fetchone()
    if type == 'D':
        if axes == ['T']:
            raise UnsupportedCaseException("return_rasterdata: does not yet support [T] data variables!")
        elif axes == ['Z']:
            raise UnsupportedCaseException("return_rasterdata: does not yet support [Z] data variables!")
        elif axes == ['Y','X']:
            data = return_rasterdata_y_x(var_id, request_body)
        elif axes == ['T','Y','X']:
            raise UnsupportedCaseException("return_rasterdata: does not yet support [T,Y,X] data variables!")
            data = return_rasterdata_t_y_x(var_id, request_body)
        elif axes == ['Z','Y','X']:
            raise UnsupportedCaseException("return_rasterdata: does not yet support [Z,Y,X] data variables!")
            data = return_rasterdata_z_y_x(var_id, request_body)
        elif axes == ['T','Z','Y','X']:
            raise UnsupportedCaseException("return_rasterdata: does not yet support [T,Z,Y,X] data variables!")
            data = return_rasterdata_t_z_y_x(var_id, request_body)
    else:
        raise UnsupportedCaseException("return_rasterdata: does not yet support variables that are not data variables!")
    # get metadata
    query = ("SELECT v.*, d.attrs FROM variable AS v, dataset AS d WHERE v.dataset_id=d.uid AND uid={}".format(var_id))
    try:
        print(query)
        rows = db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata: could not return metadata for "
                                        "variable with uid: {}".
                                        format(var_id))
    metadata = rows.fetchone()
    # build the response by combining data + metadata



def return_rasterdata_y_x(var_id, request_body):
    try:
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{values}}', value::text::jsonb)) "
                 "FROM value_lat_lon "
                 "WHERE var_id={} "
                 "AND value IS NOT NULL".
                 format(json.dumps({}), var_id))
        for filter in request_body['filters']:
            axis = filter['axis']
            if axis == "Y,X":
                form = filter['restriction']['form']
                if form == 'direct':
                    geojson = filter['restriction']['parameters']
                    query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                             "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                             "'{{values}}', value::text::jsonb)) "
                             "FROM value_lat_lon "
                             "WHERE var_id={} "
                             "AND value IS NOT NULL "
                             "AND st_intersects(geom, st_setsrid(st_geomfromgeojson(\'{}\'),4326))".
                             format(json.dumps({}), var_id, geojson))
                elif form == 'indirect':
                    gadm_id = filter['restriction']['parameters']['uid']
                    query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                             "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                             "'{{values}}', value::text::jsonb)) "
                             "FROM value_lat_lon AS v, regions as r "
                             "WHERE v.var_id={} "
                             "AND value IS NOT NULL "
                             "AND r.uid={} "
                             "AND st_intersects(v.geom, r.geom)".
                             format(json.dumps({}), var_id, gadm_id))
                else:
                    raise RequestFormatException("return_rasterdata_y_x: got malformed POST JSON request body!")
    except (KeyError, RequestFormatException) as e:
        eprint(e)
        raise RequestFormatException("return_rasterdata_y_x: got malformed POST JSON request body!")
    try:
        print(query)
        db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_y_x: could not return actual data for "
                                        "variable with uid: {}".
                                        format(var_id))
    return query.fetchone()


def return_rasterdata_t_y_x(var_id, request_body):
    try:
        query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                 "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                 "'{{values}}', value::text::jsonb)) "
                 "FROM value_time_lat_lon "
                 "WHERE var_id={} "
                 "AND value IS NOT NULL".
                 format(json.dumps({}), var_id))
        for filter in request_body['filters']:
            axis = filter['axis']
            if axis == "Y,X":
                form = filter['restriction']['form']
                if form == 'direct':
                    geojson = filter['restriction']['parameters']
                    query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                             "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                             "'{{values}}', value::text::jsonb)) "
                             "FROM value_lat_lon "
                             "WHERE var_id={} "
                             "AND value IS NOT NULL "
                             "AND st_intersects(geom, st_setsrid(st_geomfromgeojson(\'{}\'),4326))".
                             format(json.dumps({}), var_id, geojson))
                elif form == 'indirect':
                    gadm_id = filter['restriction']['parameters']['uid']
                    query = ("SELECT jsonb_agg(jsonb_set(jsonb_set(\'{}\',"
                             "'{{coordinates}}',array_to_json(ARRAY[st_x(geom),st_y(geom)])::jsonb),"
                             "'{{values}}', value::text::jsonb)) "
                             "FROM value_lat_lon AS v, regions as r "
                             "WHERE v.var_id={} "
                             "AND value IS NOT NULL "
                             "AND r.uid={} "
                             "AND st_intersects(v.geom, r.geom)".
                             format(json.dumps({}), var_id, gadm_id))
                else:
                    raise RequestFormatException("return_rasterdata_y_x: got malformed POST JSON request body!")
    except (KeyError, RequestFormatException) as e:
        eprint(e)
        raise RequestFormatException("return_rasterdata_y_x: got malformed POST JSON request body!")
    try:
        print(query)
        db_session.execute(query)
    except SQLAlchemyError as e:
        eprint(e)
        raise RasterExtractionException("return_rasterdata_y_x: could not return actual data for "
                                        "variable with uid: {}".
                                        format(var_id))
    return query.fetchone()


def return_rasterdata_z_y_x(var_id, request_body):
    table = "value_vertical_lat_lon"


def return_rasterdata_t_z_y_x(var_id, request_body):
    table = "value_time_vertical_lat_lon"


# TODO
def return_rasterdata_aggregate(dataset_id, var_id, time_id_start, time_id_step,
                                                            time_id_end, request_body):

    time_ids = range(time_id_start, time_id_end + 1, time_id_step)
    time_ids_str = '(' + ','.join(map(str, time_ids)) + ')'
    values_select = ','.join(["avg(values[{}])".format(time_id) for time_id in time_ids])
    request_args = _parse_json_body(request_body)
    kind = request_args[0]
    if kind == 'indirect':
        (_, regionset_id, region_id) = request_args
        query = ("SELECT ARRAY[{}] "
                 "FROM raster_data_series AS rd, regions AS r "
                 "WHERE rd.dataset_id={} AND rd.var_id={} "
                 "AND r.uid={} AND st_intersects(rd.geom, r.geom)".
                 format(values_select, dataset_id, var_id, region_id))
    elif kind == 'direct':
        (_, region) = request_args
        query = ("SELECT ARRAY[{}] "
                 "FROM raster_data_series "
                 "WHERE dataset_id={} AND var_id={} "
                 "AND st_intersects(geom, st_setsrid(st_geomfromgeojson(\'{}\'),4326))".
                 format(values_select, dataset_id, var_id, json.dumps(region)))
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
    (aggr_values,) = rows.first()

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
