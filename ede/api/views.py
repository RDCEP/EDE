#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import simplejson as json
except ImportError:
    import json
from datetime import date
from flask import Blueprint, make_response, request
from flask.ext.cache import Cache
from ede.config import CACHE_CONFIG
from ede.extract.extract import *
from ede.api.utils import ServerError
from flask import jsonify

cache = Cache(config=CACHE_CONFIG)

API_VERSION = 'v0'
RESPONSE_LIMIT = 1000
CACHE_TIMEOUT = 60 * 60 * 6

api = Blueprint('ede_api', __name__, url_prefix='/api/{}'.format(API_VERSION))

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None


@api.errorhandler(ServerError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@api.route('/flush-cache')
def flush_cache():
    cache.clear()
    resp = {'status': 'ok', 'message': 'cache flushed!'}
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/gridmeta', defaults={'ids': None}, methods=['GET'])
@api.route('/gridmeta/datasets/<intlist:ids>', methods=['GET'])
def get_gridmeta(ids):
    """Get metadata of gridded datasets by IDs.

    If no list is passed, the metadata of all gridded datasets is returned.

    :param ids:
    :return:
    """
    try:
        data = return_gridmeta(ids)
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("Could not handle get_gridmeta request with ids", status_code=500, payload=ids)

    status_code = 200
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/dataset/<int:dataset_id>/var/<int:var_id>/time/<int:time_id>', methods=['GET', 'POST'])
def get_griddata(dataset_id, var_id, time_id):
    """Get values of a specified dataset, variable, time, and within a polygon.

    If no polygon is specified we default to the entire globe.

    :param dataset_id:
    :param var_id:
    :param time_id
    :return:
    """
    content = request.get_json()
    # if POST, i.e. we have some content
    try:
        if content:
            poly_id = content['poly_id']  # TODO: specify in JSON request format
            poly = content['poly']  # TODO: specify in JSON request format
            # if a polygon is specified by both poly_id and directly => return Bad Request Error
            if poly_id is not None and poly is not None:
                status_code = 400
                payload = {'dataset_id': dataset_id, 'var_id': var_id, 'content': content}
                raise ServerError("Cannot specify polygon directly and by id at the same time", status_code, payload)
            # if polygon is specified by id
            elif poly_id is not None:
                data = return_griddata_datasetid_varid_polyid_timeid(dataset_id, var_id, poly_id, time_id)
            # if polygon is specified directly
            elif poly is not None:
                data = return_griddata_datasetid_varid_poly_timeid(dataset_id, var_id, poly, time_id)
            # if no polygon is specified
            else:
                data = return_griddata_datasetid_varid_timeid(dataset_id, var_id, time_id)
        # if simple GET
        else:
            data = return_griddata_datasetid_varid_timeid(dataset_id, var_id, time_id)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'dataset_id': dataset_id, 'var_id': var_id, 'time_id': time_id, 'content': content}
        raise ServerError("Could not get griddata", status_code, payload)
    except ServerError:
        raise

    return data


@api.route('/aggregate/spatial/dataset/<int:meta_id>/var/<int:var_id>', methods=['GET', 'POST'])
def get_griddata_aggregate_spatial(meta_id, var_id):
    """Do spatial aggregation over specific polygon & for specific date.

    If no polygon is passed we default to the entire globe.
    If no date is passed we default to all dates.

    :param meta_id:
    :param var_id:
    :return:
    """
    status_code = 200
    content = request.get_json()
    date = None
    poly_id = None
    poly = None
    if content:
        date = content['dates']  # must be ID = integer, #TODO: specify in JSON request format
        poly_id = content['poly_id']
        poly = content['poly']
    if poly_id:
        data = return_griddata_aggregate_spatial_by_id(meta_id, var_id, poly_id, date)
    else:
        data = return_griddata_aggregate_spatial(meta_id, var_id, poly, date)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/temporal/dataset/<int:meta_id>/var/<int:var_id>', methods=['GET', 'POST'])
def get_griddata_aggregate_temporal(meta_id, var_id):
    status_code = 200
    content = request.get_json()
    dates = None
    poly_id = None
    poly = None
    if content:
        dates = content['dates']  # must be list of date IDs
        poly_id = content['poly_id']
        poly = content['poly']
    if poly_id:
        data = return_griddata_aggregate_temporal_by_id(meta_id, var_id, poly_id, dates)
    else:
        data = return_griddata_aggregate_temporal(meta_id, var_id, poly, dates)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/polymeta', defaults={'ids': None}, methods=['GET'])
@api.route('/polymeta/<intlist:ids>', methods=['GET'])
def get_polymeta(ids):
    status_code = 200
    data = return_polymeta(ids)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/polydata/<intlist:ids>', methods=['GET'])
def get_polydata(ids):
    status_code = 200
    data = return_polydata(ids)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
