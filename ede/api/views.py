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
from ede.api.utils import RequestFormatException, ServerError
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


@api.route('/rastermeta', defaults={'dataset_id': None}, methods=['GET'])
@api.route('/rastermeta/dataset/<int:dataset_id>', methods=['GET'])
def get_rastermeta(dataset_id):
    """Get metadata of a raster dataset by its ID

    If no ID is specified, then the metadata of all raster datasets
    currently present in the backend is returned

    :param dataset_id:
    :return:
    """
    try:
        start_time = time.time()
        data = return_rastermeta(dataset_id)
        print("--- return_rastermeta took %s seconds ---" % (time.time() - start_time))
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("get_rastermeta: could not handle request with dataset_id",
                          status_code=500, payload=dataset_id)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/rasterdata/dataset/<int:dataset_id>/var/<int:var_id>/time/<int:time_id>', methods=['POST'])
def get_rasterdata(dataset_id, var_id, time_id):
    """Get raster data values for a specific dataset:variable:time,
    and restricted to a region provided in the POST body

    :param dataset_id:
    :param var_id:
    :param time_id:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_rasterdata(dataset_id, var_id, time_id, request_body)
        print("--- return_rasterdata took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata: could not get rasterdata", status_code=500,
                          payload={'dataset_id': dataset_id, 'var_id': var_id,
                                   'time_id': time_id, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/rasterdata/dataset/<int:dataset_id>/var/<int:var_id>/time/'
           '<int:time_id_start>:<int:time_id_step>:<int:time_id_end>', methods=['POST'])
def get_rasterdata(dataset_id, var_id, time_id_start, time_id_step, time_id_end):
    """Get raster data values for a specific dataset:variable:[time_id_start:time_id_step:time_id_end],
    and restricted to a region provided in the POST body

    :param dataset_id:
    :param var_id:
    :param time_id_start:
    :param time_id_step:
    :param time_id_end:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_rasterdata(dataset_id, var_id, time_id_start, time_id_step, time_id_end, request_body)
        print("--- return_rasterdata took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata: could not get rasterdata", status_code=500,
                          payload={'dataset_id': dataset_id, 'var_id': var_id,
                                   'time_id_start': time_id_start, 'time_id_step': time_id_step,
                                   'time_id_end': time_id_end, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/spatial/dataset/<int:dataset_id>/var/<int:var_id>/time/<int:time_id>', methods=['POST'])
def get_rasterdata_aggregate_spatial(dataset_id, var_id, time_id):
    """Do spatial aggregation over a region provided in the POST body at time time_id

    :param dataset_id:
    :param var_id:
    :param time_id
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_griddata_aggregate_spatial(dataset_id, var_id, time_id, request_body)
        print("--- return_rasterdata_aggregate_spatial took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata_aggregate_spatial: could not aggregate rasterdata", status_code=500,
                          payload={'dataset_id': dataset_id, 'var_id': var_id,
                                   'time_id': time_id, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/spatial/dataset/<int:dataset_id>/var/<int:var_id>/time/'
           '<int:time_id_start>:<int:time_id_step>:<int:time_id_end>', methods=['POST'])
def get_rasterdata_aggregate_spatial(dataset_id, var_id, time_id_start, time_id_step, time_id_end):
    """Do spatial aggregation over a region provided in the POST body at times time_id:start:time_id_step:time_id_end

    :param dataset_id:
    :param var_id:
    :param time_id_start:
    :param time_id_step:
    :param time_id_end:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_griddata_aggregate_spatial(dataset_id, var_id, time_id_start, time_id_step, time_id_end,
                                                 request_body)
        print("--- return_rasterdata_aggregate_spatial took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata_aggregate_spatial: could not aggregate rasterdata", status_code=500,
                          payload={'dataset_id': dataset_id, 'var_id': var_id,
                                   'time_id_start': time_id_start, 'time_id_step': time_id_step,
                                   'time_id_end': time_id_end, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/temporal/dataset/<int:dataset_id>/var/<int:var_id>/time/'
           '<int:time_id_start>:<int:time_id_step>:<int:time_id_end>', methods=['POST'])
def get_rasterdata_aggregate_temporal(dataset_id, var_id, time_id_start, time_id_step, time_id_end):
    """Do temporal aggregation over times time_id:start:time_id_step:time_id_end for points
    specified in the region provided in the JSON POST body

    :param dataset_id:
    :param var_id:
    :param time_id_start:
    :param time_id_step:
    :param time_id_end:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_griddata_aggregate_temporal(dataset_id, var_id, time_id_start, time_id_step, time_id_end,
                                                 request_body)
        print("--- return_rasterdata_aggregate_temporal took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata_aggregate_temporal: could not aggregate rasterdata", status_code=500,
                          payload={'dataset_id': dataset_id, 'var_id': var_id,
                                   'time_id_start': time_id_start, 'time_id_step': time_id_step,
                                   'time_id_end': time_id_end, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/regionmeta', defaults={'regionset_id': None}, methods=['GET'])
@api.route('/regionmeta/regionset/<int:regionset_id>', methods=['GET'])
def get_regionmeta(regionset_id):
    """Get metadata of a regionset by its ID

    If no ID is specified, then the metadata of all regionsets
    currently present in the backend is returned

    :param regionset_id:
    :return:
    """
    try:
        start_time = time.time()
        data = return_regionmeta(regionset_id)
        print("--- return_regionmeta took %s seconds ---" % (time.time() - start_time))
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("get_regionmeta: could not handle request with regionset_id",
                          status_code=500, payload=regionset_id)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/regiondata/regionset/<int:regionset_id>', methods=['POST'])
def get_regiondata(regionset_id):
    """Filter regions within a regionset by some attributes

    :param regionset_id:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_regiondata(regionset_id, request_body)
        print("--- return_regiondata took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_regiondata: could not handle request", status_code=500,
                          payload={'regionset_id': regionset_id, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code=200)
    resp.headers['Content-Type'] = 'application/json'
    return resp
