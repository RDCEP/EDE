#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import simplejson as json
except ImportError:
    import json
from datetime import date
import time
from flask import Blueprint, make_response, request
from flask_cache import Cache
from ede.config import CACHE_CONFIG
from ede.extract.extract import *
from ede.api.utils import RequestFormatException, UnsupportedCaseException, ServerError
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


@api.route('/rastermeta/datasets', defaults={'dataset_id': None}, methods=['GET'])
@api.route('/rastermeta/datasets/<int:dataset_id>', methods=['GET'])
def get_dataset_meta(dataset_id):
    """Get metadata of a raster dataset by its ID

    If no ID is specified, then the metadata of all raster datasets
    currently present is returned

    :param dataset_id:
    :return:
    """
    try:
        start_time = time.time()
        data = return_dataset_meta(dataset_id)
        print("--- get_dataset_meta took %s seconds ---" % (time.time() - start_time))
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("get_dataset_meta: could not handle request with dataset_id",
                          status_code=500, payload=dataset_id)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/rastermeta/variables', defaults={'var_id': None}, methods=['GET'])
@api.route('/rastermeta/variables/<int:var_id>', methods=['GET'])
def get_variable_meta(var_id):
    """Get metadata of a raster variable by its ID

    If no ID is specified, then the metadata of all raster variables
    currently present is returned

    :param var_id:
    :return:
    """
    try:
        start_time = time.time()
        data = return_variable_meta(var_id)
        print("--- get_variable_meta took %s seconds ---" % (time.time() - start_time))
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("get_variable_meta: could not handle request with var_id",
                          status_code=500, payload=var_id)
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/rasterdata/vars/<int:var_id>', methods=['POST'])
def get_rasterdata(var_id):
    """Get raster data values

    :param var_id:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_rasterdata(var_id, request_body)
        print("--- return_rasterdata took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException, UnsupportedCaseException) as e:
        eprint(e)
        raise ServerError("get_rasterdata: could not get rasterdata", status_code=500,
                          payload={'var_id': var_id, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/vars/<int:var_id>', methods=['POST'])
def get_rasterdata_aggregate(var_id):
    """Aggregate a variable over some axis

    :param var_id:
    :return:
    """
    request_body = request.get_json()
    try:
        start_time = time.time()
        data = return_rasterdata_aggregate(var_id, request_body)
        print("--- return_rasterdata_aggregate took %s seconds ---" % (time.time() - start_time))
    except (RasterExtractionException, RequestFormatException) as e:
        eprint(e)
        raise ServerError("get_rasterdata_aggregate: could not aggregate rasterdata", status_code=500,
                          payload={'var_id': var_id, 'request_body': request_body})
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), 200)
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
    resp = make_response(json.dumps(data, default=dthandler), 200)
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
    resp = make_response(json.dumps(data, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp
