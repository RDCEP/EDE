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


@api.route('/gridmeta', defaults={'dataset_ids': None}, methods=['GET'])
@api.route('/gridmeta/dataset/<intlist:dataset_ids>', methods=['GET'])
def get_gridmeta(dataset_ids):
    """Get metadata of gridded datasets by IDs.

    If no list is passed, the metadata of all gridded datasets is returned.

    :param dataset_ids:
    :return:
    """
    try:
        data = return_gridmeta(dataset_ids)
    except RasterExtractionException as e:
        eprint(e)
        raise ServerError("get_gridmeta: could not handle request with dataset_ids", status_code=500, payload=dataset_ids)
    status_code = 200
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/dataset/<int:dataset_id>/var/<int:var_id>/time/<int:time_id>', methods=['GET', 'POST'])
def get_griddata(dataset_id, var_id, time_id):
    """Get values of a specified dataset:variable:time, potentially restricted to a polygon provided in the POST body.

    If no polygon is specified we default to the entire globe.

    :param dataset_id:
    :param var_id:
    :param time_id
    :return:
    """
    content = request.get_json()
    try:
        # we have some content, i.e. POST
        if content:
            poly_id = content['poly_id']  # TODO: specify in JSON request format
            poly = content['poly']  # TODO: specify in JSON request format
            # if a polygon is specified by both poly_id and directly => return Bad Request Error
            if poly_id is not None and poly is not None:
                status_code = 400
                payload = {'dataset_id': dataset_id, 'var_id': var_id, 'content': content}
                raise ServerError("get_griddata: cannot specify polygon directly and by id at the same time", status_code, payload)
            # if polygon is specified by id
            elif poly_id is not None:
                data = return_griddata(dataset_id, var_id, poly_id, time_id)
            # if polygon is specified directly
            elif poly is not None:
                data = return_griddata(dataset_id, var_id, poly, time_id)
            # if no polygon is specified
            else:
                data = return_griddata(dataset_id, var_id, None, time_id)
        # we have no content, i.e. GET
        else:
            data = return_griddata(dataset_id, var_id, None, time_id)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'dataset_id': dataset_id, 'var_id': var_id, 'time_id': time_id, 'content': content}
        raise ServerError("get_griddata: could not get griddata", status_code, payload)
    except ServerError:
        raise
    return data


@api.route('/aggregate/spatial/dataset/<int:dataset_id>/var/<int:var_id/time/<intlist:time_ids>', methods=['GET', 'POST'])
def get_griddata_aggregate_spatial(dataset_id, var_id, time_ids):
    """Do spatial aggregation at specific dates, over a potentially provided polygon.

    If no polygon is provided we default to the entire globe.

    :param dataset_id:
    :param var_id:
    :param time_ids:
    :return:
    """
    content = request.get_json()
    try:
        # we have some content, i.e. POST
        if content:
            poly_id = content['poly_id']  # TODO: specify in JSON request format
            poly = content['poly']  # TODO: specify in JSON request format
            # if a polygon is specified by both poly_id and directly => return Bad Request Error
            if poly_id is not None and poly is not None:
                status_code = 400
                payload = {'dataset_id': dataset_id, 'var_id': var_id, 'content': content}
                raise ServerError("get_griddata_aggregate_spatial: cannot specify polygon directly and by id at the same time", status_code, payload)
            # if polygon is specified by id
            elif poly_id is not None:
                data = return_griddata_aggregate_spatial(dataset_id, var_id, poly_id, time_ids)
            # if polygon is specified directly
            elif poly is not None:
                data = return_griddata_aggregate_spatial(dataset_id, var_id, poly, time_ids)
            # if no polygon is specified
            else:
                data = return_griddata_aggregate_spatial(dataset_id, var_id, None, time_ids)
        # we have no content, i.e. GET
        else:
            data = return_griddata_aggregate_spatial(dataset_id, var_id, None, time_ids)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'dataset_id': dataset_id, 'var_id': var_id, 'time_ids': time_ids, 'content': content}
        raise ServerError("get_griddata_aggregate_spatial: could not aggregate griddata", status_code, payload)
    return data


@api.route('/aggregate/temporal/dataset/<int:dataset_id>/var/<int:var_id>/time/<intlist:time_ids>', methods=['GET', 'POST'])
def get_griddata_aggregate_temporal(dataset_id, var_id, time_ids):
    """Do temporal aggregation over specified dates, over a potentially provided polygon.

    If no polygon is provided we default to the entire globe.

    :param dataset_id:
    :param var_id:
    :param time_ids:
    :return:
    """
    content = request.get_json()
    try:
        # we have some content, i.e. POST
        if content:
            poly_id = content['poly_id']  # TODO: specify in JSON request format
            poly = content['poly']  # TODO: specify in JSON request format
            # if a polygon is specified by both poly_id and directly => return Bad Request Error
            if poly_id is not None and poly is not None:
                status_code = 400
                payload = {'dataset_id': dataset_id, 'var_id': var_id, 'content': content}
                raise ServerError("get_griddata_aggregate_temporal: cannot specify polygon directly and by id at the same time", status_code, payload)
            # if polygon is specified by id
            elif poly_id is not None:
                data = return_griddata_aggregate_temporal(dataset_id, var_id, poly_id, time_ids)
            # if polygon is specified directly
            elif poly is not None:
                data = return_griddata_aggregate_temporal(dataset_id, var_id, poly, time_ids)
            # if no polygon is specified
            else:
                data = return_griddata_aggregate_temporal(dataset_id, var_id, None, time_ids)
        # we have no content, i.e. GET
        else:
            data = return_griddata_aggregate_temporal(dataset_id, var_id, None, time_ids)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'dataset_id': dataset_id, 'var_id': var_id, 'time_ids': time_ids, 'content': content}
        raise ServerError("get_griddata_aggregate_temporal: could not aggregate griddata", status_code, payload)
    return data


@api.route('/regionmeta', defaults={'regionset_ids': None}, methods=['GET'])
@api.route('/regionmeta/regionset/<intlist:regionset_ids>', methods=['GET'])
def get_regionmeta(regionset_ids):
    try:
        data = return_regionmeta(regionset_ids)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'regionset_ids': regionset_ids}
        raise ServerError("get_regionmeta: could not get region metadata", status_code, payload)
    status_code = 200
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/regiondata/regionset/<int:regionset_id>/region/<int:region_id>', methods=['GET'])
def get_regiondata(regionset_id, region_id):
    try:
        data = return_regiondata(regionset_id, region_id)
    except RasterExtractionException as e:
        eprint(e)
        status_code = 500
        payload = {'regionset_id': regionset_id, 'region_id': region_id}
        raise ServerError("get_regiondata: could not get region data", status_code, payload)
    status_code = 200
    data['request']['url'] = request.path
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
