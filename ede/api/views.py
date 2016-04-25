#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import simplejson as json
except ImportError:
    import json
from datetime import date, time
from flask import Blueprint, make_response, request
from flask.ext.cache import Cache
from ede.config import CACHE_CONFIG
from ede.extract.extract import *

cache = Cache(config=CACHE_CONFIG)

API_VERSION = 'v0'
RESPONSE_LIMIT = 1000
CACHE_TIMEOUT = 60 * 60 * 6

api = Blueprint('ede_api', __name__, url_prefix='/api/{}'.format(API_VERSION))

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None


def base_response(r):
    return dict(
        datetime=time.strftime('%Y-%m-%d %H:%M:%S'),
        status='OK',
        status_code=200,
        request=dict(url=r.path),
    )


@api.route('/flush-cache')
def flush_cache():
    cache.clear()
    resp = {'status': 'ok', 'message': 'cache flushed!'}
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/gridmeta', defaults={'ids': None}, methods=['GET', 'POST'])
@api.route('/gridmeta/datasets/<intlist:ids>', methods=['GET', 'POST'])
def get_gridmeta(ids):
    """Get metadata of gridded datasets by IDs.

    If no list is passed, the metadata of all gridded datasets is returned.

    :param ids:
    :return:
    """
    data = base_response(request)

    try:
        status_code = 200
        if request.method == 'GET':
            data['data'] = return_gridmeta(ids)
        else:
            search = request.json.get('search')
            data['data'] = return_gridmeta(ids)
    except Exception:
        status_code = 500
        data['status'] = 'Error'
    data['status_code'] = status_code
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/dataset/<int:meta_id>/var/<int:var_id>',
           methods=['GET', 'POST'])
def get_griddata(meta_id, var_id):
    """Get values within specific polygon & date, by their IDs.

    If no polygon is specified we default to the entire globe.
    If no date is specified we default to all dates.

    :param meta_id:
    :param var_id:
    :return:
    """
    data = base_response(request)
    date = None
    poly_id = None
    poly = None
    try:
        status_code = 200
        if request.method == 'GET':
            data = dict(data, **return_griddata(meta_id, var_id, poly, date))
        else:
            # TODO: specify JSON request format
            content = request.get_json()
            if content:
                date = content['date'] if 'date' in content.keys() else None
                poly_id = content['poly_id'] if 'poly_id' in content.keys() else None
                poly = content['poly'] if 'poly' in content.keys() else None
            if poly_id:
                data = dict(data, **return_griddata_by_id(meta_id, var_id, poly_id, date))
            else:
                data = dict(data, **return_griddata(meta_id, var_id, poly, date))
    except:
        status_code = 500
        data['status'] = 'Error'
    data['status_code'] = status_code
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/spatial/dataset/<int:meta_id>/var/<int:var_id>',
           methods=['GET', 'POST'])
def get_griddata_aggregate_spatial(meta_id, var_id):
    """Do spatial aggregation over specific polygon & for specific date.

    If no polygon is passed we default to the entire globe.
    If no date is passed we default to all dates.

    :param meta_id:
    :param var_id:
    :return:
    """
    data = base_response(request)
    date = None
    poly_id = None
    poly = None
    try:
        status_code = 200
        if request.method == 'GET':
            data = dict(data, **return_griddata_aggregate_spatial(
                meta_id, var_id, poly, date))
        else:
            content = request.get_json()
            if content:
                date = int(content['dates']) # must be ID = integer, #TODO: specify in JSON request format
                poly_id = int(content['poly_id'])
                poly = content['poly']
            if poly_id:
                data = dict(data, **return_griddata_aggregate_spatial_by_id(
                    meta_id, var_id, poly_id, date))
            else:
                data = dict(data, **return_griddata_aggregate_spatial(
                    meta_id, var_id, poly, date))
    except:
        status_code = 500
        data['status'] = 'Error'
    data['status_code'] = status_code
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/temporal/dataset/<int:meta_id>/var/<int:var_id>', methods=['GET', 'POST'])
def get_griddata_aggregate_temporal(meta_id, var_id):
    data = base_response(request)
    dates = None
    poly_id = None
    poly = None
    if request.method == 'POST':
        content = request.get_json()
        if content:
            dates = content['dates'] # must be list of date IDs
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