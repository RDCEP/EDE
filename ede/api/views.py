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

cache = Cache(config=CACHE_CONFIG)

API_VERSION = 'v0'
RESPONSE_LIMIT = 1000
CACHE_TIMEOUT = 60 * 60 * 6

api = Blueprint('ede_api', __name__, url_prefix='/api/{}'.format(API_VERSION))

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None


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
    status_code = 200
    data = return_gridmeta(ids)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/dataset/<int:meta_id>/var/<int:var_id>', methods=['GET', 'POST'])
def get_griddata(meta_id, var_id):
    """Get values within specific polygon & date, by their IDs.

    If no polygon is specified we default to the entire globe.
    If no date is specified we default to all dates.

    :param meta_id:
    :param var_id:
    :return:
    """
    status_code = 200
    content = request.get_json()
    poly = content['coordinates']
    date = content['date']
    data = return_griddata(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


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
    poly = content['coordinates']
    date = content['dates'] # must be ID = integer
    data = return_griddata_aggregate_spatial(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/aggregate/temporal/dataset/<int:meta_id>/var/<int:var_id>', methods=['GET', 'POST'])
def get_griddata_aggregate_temporal(meta_id, var_id):
    status_code = 200
    content = request.get_json()
    poly = content['coordinates'] # must be [p0,p1,...,pn] where p0=pn + each pi = [lon_i,lat_i]
    dates = content['dates'] # must be list of date IDs
    data = return_griddata_aggregate_temporal(meta_id, var_id, poly, dates)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp