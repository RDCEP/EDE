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


# QUERY 0
@api.route('/gridmeta', defaults={'ids': None}, methods=['GET'])
@api.route('/gridmeta/<intlist:ids>', methods=['GET'])
def get_gridmeta(ids):
    print 'Hello Sevi!'
    status_code = 200
    data = return_all_metadata(ids)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 1 + 5
@api.route('/griddata/select/<int:meta_id>/<int:var_id>/<int:poly>/<int:date>', methods=['GET'])
def get_griddata_by_points(meta_id, var_id, poly, date):
    status_code = 200
    data = return_within_region_fixed_time(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 3
@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>/<int:poly>/<int:date>', methods=['GET'])
def get_spatial_aggregation(meta_id, var_id, poly, date):
    status_code = 200
    data = return_aggregate_polygon_fixed_time(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 4
@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>/<int:poly>/<intlist:dates>', methods=['GET'])
def get_temporal_aggregation(meta_id, var_id, poly, dates):
    status_code = 200
    data = return_aggregate_time_within_polygon(meta_id, var_id, poly, dates)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


'''
@api.route('/polymeta/<list:ids>', defaults={'ids': None}, methods=['GET'])
def get_polymeta(ids):
    """Get metadata for sets of polygonal regions.

    Passing no ids returns metadata for all avialable polygon sets. A
    comma-separated list of ids returns metadata for multiple polygon sets.
    A single integer id returns metadata for a single set of polygons.

    If ids is empty, filter resuts by parameters in the body of the request.

    :param ids:
    :return:
    """
    pass


@api.route('/polydata/<int:id>', methods=['GET'])
def get_polydata(id):
    """Get a set of polygonal regions.

    Note: because the `id` parameter is converted to an `int`, you can only
    get one set of polygons at a time.

    :param id:
    :return:
    """
    pass
'''