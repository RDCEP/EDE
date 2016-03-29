#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import simplejson as json
except ImportError:
    import json
from datetime import date
from flask import Blueprint, make_response
from flask.ext.cache import Cache
from ede.schema.models import Grid_Meta
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
# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
# @crossdomain(origin="*")
def get_gridmeta(ids):
    """Get metadata from gridded datasets.

    Passing no ids returns metadata for all datasets. A comma-separated list
    of ids returns metadata for multiple datasets. A single integer id
    returns metadata for a single dataset.

    If ids is empty, filter resuts by parameters in the body of the request.

    :param ids:
    :return:
    """
    status_code = 200
    resp = {
        'meta': {'status': 'ok', 'message': '', },
        'objects': [] }

    if ids is None or len(ids) == 0:
        q = db_session.query(Grid_Meta)
    else:
        q = db_session.query(Grid_Meta).filter(Grid_Meta.uid.in_(ids))
    metas = q.all()

    for m in metas:
        resp['objects'].append(m.__dict__)

    resp['meta']['total'] = len(resp['objects'])
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 1
@api.route('/griddata/point/<int:meta_id>/<int:var_id>/<string:poly>/<string:time>', methods=['GET'])
def get_griddata_by_points(meta_id, var_id, poly, time):
    print meta_id, var_id, poly, time
    poly = poly.split(';')
    poly = [ pt.split(',') for pt in poly]
    print poly


# QUERY 2
@api.route('/griddata/tile/<int:meta_id>/<int:var_id>/<string:poly>/<string:time>', methods=['GET'])
def get_griddata_by_tile(meta_id, var_id, poly, time):
    print meta_id, var_id, poly, time


# QUERY 3
@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>/<string:poly>/<string:time>', methods=['GET'])
def get_spatial_aggregation(meta_id, var_id, poly, t):
    """Get spatial aggregation of gridded data within some polygon.

    :return:
    """
    status_code = 200
    data = return_aggregate_polygon_fixed_time(meta_id, var_id, poly, t)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 4
@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>/<string:poly>/<string:start_time>/<string:end_time>', methods=['GET'])
def get_temporal_aggregation(meta_id, var_id, poly, start_time, end_time):
    """Get temporal aggregation of gridded data over some time interval.

    :return:
    """
    status_code = 200
    data = return_aggregate_time_within_polygon(meta_id, var_id, poly, start_time, end_time)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# QUERY 5
@api.route('/griddata/<int:meta_id>/<int:var_id>', methods=['GET'])
def get_griddata(meta_id, var_id):
    """Get gridded dataset by its dataset id + variable id.

    Note: Because the `id` parameter is converted to an `int`, you can only
    get one dataset at a time.

    :param meta_id:
    :param var_id:
    :return:
    """
    status_code = 200
    data = return_all_frames(meta_id, var_id)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp



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