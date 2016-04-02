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
@api.route('/gridmeta/<intlist:ids>', methods=['GET'])
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


@api.route('/griddata/select/<int:meta_id>/<int:var_id>', defaults={'poly': None, 'date': None}, methods=['GET'])
@api.route('/griddata/select/<int:meta_id>/<int:var_id>/<rect:poly>', defaults={'date': None}, methods=['GET'])
@api.route('/griddata/select/<int:meta_id>/<int:var_id>/<int:date>', defaults={'poly': None}, methods=['GET'])
@api.route('/griddata/select/<int:meta_id>/<int:var_id>/<rect:poly>/<int:date>', methods=['GET'])
def get_griddata_select(meta_id, var_id, poly, date):
    """Get values within specific polygon & date, by their IDs.

    If no polygon is specified we default to the entire globe.
    If no date is specified we default to all dates.

    :param meta_id:
    :param var_id:
    :param poly:
    :param date:
    :return:
    """
    status_code = 200
    data = return_griddata_select(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>', defaults={'poly': None, 'date': None}, methods=['GET'])
@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>/<rect:poly>', defaults={'date': None}, methods=['GET'])
@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>/<int:date>', defaults={'poly': None}, methods=['GET'])
@api.route('/griddata/aggregate/spatial/<int:meta_id>/<int:var_id>/<rect:poly>/<int:date>', methods=['GET'])
def get_griddata_aggregate_spatial(meta_id, var_id, poly, date):
    """Do spatial aggregation over specific polygon & for specific date.

    If no polygon is passed we default to the entire globe.
    If no date is passed we default to all dates.

    :param meta_id:
    :param var_id:
    :param poly:
    :param date:
    :return:
    """
    status_code = 200
    data = return_griddata_aggregate_spatial(meta_id, var_id, poly, date)
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>', defaults={'polys': None, 'dates': None}, methods=['GET'])
@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>/<intlist:polys>', defaults={'dates': None}, methods=['GET'])
@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>/<intlist:dates>', defaults={'polys': None}, methods=['GET'])
@api.route('/griddata/aggregate/temporal/<int:meta_id>/<int:var_id>/<intlist:polys>/<intlist:dates>', methods=['GET'])
def get_griddata_aggregate_temporal(meta_id, var_id, polys, dates):
    """Do temporal aggregation over specific dates & for points within specific polygons.

    If no polygons are passed we default to the entire globe.
    If no dates are passed we default to all dates.

    :param meta_id:
    :param var_id:
    :param polys:
    :param dates:
    :return:
    """
    status_code = 200
    data = return_griddata_aggregate_temporal(meta_id, var_id, polys, dates) #TODO
    resp = make_response(json.dumps(data, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp