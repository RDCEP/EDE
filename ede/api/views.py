#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import simplejson as json
except ImportError:
    import json
from datetime import date
from flask import Blueprint, make_response
from flask.ext.cache import Cache
from ede.database import db_session
from ede.schema.models import Grid_Meta
from ede.config import CACHE_CONFIG
from ede.api.crossdomain import crossdomain
from ede.api.cache_key import make_cache_key


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


@api.route('/griddata/<int:id>', methods=['GET'])
def get_griddata(id):
    """Get gridded dataset by the id of its metadata.

    Note: because the `id` parameter is converted to an `int`, you can only
    get one dataset at a time.

    :param id:
    :return:
    """
    return 'Hello Sevi'


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


@api.route('/aggregate', methods=['GET'])
def get_aggregation():
    """Get aggregation of gridded data to a set of polygons.

    :return:
    """
    pass


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
