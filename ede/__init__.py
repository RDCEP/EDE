try:
    import simplejson as json
except ImportError:
    import json
from datetime import date
from flask import Flask, g, make_response
from flask.ext.cache import Cache
from ede.config import CACHE_CONFIG
from ede.database import engine, db_session
from ede.api.utils import ListConverter, IntListConverter


app = Flask(__name__)
app.config.from_object('ede.config')
app.url_map.converters['list'] = ListConverter
app.url_map.converters['intlist'] = IntListConverter
app.url_map.strict_slashes = False

cache = Cache(app, config=CACHE_CONFIG)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None


@app.errorhandler(404)
def not_found(error):
    resp = {'meta': {'status': '', 'message': '', },
            'objects': [] }
    resp = make_response(json.dumps(resp, default=dthandler), 404)
    return resp


@app.errorhandler(403)
def not_found(error):
    resp = {'meta': {'status': '', 'message': '', },
            'objects': [] }
    resp = make_response(json.dumps(resp, default=dthandler), 403)
    return resp


@app.errorhandler(500)
def not_found(error):
    resp = {'meta': {'status': '', 'message': '', },
            'objects': [] }
    resp = make_response(json.dumps(resp, default=dthandler), 500)
    return resp


from ede.api.views import api as api_module
app.register_blueprint(api_module)


@app.before_request
def before_request():
    g.db = engine.dispose()


@app.teardown_request
@app.teardown_appcontext
def shutdown_session(exception=None):
    try:
        g.db.close()
        db_session.remove()
    except:
        try:
            db_session.rollback()
            db_session.close()
            g.db.close()
        except:
            pass
