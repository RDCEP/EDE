from flask import Flask, render_template, g
from flask.ext.cache import Cache
from ede_test.config import CACHE_CONFIG
from ede_test.database import engine, db_session
from ede_test.api.utils import ListConverter, IntListConverter


app = Flask(__name__)
app.config.from_object('ede_test.config')
app.url_map.converters['list'] = ListConverter
app.url_map.converters['intlist'] = IntListConverter
app.url_map.strict_slashes = False

cache = Cache(app, config=CACHE_CONFIG)


@app.errorhandler(404)
def not_found(error):
    return render_template('errors/404.html'), 404


@app.errorhandler(403)
def not_found(error):
    return render_template('errors/403.html'), 403


@app.errorhandler(500)
def not_found(error):
    return render_template('errors/500.html'), 500


from ede_test.api.views import api as api_module
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
