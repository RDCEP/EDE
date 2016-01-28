SECRET_KEY = 'abcdefghijklmnop'
EDE_SENTRY_URL = ''
CELERY_SENTRY_URL = ''
DATA_DIR = '/tmp'

DB_USER = 'postgres'
DB_PASSWORD = ''
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ede_test'
DATABASE_CONN = 'postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

# See: https://pythonhosted.org/Flask-Cache/#configuring-flask-cache
# for config options
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
}

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
S3_BUCKET = ''

# Optional dict with attributes for a default web admin
DEFAULT_USER = {
    'name': 'ede_user',
    'email': 'youremail@example.com',
    'password': 'your password'
}

# Email address for notifying site administrators
ADMIN_EMAIL = ''

# For emailing users. ('MAIL_USERNAME' is an email address.)
MAIL_SERVER = 'smtp.gmail.com'
MAIL_PORT = 587
MAIL_USE_TLS = True
MAIL_DISPLAY_NAME = 'Plenar.io Team'
MAIL_USERNAME = ''
MAIL_PASSWORD = ''

CENSUS_BLOCKS = {
    'dataset_name': u'census_blocks',
    'business_key': 'geoid',
    'srid': 4269,
    'source_url': 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_17031_tabblock10.zip',
    'human_name': u'Plenario Census Blocks'
}

# Toggle maintenance mode
MAINTENANCE = False
