# Plenar.io

RESTful API for geospatial and time aggregation across multiple open datasets.

## Running locally

* Get the EDE source:

``` bash
git clone git@github.com:RDCEP/plenario.git
```

Install support libraries for Python:

``` bash
cd EDE
pip install -r requirements.txt
```

Create a PostgreSQL database for Plenario. (If you aren't already running
[PostgreSQL](http://www.postgresql.org/), we recommend installing version 9.3 or
later.) 
sudo apt-get install -y postgresql postgresql-contrib
sudo apt-get install libpq-dev
wget http://download.osgeo.org/geos/geos-3.5.0.tar.bz2
sudo apt-get install libgeos-dev
sudo apt-get install libxslt-dev
sudo apt-get install libxml2 libxslt
sudo apt-get install libxml2-dev

The following command creates the default database, `quantum`.
sudo -u postgres createuser -P USER_NAME_HERE
sudo -u postgres createdb -O USER_NAME_HERE DATABASE_NAME_HERE

This corresponds with the `DB_NAME` setting in your `ede/settings.py` file
and can be modified.

```
createdb quantum
```
Install from https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS21UbuntuPGSQL93Apt
Make sure your local database has the [PostGIS](http://postgis.net/) extension:

sudo service postgresql start 9.4
sudo service postgresql stop 9.4
got to remove postgres 9.5 change port with netstat -a in /etc/postgresql/9.5/main/postgresql.conf
```
psql plenario_test
plenario_test=# CREATE EXTENSION postgis;
```

You'll need the ogr2ogr utility; it's part of the gdal package (we use it toimport and export shape datasets)

OSX
```
brew install gdal --with-postgresql
```

Ubuntu/Debian

```
sudo apt-get install gdal-bin
```

Create your own `settings.py` files:
=======


```
cp ede/settings.py.example ede/settings.py
cp ede/celery_settings.py.example ede/celery_settings.py
```

You will want to change, at the minimum, the following `settings.py` fields:

* `DATABASE_CONN`: edit this field to reflect your PostgreSQL
  username, server hostname, port, and database name.

* `DEFAULT_USER`: change the username, email and password on the administrator account you will use on Plenario locally.

If you want your datasets hosted on an S3 bucket, edit the fields
`AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, and `S3_BUCKET`. Otherwise,
datasets will be downloaded locally to the directory in the `DATA_DIR`
field.

Additionally, create your own `celery_settings.py` file:

```
cp ede/celery_settings.py.example ede/celery_settings.py
```

You probably do not need to change any values in `celery_settings.py`,
unless you are running redis remotely (see `BROKER_URL`).

Before running the server, [Redis](http://redis.io/) and
[Celery](http://www.celeryproject.org/) also need to be running.

Install redis-server
* To start Redis locally (in the background):
```
redis-server &
```

Celery requires quite a few installs
* To start Celery locally (in the background):
```
celery -A plenario.celery_app worker --loglevel=info &
```

Initialize the plenario database by running `python init_db.py`.
important if the user is siiferent: http://www.depesz.com/2007/10/04/ident/
Finally, run the server:

```
python runserver.py
```

Once the server is running, navigate to http://localhost:5001/ . From
the homepage, click 'Login' to log in with the username and password
from `settings.py`. Once logged in, go to 'Add a dataset' under the
'Admin' menu to add your own datasets.

# Dependencies
We use the following open source tools:

* [PostgreSQL](http://www.postgresql.org/) - database version 9.3 or greater
* [PostGIS](http://postgis.net/) - spatial database for PostgreSQL
* [Flask](http://flask.pocoo.org/) - a microframework for Python web applications
* [SQL Alchemy](http://www.sqlalchemy.org/) - Python SQL toolkit and Object Relational Mapper
* [psycopg2](http://initd.org/psycopg/) - PostgreSQL adapter for the Python
* [GeoAlchemy 2](http://geoalchemy-2.readthedocs.org/en/0.2.4/) - provides extensions to SQLAlchemy for working with spatial databases
* [Celery](http://www.celeryproject.org/) - asynchronous task queue
* [Redis](http://redis.io/) - key-value cache
 Send us a pull request. Bonus points for topic branches.


## Copyright

