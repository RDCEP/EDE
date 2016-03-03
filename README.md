# Setup instructions

1. Download<br/>
`git clone https://github.com/legendOfZelda/ede_test`<br/>
`cd ede_test`

2. Setup virtual environment<br/>
`virtualenv venv`<br/>
`source venv/bin/activate`

3. Download requirements<br/>
`pip install -r requirements.txt`

4. Add project's top-level directory to pythonpath<br/>
`export PYTHONPATH=$PYTHONPATH:[path where ede_test was cloned into]`<br/>
( e.g. if you cloned `ede_test` into your home directory, then you need todo: `export PYTHONPATH=$PYTHONPATH:~` )<br/>
That way imports of the form `from ede_test.schema.models import Base` (in `ingest/create_tables.py`) will work.

5. Have Postgres with PostGIS support running on the default port with an already created database called `ede`.

6. Create `netcdf_meta` and `netcdf_data` tables through SQLAlchemy by running
`python create_tables.py`

7. Ingest a NetCDF into Postgres (its metadata into `netcdf_meta` and its actual data into `netcdf_data`) using
`python ingest.py [path to netcdf to be ingested]`
