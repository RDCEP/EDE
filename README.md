# Setup instructions (WIP)

1. Download

   ```
   git clone https://github.com/rdcep/ede
   cd ede
   ```

2. Setup virtual environment

   ```
   virtualenv venv
   source venv/bin/activate
   ```

3. Add project's top-level directory to pythonpath

   ```
   export PYTHONPATH=$(pwd):$PYTHONPATH
   ```

4. Install PostgreSQL, PostGIS, and dependencies.

   Eg, for Ubuntu:
   ```
   sudo apt-get update
   sudo apt-get install postgresql
   sudo apt-get install gdal-bin

   ```

5. Download requirements

   ```
   pip install -r requirements.txt
   ```

6. Start PostgreSQL and create a database called `ede` with PostGIS
   support.

   ```
   createdb ede
   psql ede
   > CREATE EXTENSION postgis;
   ```

7. Create an `credentials.py` file in the `ede` package.

   ```
   DB_USER = 'postgres'
   DB_PASS = ''
   DB_HOST = 'localhost'
   DB_NAME = 'ede'
   DB_PORT = '5432'
   SECRET_KEY = 'avc7o9EIXhJVa9JnlLa0voxf'
   ```

8. Initialize database tables.

   ```
   python ede/schema/create_tables.py
   ```

9. Ingest a NetCDF into Postgres

   ```
   python ede/ingest/ingest.py <path to netCDF>
   ```
