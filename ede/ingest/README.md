# The Database Schema

This directory contains the file schema.sql which specifies the database schema.
The current version of the schema is: with_raster_type_unstacked-v0.1.0

## Steps to prepare DB

1.  CREATE DATABASE with_raster_type_unstacked;
2.  CREATE EXTENSION postgis;
3.  Create the schema through: psql -U postgres -d with_raster_type_unstacked -f schema.sql
4.  Ingest shapefiles: python ingest_shapefile.py --input [path2shapefile]
    Note:
    *   For gadm28.shp (#features=255272) this takes 21 minutes (time 'real' output by 'time' command)
5.  Ingest psims netcdf: python ingest_netcdf.py --input [path2netcdf] --output [absolutepath2wkboutputfile]
    Note:
    *   The output argument must be an absolute path because that file will be ingested using Postgres' COPY FROM,
        which itself needs the given path to be absolute.
    *   For say papsim_wfdei.cru_hist_default_firr_aet_whe_annual_1979_2012.nc4 (4.7MB)
        this takes (again: time 'real' output by 'time' command): 44 seconds
    *   The size of the ingested data within the DB for say the same file is: 7.64MB
        (thus, a blowup factor of = 7.64 / 4.7 ~ 1.63)
        (measured by taking the difference of the \d+ outputs for the raster_data table)
    *   If you want to ingest all psims netcdfs, do: ./ingest_netcdf_all

6.  Make some corrections to the DB because right now we have the wrong dataset_ids
    (handled wrongly in ingest_netcdf.py): psql -U postgres -d with_raster_type_unstacked -f patch.sql

7.  Make some final preparations within the DB:
    psql -U postgres -d with_raster_type_unstacked -f prepare_db.sql
    This creates indexes.
    
    Note: For 5 psims files this took (using EXPLAIN ANALYZE whenever that was possible):
    * raster_data_dataset_id_var_id_idx: instantly
    * raster_data_time_id_idx: instantly
    * raster_data_rast_idx: 1 second
    
## Files involved

*   schema.sql
*   ingest_shapefile.py
*   ingest_netcdf.py, ingest_netcdf_all (which uses filenames.txt)
*   patch.sql
*   prepare_db.sql