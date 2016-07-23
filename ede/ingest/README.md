# The Database Schema

This directory contains the file schema.sql which specifies the database schema.
The current version of the schema is: without_raster_type-v0.1.0

## Steps to prepare DB

1.  CREATE DATABASE without_raster_type;
2.  CREATE EXTENSION postgis;
3.  Create the schema through: psql -U postgres -d without_raster_type -f schema.sql
4.  Ingest shapefiles: python ingest_shapefile.py --input [path2shapefile]
    Note:
    *   For gadm28.shp (#features=255272) this takes 21 minutes (time 'real' output by 'time' command)
5.  Ingest psims netcdf: python ingest_netcdf.py --input [path2netcdf]
    Note:
    *   For say papsim_wfdei.cru_hist_default_firr_aet_whe_annual_1979_2012.nc4 (4.7MB)
        this takes (again: time 'real' output by 'time' command): 3 minutes 20 seconds
    *   The size of the ingested data within the DB for say the same file is: 721MB
        (thus, a blowup factor of = 721 / 4.7 ~ 153)
        (measured by taking the difference of the \d+ outputs for the raster_data_single table)
        (actually, if we also take into account the space occupied by the same data in raster_data_series,
         then we have an additional 37MB for the same file in that table)
    *   If you want to ingest all psims netcdfs, do: ./ingest_netcdf_all
6.  Make some corrections to the DB because right now we have the wrong dataset_ids
    (handled wrongly in ingest_netcdf.py): psql -U postgres -d without_raster_type -f patch.sql
7.  Make some final preparations within the DB:
    psql -U postgres -d without_raster_type -f prepare_db.sql
    This:
    *   fills raster_data_series from raster_data_single
    *   creates indexes
    *   creates user-defined functions needed by the queries
    
    Note: For 5 psims files this took (using EXPLAIN ANALYZE whenever that was possible):
    *   raster_data_single -> raster_data_series: 12 minutes 30 seconds
    *   raster_data_single_dataset_id_var_id_idx: 1 minute 45 seconds
    *   raster_data_single_geom_idx: 11 minutes 15 seconds
    *   raster_data_single_time_id_idx: 1 minute 50 seconds
    *   raster_data_series_dataset_id_var_id_idx: 4 seconds
    *   raster_data_series_geom_idx: 20 seconds
    
## Files involved

*   schema.sql
*   ingest_shapefile.py
*   ingest_netcdf.py, ingest_netcdf_all (which uses filenames.txt)
*   patch.sql
*   prepare_db.sql