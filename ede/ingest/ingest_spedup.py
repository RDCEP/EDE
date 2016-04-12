import os, sys, subprocess, time
from netCDF4 import Dataset
from osgeo import gdal
import psycopg2
from psycopg2.extras import Json
import re
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime, timedelta

def main(netcdf_filename):

    ### Get meta data ###
    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    # The dimensions
    dimensions = []
    num_dates = None
    for dim in rootgrp.dimensions.values():
        dimensions.append({
            "name":dim.name,
            "size":dim.size
        })
        if dim.name == "time":
            num_dates = dim.size

    # The variables
    variables=[]
    date_field_str = None
    dates_offset = None
    for var in rootgrp.variables.values():
        # The dimensions the variable depends on
        dimensions=[]
        for dim in var.dimensions:
            dimensions.append(dim)
        # The attributes of the variable
        attributes=[]
        for attr in var.ncattrs():
            attributes.append({
                "name":attr,
                "value": str(var.getncattr(attr))
            })
            if var.name == "time" and attr == "units":
                date_field_str = str(var.getncattr(attr))
        # The variable's info
        variables.append({
            "name":var.name,
            "dtype":str(var.dtype),
            "ndim":var.ndim,
            "shape":var.shape,
            "dimensions":dimensions,
            "attributes":attributes
        })
        if var.name == "time":
            dates_offset = var[0]

    has_time = date_field_str and dates_offset
    if has_time:
        # Get the starting date and the time increment as time objects
        date_fields_str = date_field_str.split("since")
        date_unit_str = date_fields_str[0].strip()
        date_delta = None
        if date_unit_str == "days":
            date_delta = timedelta(days=1)
        elif date_unit_str == "growing seasons":
            date_delta = timedelta(days=365)
        date_start_str = date_fields_str[1].strip()
        date_start = datetime.strptime(date_start_str, "%Y-%m-%d %H:%M:%S") +\
                     timedelta(seconds=dates_offset * date_delta.total_seconds())
        # Compute the dates
        dates_obj = [date_start + t * date_delta for t in range(num_dates)]
        dates = [t.strftime("%Y-%m-%d %H:%M:%S") for t in dates_obj]

    # The global attributes
    attributes = []
    for attr_key in rootgrp.ncattrs():
        attributes.append({
            "name":attr_key,
            "value":rootgrp.getncattr(attr_key)
        })

    meta_data = {
        "name":os.path.basename(netcdf_filename),
        "dimensions":dimensions,
        "variables":variables,
        "attributes":attributes
    }

    # The bounding box
    geo = False

    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['lon']))
            lon2=float(max(rootgrp.variables['lon']))
            lat1=float(min(rootgrp.variables['lat']))
            lat2=float(max(rootgrp.variables['lat']))
            geo=True
        except:
            pass

    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['X']))
            lon2=float(max(rootgrp.variables['X']))
            lat1=float(min(rootgrp.variables['Y']))
            lat2=float(max(rootgrp.variables['Y']))
            geo=True
        except:
            pass

    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['longitude']))
            lon2=float(max(rootgrp.variables['longitude']))
            lat1=float(min(rootgrp.variables['latitude']))
            lat2=float(max(rootgrp.variables['latitude']))
            geo=True
        except:
            pass

    if geo is True:
        meta_data["loc"]={
            "type": "Polygon",
            "coordinates": [[[lon1,lat1],[lon2,lat1],[lon2,lat2],[lon1,lat2],[lon1,lat1]]]
        }

    rootgrp.close()

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    # (1) Ingest into grid_meta + get meta_id
    cur.execute("insert into grid_meta (filename, filesize, filetype, meta_data, date_created) values (\'%s\', %s, \'%s\', %s, \'%s\') returning uid" %
        (os.path.basename(netcdf_filename), os.path.getsize(netcdf_filename), 'HDF', Json(meta_data), time.ctime(os.path.getctime(netcdf_filename))))
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])

    # (2) Determine variables to loop over + loop over them
    vars = map(str, rootgrp.variables.keys())
    for var in vars:
        print var
        print rootgrp.variables[var]

    '''
    for i, vname in enumerate(vnames):
        # (3.1) Ingest into grid_vars + get var_id
        cur.execute("select uid from grid_vars where vname = \'%s\'" % (vname)) # check if variable already there
        rows = cur.fetchall()
        if not rows:
            cur.execute("insert into grid_vars (vname) values (\'%s\') returning uid" % (vname)) # insert if variable not already there
            rows = cur.fetchall()
        for row in rows:
            var_id = int(row[0])

        # In case the NetCDF does have a time dimension
        if has_time:
            for band in range(num_dates):
                # (3.2) Ingest (meta_id, dates[band]) into grid_dates + get date_id
                cur.execute("insert into grid_dates (meta_id, date) values (%s, \'%s\') returning uid" % (meta_id, dates[band]))
                rows = cur.fetchall()
                for row in rows:
                    date_id = int(row[0])

                # (4) Ingest into grid_data
                # The case where we don't have subdatasets, i.e. NetCDFs from Joshua
                if not subdatasets:
                # The case where we do have subdatasets, i.e. NetCDFs from Alison
                    print "Case without subdatasets"
                else:
                    print "Case with subdatasets"
        # In case the NetCDF does not have a time dimension
        else:
            # (4) Ingest into grid_data
            # The case where we don't have subdatasets, i.e. NetCDFs from Joshua
            if not subdatasets:
                print "Case without subdatasets"
            # The case where we do have subdatasets, i.e. NetCDFs from Alison
            else:
                print "Case with subdatasets"
        '''
    # conn.commit()

if __name__ == "__main__":
    netcdf_filename = sys.argv[1]
    main(netcdf_filename)
