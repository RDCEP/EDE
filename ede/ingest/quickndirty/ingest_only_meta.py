import os, sys, time
from netCDF4 import Dataset
from osgeo import gdal
import psycopg2
from psycopg2.extras import Json
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime, timedelta

def main(in_filename):

    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)

    cur = conn.cursor()

    rootgrp = Dataset(in_filename, "r", format="NETCDF4")

    for var in rootgrp.variables.values():
        vname = var.name
        if vname != 'lat' and vname != 'lon' and vname != 'time':
            # The attributes of the variable
            var_attrs={}
            for attr in var.ncattrs():
                var_attrs[attr] = str(var.getncattr(attr))
            print vname
            print var_attrs
            # cur.execute("insert into grid_vars values (default, 1, \'{}\', \'%s\')".format(vname, Json(var_attrs)))

    rootgrp.close()

    conn.commit()

if __name__ == "__main__":
    in_filename = sys.argv[1]
    start = time.time()
    main(in_filename)
    end = time.time()
    print "Elapsed time: %.2f" % (end-start)