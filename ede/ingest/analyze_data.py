import os, sys
from netCDF4 import Dataset
import numpy as np


def main(netcdf_filename):
    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    lons = rootgrp.variables['lon'][:]
    lats = rootgrp.variables['lat'][:]
    yield_whe = rootgrp.variables['yield_whe'][:]
    print "number of longitudes: %d" % lons.size
    print "number of latitudes: %d" % lats.size
    print "number of yield_whe's: %d" % yield_whe.size

    rootgrp.close()

if __name__ == "__main__":
    netcdf_filename = "/var/www/ede_unstacked/ede/data/atlas/papsim_wfdei.cru_hist_harmnon_noirr_yield_whe_annual_1979_2012.nc4"
    main(netcdf_filename)
