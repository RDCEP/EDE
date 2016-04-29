import os, sys
from netCDF4 import Dataset
from numpy.linalg import norm

def main(netcdf_filename):

    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    # lons = rootgrp.variables['lon'][:]
    # lats = rootgrp.variables['lat'][:]
    time = rootgrp.variables['time']

    yield_whe_frame_prev = None
    yield_whe_frame_next = None
    for t in range(time.size):
        yield_whe_frame_prev = yield_whe_frame_next
        yield_whe_frame_next = rootgrp.variables['yield_whe'][t]
        if yield_whe_frame_prev is not None and yield_whe_frame_next is not None:
            print "Euclidean difference: %d" % norm(yield_whe_frame_next-yield_whe_frame_prev)

    # print "number of longitudes: %d" % lons.size
    # print "number of latitudes: %d" % lats.size
    # print "number of yield_whe's: %d" % yield_whe.size

    rootgrp.close()

if __name__ == "__main__":
    netcdf_filename = "/var/www/ede_unstacked/ede/data/atlas/papsim_wfdei.cru_hist_harmnon_noirr_yield_whe_annual_1979_2012.nc4"
    main(netcdf_filename)
